#include <any>
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <cassert>
#include <memory>
#include <string>
#include <map>
#include <vector>
#include <list>
#include <iostream>

#include <arrow/io/file.h>
#include <arrow/api.h>
#include <arrow/io/api.h>

#include <parquet/api/reader.h>
#include <parquet/encryption/encryption.h>
#include <parquet/arrow/reader.h>
#include <parquet/file_reader.h>
#include <parquet/exception.h>
#include <parquet/stream_reader.h>
#include <parquet/stream_writer.h>

#include <file.h>
#include <log.h>
#include <mqtt.h>
#include <nng/nng.h>

using namespace std;

static string path_int64 = "key";
static string path_str   = "data";

int
write_parquet(char *fname, const char *footkey, const char *col1key, const char *col2key, map<string, any> lm)
{

using parquet::ConvertedType;
using parquet::Encoding;
using parquet::Repetition;
using parquet::Type;
using parquet::schema::GroupNode;
using parquet::schema::PrimitiveNode;

try {
	const char *schema[2];
	schema[0] = path_int64.c_str(); schema[1] = path_str.c_str();
	parquet::schema::NodeVector fields;
	fields.push_back(
	    PrimitiveNode::Make(schema[0], parquet::Repetition::REQUIRED,
	        parquet::Type::INT64, parquet::ConvertedType::UINT_64));
	fields.push_back(
	    PrimitiveNode::Make(schema[1], Repetition::OPTIONAL,
	        Type::BYTE_ARRAY, ConvertedType::NONE));
	shared_ptr<GroupNode> _schema = static_pointer_cast<GroupNode>(
		GroupNode::Make("schema", Repetition::REQUIRED, fields));

	parquet::WriterProperties::Builder builder;
	builder.created_by("NanoMQ-ParquetTool")
	    ->version(parquet::ParquetVersion::PARQUET_2_6)
	    ->data_page_version(parquet::ParquetDataPageVersion::V2)
	    ->encoding(schema[0], Encoding::DELTA_BINARY_PACKED)
	    ->disable_dictionary(schema[0])
	    ->compression(static_cast<arrow::Compression::type>(4)); // ZSTD

	// TODO Decrypt

	shared_ptr<parquet::WriterProperties> props = builder.build();
	using FileClass = arrow::io::FileOutputStream;
	shared_ptr<FileClass> out_file;
	PARQUET_ASSIGN_OR_THROW(out_file, FileClass::Open(fname));
	shared_ptr<parquet::ParquetFileWriter> file_writer =
	    parquet::ParquetFileWriter::Open(out_file, _schema, props);

	ptlog("parquet writter init done");

	// Append a RowGroup with a specific number of rows.
	parquet::RowGroupWriter *rg_writer = file_writer->AppendRowGroup();

	// Write the Int64 column
	list<int64_t> col1 = any_cast<list<int64_t>>(lm[path_int64]);
	list<int64_t>::iterator it1 = col1.begin();
	parquet::Int64Writer *int64_writer =
	    static_cast<parquet::Int64Writer *>(rg_writer->NextColumn());
	for (uint32_t r = 0; r < col1.size(); r++) {
		int64_t value            = *it1;
		int16_t definition_level = 1;
		int64_writer->WriteBatch(1, &definition_level, nullptr, &value);
		it1++;
	}

	// Write the ByteArray column. Make every alternate values NULL
	list<string>  col2 = any_cast<list<string>>(lm[path_str]);
	list<string>::iterator  it2 = col2.begin();
	parquet::ByteArrayWriter *ba_writer =
	    static_cast<parquet::ByteArrayWriter *>(rg_writer->NextColumn());
	for (uint32_t r = 0; r < col2.size(); r++) {
		if (it2->length() != 0) {
			int16_t definition_level = 1;
			parquet::ByteArray value;
			value.ptr = (const uint8_t *)it2->c_str();
			value.len = it2->length();
			ba_writer->WriteBatch(1, &definition_level, nullptr, &value);
		} else {
			int16_t definition_level = 0;
			ba_writer->WriteBatch(1, &definition_level, nullptr, nullptr);
		}
		it2 ++;
	}
	ptlog("%ld records has be written to %s", col2.size(), fname);

	// Close the RowGroupWriter
	rg_writer->Close();

	// Close the ParquetFileWriter
	file_writer->Close();
} catch (const exception &e) {
	string exception_msg = e.what();
	ptlog("exception_msg=[%s]\n", exception_msg.c_str());
	return -1;
}

	return 0;
}

map<string, any>
read_parquet(char *fname, const char *footkey, const char *col1key, const char *col2key)
{
	map<string, any> lm;
	std::unique_ptr<parquet::ParquetFileReader> parquet_reader;
	parquet::ReaderProperties reader_properties = parquet::default_reader_properties();

	std::map<std::string, std::shared_ptr<parquet::ColumnDecryptionProperties>> decryption_cols;

	parquet::ColumnDecryptionProperties::Builder decryption_col_builder31(path_int64);
	if (col1key) {
		decryption_cols[path_int64] = decryption_col_builder31.key(col1key)->build();
	}
	parquet::ColumnDecryptionProperties::Builder decryption_col_builder32(path_str);
	if (col2key) {
		decryption_cols[path_str]   = decryption_col_builder32.key(col2key)->build();
	}

	parquet::FileDecryptionProperties::Builder file_decryption_builder_3;
	if (footkey) {
		std::vector<std::shared_ptr<parquet::FileDecryptionProperties>> vector_of_decryption_configurations;
		vector_of_decryption_configurations.push_back(
		    file_decryption_builder_3.footer_key(footkey)->column_keys(decryption_cols)->build());
		// Add the current decryption configuration to ReaderProperties.
		reader_properties.file_decryption_properties(vector_of_decryption_configurations[0]->DeepClone());
	}

	try {
		parquet_reader = parquet::ParquetFileReader::OpenFile(fname, false, reader_properties);
		// Get the File MetaData
		std::shared_ptr<parquet::FileMetaData> file_metadata = parquet_reader->metadata();
		int num_row_groups = file_metadata->num_row_groups();  //Get the number of RowGroups
		int num_columns = file_metadata->num_columns();   //Get the number of Columns
		assert(num_columns == 2);
		//ptlog("%s:num_row_groups=[%d],num_columns=[%d]", fname, num_row_groups, num_columns);

		for (int r = 0; r < num_row_groups; ++r) {
			std::shared_ptr<parquet::RowGroupReader> row_group_reader = parquet_reader->RowGroup(r);
			string strCont;
			int64_t values_read = 0;
			int64_t rows_read = 0;
			int16_t definition_level;
			int16_t repetition_level;
			std::shared_ptr<parquet::ColumnReader> column_reader;

			// Get the Column Reader for the Int64 column
			column_reader = row_group_reader->Column(0);
			parquet::Int64Reader* int64_reader = static_cast<parquet::Int64Reader*>(column_reader.get());
			int col1n = 0, col2n = 0;
			list<int64_t> col1;
			list<string> col2;

			while (int64_reader->HasNext()) {
				int64_t value;
				rows_read = int64_reader->ReadBatch(1, &definition_level, &repetition_level, &value, &values_read);
				if (1 == rows_read && 1 == values_read)  {
					col1.push_back(std::move(value));
				}
				col1n ++;
			}

			// Get the Column Reader for the ByteArray column
			column_reader = row_group_reader->Column(1);
			parquet::ByteArrayReader* ba_reader = static_cast<parquet::ByteArrayReader*>(column_reader.get());

			while (ba_reader->HasNext()) {
				parquet::ByteArray value;
				rows_read = ba_reader->ReadBatch(1, &definition_level, nullptr, &value, &values_read);
				if (1 == rows_read && 1 == values_read)  {
					string strTemp = string((char*)value.ptr, value.len);
					col2.push_back(std::move(strTemp));
				}
				col2n ++;
			}
			if (col1n != col2n) {
				ptlog("Invaild parquet file %s col1n=%d,col2n=%d", fname, col1n, col2n);
				return lm;
			}

			lm[path_int64] = col1;
			lm[path_str]   = col2;
			return lm;
		}
	} catch (const std::exception &e) {
		ptlog("exception_msg=[%s]", e.what());
	}
	return lm;
}

map<string, any>
parquet_map(vector<pair<int64_t, string>> fv)
{
	map<string, any> fm;
	list<int64_t> lk;
	list<string>  lp;
	for (auto x : fv) {
		lk.push_back(x.first);
		lp.push_back(x.second);
	}
	fm[path_int64] = lk;
	fm[path_str]   = lp;
	return fm;
}

static void
showvector(vector<pair<int64_t, string>> res, char *col)
{
	for (pair<int64_t, string> e : res) {
		if (path_str.compare(col) == 0) {
			for (int i = 0; i < e.second.length(); i++)
				printf("%c", e.second.c_str()[i]);
			if (!is_quiet_mode())
				printf("\n");
		} else if (strcmp("both", col) == 0) {
			printf("%lld,", e.first);
			for (int i = 0; i < e.second.length(); i++)
				printf("%c", e.second.c_str()[i]);
			printf("\n");
		}
	}
}

static void
showparquet(map<string, any>& lm, char *col)
{
	if (lm.end() == lm.find(path_int64) || lm.end() == lm.find(path_str)) {
		ptlog("No key or data found");
		return;
	}
	list<int64_t> col1 = any_cast<list<int64_t>>(lm[path_int64]);
	list<string>  col2 = any_cast<list<string>>(lm[path_str]);

	list<int64_t>::iterator it1 = col1.begin();
	list<string>::iterator  it2 = col2.begin();

	if (0 == path_int64.compare(string(col))) {
		while (col1.end() != it1) {
			printf("%lld\n", *it1);
			it1++;
		}
	} else if (0 == path_str.compare(string(col))) {
		while (col2.end() != it2) {
			for (int i = 0; i < it2->length(); i++)
				printf("%c", it2->c_str()[i]);
			if (!is_quiet_mode())
				printf("\n");
			it2++;
		}
	} else if (0 == strcmp("both", col)) {
		while (col1.end() != it1 && col2.end() != it2) {
			printf("%lld,", *it1);
			for (int i = 0; i < it2->length(); i++)
				printf("%c", it2->c_str()[i]);
			printf("\n");
			it1++;
			it2++;
		}
	}
}

static vector<pair<int64_t, string>>
fuzz_ts_rangeof(map<string, any> lm, char *start_key, char *end_key, int64_t& lastts)
{
	vector<pair<int64_t, string>> res;

	if (lm.end() == lm.find(path_int64) || lm.end() == lm.find(path_str)) {
		ptlog("No key or data found");
		return res;
	}

	list<int64_t> col1 = any_cast<list<int64_t>>(lm[path_int64]);
	list<string>  col2 = any_cast<list<string>>(lm[path_str]);

	list<int64_t>::iterator it1 = col1.begin();
	list<string>::iterator  it2 = col2.begin();

	int64_t sk = stoll(start_key);
	int64_t ek = stoll(end_key);

	while (it1 != col1.end() && it2 != col2.end()) {
		if (lastts >= ek) {
			break;
		}
		if (sk <= *it1 && *it1 <= ek && lastts < *it1) {
			res.push_back(pair<int64_t, string>(*it1, *it2));
			lastts = *it1;
		}
		it1 ++;
		it2 ++;
	}
	return res;
}

static vector<pair<int64_t, string>>
rangeof(map<string, any> lm, char *start_key, char *end_key)
{
	vector<pair<int64_t, string>> res;

	if (lm.end() == lm.find(path_int64) || lm.end() == lm.find(path_str)) {
		ptlog("No key or data found");
		return res;
	}

	list<int64_t> col1 = any_cast<list<int64_t>>(lm[path_int64]);
	list<string>  col2 = any_cast<list<string>>(lm[path_str]);

	list<int64_t>::iterator it1 = col1.begin();
	list<string>::iterator  it2 = col2.begin();

	int64_t sk = stoll(start_key);
	int64_t ek = stoll(end_key);

	while (it1 != col1.end() && it2 != col2.end()) {
		if (sk <= *it1 && *it1 <= ek) {
			res.push_back(pair<int64_t, string>(*it1, *it2));
		}
		it1 ++;
		it2 ++;
	}
	return res;
}

static bool
compare_by_col1(pair<int64_t, string> a, pair<int64_t, string> b) {
	return a.first < b.first;
}

void
pt_cat(char *col, char *fname, char *footkey, char *col1key, char *col2key)
{
	map<string, any> lm = read_parquet(fname, footkey, col1key, col2key);
	showparquet(lm, col);
}

void
pt_replay(char *interval, char *url, char *topic, char *file, char *footkey, char *col1key, char *col2key)
{
	nng_socket sock;
	mqtt_connect(&sock, url);
	int timepast = 0; // ms

	map<string, any> lm = read_parquet(file, footkey, col1key, col2key);
	if (lm.end() == lm.find(path_str)) {
		pterr("No data found");
		return;
	}
	int cnt = 0;
	list<string> col2 = any_cast<list<string>>(lm[path_str]);
	list<string>::iterator it2 = col2.begin();
	while (col2.end() != it2) {
		mqtt_publish(sock, topic, it2->c_str(), it2->length());
		ptlog("%.*X", it2->length(), it2->c_str());
		it2++; cnt++;
		usleep(stoi(interval) * 1000);
		if ((timepast += stoi(interval)) % 5000 == 0)
			ptlog("sent %d msgs in %s", cnt, file);
	}
}

void
pt_search(char *signal, char *start_key, char *end_key, char *dir, char *footkey, char *col1key, char *col2key)
{
	vector<pair<int64_t, string>> res;
	vector<string> fv = listdir((const char *)dir, "parquet");
	map<string, vector<string>> fm = sortby(fv, (char *)"signal");
	for (auto& x: fm) {
		if (x.first.compare(signal) != 0)
			continue;
		for (string fname: x.second) {
			string p = string(dir);
			if (p.back() != '/') p.push_back('/');
			string fullpath = fname.insert(0, p);
			map<string, any> lm = read_parquet((char *)fullpath.c_str(), footkey, col1key, col2key);
			vector<pair<int64_t, string>> rm = rangeof(lm, start_key, end_key);
			res.insert(res.end(), rm.begin(), rm.end());
		}
	}
	sort(res.begin(), res.end(), compare_by_col1);
	map<string, any> resm = parquet_map(res);
	char foname[128];
	sprintf(foname, "search-%s-%s-%s.parquet", signal, start_key, end_key);
	write_parquet(foname, footkey, col1key, col2key, resm);
}

void
pt_fuzz(char *signal, char *start_key, char *end_key, char *dir, char *footkey, char *col1key, char *col2key)
{
	int64_t lastts = 0;
	vector<pair<int64_t, string>> res;
	vector<string> fv = listdir((const char *)dir, "parquet");
	map<string, vector<string>> fm = sortby(fv, (char *)"signal");
	for (auto& x: fm) {
		if (x.first.compare(signal) != 0)
			continue;
		for (string fname: x.second) {
			string p = string(dir);
			if (p.back() != '/') p.push_back('/');
			string fullpath = fname.insert(0, p);
			map<string, any> lm = read_parquet((char *)fullpath.c_str(), footkey, col1key, col2key);
			vector<pair<int64_t, string>> rm = fuzz_ts_rangeof(lm, start_key, end_key, lastts);
			res.insert(res.end(), rm.begin(), rm.end());
		}
	}
	map<string, any> resm = parquet_map(res);
	char foname[128];
	sprintf(foname, "fuzz-%s-%s-%s.parquet", signal, start_key, end_key);
	write_parquet(foname, footkey, col1key, col2key, resm);
}

