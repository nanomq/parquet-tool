#include <any>
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <cassert>
#include <memory>
#include <string>
#include <map>
#include <utility>
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
#include <schema.h>
#include <nng/nng.h>

using namespace std;

static string path_int64  = "key";
static string path_str    = "data";
static string path_schema = "schemadata";

int
write_parquet(char *fname, const char *footkey, const char *colkey, map<string, any> lm)
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

	// Encrypt
	if (footkey && colkey) {
		shared_ptr<parquet::FileEncryptionProperties> enc_prop;
		std::map<std::string, std::shared_ptr<parquet::ColumnEncryptionProperties>> col_enc_map;
		for (int i=0; i<2; ++i) {
			const char *col_name = schema[i];
			parquet::ColumnEncryptionProperties::Builder col_builder(col_name);
			col_builder.key(colkey)->key_metadata("col_key_metadata");
			col_enc_map[col_name] = col_builder.build();
		}
		parquet::FileEncryptionProperties::Builder file_encryption_builder(footkey);
		enc_prop = file_encryption_builder
			.footer_key_metadata("fk")
			->encrypted_columns(col_enc_map)
			->algorithm(static_cast<parquet::ParquetCipher::type>(0)) // AES_GCM_V1
			->set_plaintext_footer()
			->build();

		builder.encryption(enc_prop);
	}

	shared_ptr<parquet::WriterProperties> props = builder.build();
	using FileClass = arrow::io::FileOutputStream;
	shared_ptr<FileClass> out_file;
	PARQUET_ASSIGN_OR_THROW(out_file, FileClass::Open(fname));
	shared_ptr<parquet::ParquetFileWriter> file_writer =
	    parquet::ParquetFileWriter::Open(out_file, _schema, props);

	shared_ptr<arrow::KeyValueMetadata> kv = make_shared<arrow::KeyValueMetadata>();
	kv->Append("nmq.meta.version", "1");
	kv->Append("nmq.topic", "tool_test_topic");
	if (footkey && colkey) {
		kv->Append("nmq.enc.cipher", "AES_GCM_V1");
		kv->Append("nmq.key.id", "fk");
		kv->Append("nmq.key.wrap_alg", "NMQ_CONF_CIPHER_AES_GCM_BASE64");
		kv->Append("nmq.key.wrapped", "aaaaaaaaaa");
	}
	kv->Append("nmq.created_by", "NanoMQ");
	kv->Append("nmq.created_at", std::to_string((long long) time(NULL)));
	kv->Append("number", "1");
	file_writer->AddKeyValueMetadata(kv);

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
read_parquet_schema(char *fname, vector<string>& schema_vec, char *schemafile)
{
	map<string, any> lm;
	std::unique_ptr<parquet::ParquetFileReader> parquet_reader;
	parquet::ReaderProperties reader_properties = parquet::default_reader_properties();
try {
	parquet_reader = parquet::ParquetFileReader::OpenFile(fname, false, reader_properties);
	// Get the File MetaData
	shared_ptr<parquet::FileMetaData> file_metadata = parquet_reader->metadata();
	if (schemafile)
		schema_vec = read_schemafile_to_vec(schemafile);
	int num_row_groups = file_metadata->num_row_groups();  //Get the number of RowGroups
	int num_columns = file_metadata->num_columns();   //Get the number of Columns
	//ptlog("%s:num_row_groups=[%d],num_columns=[%d]", fname, num_row_groups, num_columns);
	// TODO the num_row_groups should always be 1
	for (int r = 0; r < num_row_groups; ++r) {
		vector<uint64_t>               ts;
		std::shared_ptr<parquet::RowGroupReader> row_group_reader = parquet_reader->RowGroup(r);
		std::shared_ptr<parquet::ColumnReader> column_reader;
		// Get the Column Reader for the Int64 column
		column_reader = row_group_reader->Column(0);
		parquet::Int64Reader* int64_reader = static_cast<parquet::Int64Reader*>(column_reader.get());
		int col1n = 0, colarrn = 0;
		list<int64_t> col1;
		vector<vector<string>> colarr;

		string strCont;
		int64_t values_read = 0, rows_read = 0;
		int16_t definition_level, repetition_level;

		while (int64_reader->HasNext()) {
			int64_t value;
			rows_read = int64_reader->ReadBatch(1, &definition_level, &repetition_level, &value, &values_read);
			if (1 == rows_read && 1 == values_read)  {
				col1.push_back(std::move(value));
			}
			col1n ++;
		}

		for (int i=1; i<num_columns; ++i) {
			const char *column_name = file_metadata->schema()->Column(i)->name().c_str();
			if (!schemafile)
				schema_vec.push_back(string(column_name, strlen(column_name)));

			column_reader = row_group_reader->Column(i);
			auto ba_reader = dynamic_pointer_cast<parquet::ByteArrayReader>(column_reader);
			vector<string> col2;
			int col2n = 0;

			while (ba_reader->HasNext()) {
				parquet::ByteArray value;
				rows_read = ba_reader->ReadBatch(1, &definition_level, &repetition_level, &value, &values_read);
				if (1 == rows_read && 1 == values_read)  {
					string strTemp = string((char*)value.ptr, value.len);
					col2.push_back(std::move(strTemp));
				} else {
					col2.push_back(string(""));
				}
				col2n ++;
			}
			if (colarrn == 0)
				colarrn = col2n;
			else
				if (col2n != colarrn)
					ptlog("columns in schema payload has different length.");

			colarr.push_back(col2);
		}

		lm[path_int64] = col1;
		lm[path_schema] = colarr;
		return lm;
	}
} catch (const std::exception &e) {
	ptlog("exception_msg=[%s]", e.what());
}
	return lm;
}

map<string, string>
read_parquet_metadata(char *fname, const char *footkey)
{
	map<string, string> res;
	std::unique_ptr<parquet::ParquetFileReader> parquet_reader;
	parquet::ReaderProperties reader_properties = parquet::default_reader_properties();

	try {
		parquet_reader = parquet::ParquetFileReader::OpenFile(fname, false, reader_properties);
		// Get the File MetaData
		std::shared_ptr<parquet::FileMetaData> file_metadata = parquet_reader->metadata();
		auto kv = file_metadata->key_value_metadata(); // User metadata
		int num_row_groups = file_metadata->num_row_groups();  //Get the number of RowGroups
		int num_columns = file_metadata->num_columns();   //Get the number of Columns
		// ptlog("%s:num_row_groups=[%d],num_columns=[%d]", fname, num_row_groups, num_columns);

		res["num_row_groups"] = std::to_string(num_row_groups);
		res["num_columns"]    = std::to_string(num_columns);

		if (!kv) {
			ptlog("No MetaData found");
			return res;
		}

		int idx;
		if (((idx = kv->FindKey("nmq.created_by")) >= 0) && kv->value(idx).compare("NanoMQ") == 0) {
			res["nmq.created_by"] = kv->value(idx);
		} else {
			ptlog("MetaData found But was not created by NanoMQ");
			return res;
		}
		if ((idx = kv->FindKey("nmq.created_at")) >= 0)   res["nmq.created_at"] = kv->value(idx);

		if ((idx = kv->FindKey("nmq.meta.version")) >= 0) res["nmq.meta.version"] = kv->value(idx);
		if ((idx = kv->FindKey("nmq.topic")) >= 0)        res["nmq.topic"] = kv->value(idx);
		if ((idx = kv->FindKey("nmq.enc.cipher")) >= 0)   res["nmq.enc.cipher"] = kv->value(idx);
		if ((idx = kv->FindKey("nmq.key.id")) >= 0)       res["nmq.key.id"] = kv->value(idx);
		if ((idx = kv->FindKey("nmq.key.wrap_alg")) >= 0) res["nmq.key.wrap_alg"] = kv->value(idx);
		if ((idx = kv->FindKey("nmq.key.wrapped")) >= 0)  res["nmq.key.wrapped"] = kv->value(idx);
		if ((idx = kv->FindKey("number")) >= 0)           res["number"] = kv->value(idx);
	} catch (const std::exception &e) {
		ptlog("exception_msg=[%s]", e.what());
	}
	return res;
}

class UniformKeyRetriever : public parquet::DecryptionKeyRetriever {
	std::string key_;
public:
	UniformKeyRetriever(const std::string& key) : key_(key) {}
	std::string GetKey(const std::string&) override { return key_; }
};

map<string, any>
read_parquet(char *fname, const char *footkey, const char *colkey)
{
	map<string, any> lm;
	std::unique_ptr<parquet::ParquetFileReader> parquet_reader;
	parquet::ReaderProperties reader_properties = parquet::default_reader_properties();

	parquet::FileDecryptionProperties::Builder file_decryption_builder_3;
	if (footkey) {
		shared_ptr<parquet::FileDecryptionProperties> decryption_configuration = 
			file_decryption_builder_3.footer_key(footkey)
			->key_retriever(std::make_shared<UniformKeyRetriever>(colkey))->build();
		reader_properties.file_decryption_properties(decryption_configuration->DeepClone());
	}

	try {
		parquet_reader = parquet::ParquetFileReader::OpenFile(fname, false, reader_properties);
		// Get the File MetaData
		std::shared_ptr<parquet::FileMetaData> file_metadata = parquet_reader->metadata();
		int num_row_groups = file_metadata->num_row_groups();  //Get the number of RowGroups
		int num_columns = file_metadata->num_columns();   //Get the number of Columns
		assert(num_columns == 2);
		// ptlog("%s:num_row_groups=[%d],num_columns=[%d]", fname, num_row_groups, num_columns);

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
showparquet(map<string, any>& lm, char *col, char *deli)
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
			printf("%lld", *it1);
			if (!deli)
				printf("\n");
			else
				printf("%s", deli);
			it1++;
		}
	} else if (0 == path_str.compare(string(col))) {
		while (col2.end() != it2) {
			for (int i = 0; i < it2->length(); i++)
				printf("%c", it2->c_str()[i]);
			if (!deli)
				printf("\n");
			else
				printf("%s", deli);
			it2++;
		}
	} else if (0 == strcmp("both", col)) {
		while (col1.end() != it1 && col2.end() != it2) {
			printf("%lld,", *it1);
			for (int i = 0; i < it2->length(); i++)
				printf("%c", it2->c_str()[i]);
			if (!deli)
				printf("\n");
			else
				printf("%s", deli);
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

map<string, any>
pt_cat(char *col, char *fname, char *deli, char *footkey, char *colkey)
{
	map<string, any> lm = read_parquet(fname, footkey, colkey);
	return lm;
}

void
ipt_metadata(char *fname, char *footkey)
{
	map<string, string> md = read_parquet_metadata(fname, footkey);
	for (auto& x: md) {
		ptlog("%s: %s", x.first.c_str(), x.second.c_str());
	}
}

void
ipt_cat(char *col, char *fname, char *deli, char *footkey, char *colkey)
{
	map<string, any> lm = pt_cat(col, fname, deli, footkey, colkey);
	showparquet(lm, col, deli);
}

void
ipt_replay(char *interval, char *url, char *topic, char *file, char *footkey, char *colkey)
{
	nng_socket sock;
	mqtt_connect(&sock, url);
	int timepast = 0; // ms

	map<string, any> lm = read_parquet(file, footkey, colkey);
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

map<string, any>
pt_search(char *signal, char *start_key, char *end_key, char *dir, char *footkey, char *colkey)
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
			map<string, any> lm = read_parquet((char *)fullpath.c_str(), footkey, colkey);
			vector<pair<int64_t, string>> rm = rangeof(lm, start_key, end_key);
			res.insert(res.end(), rm.begin(), rm.end());
		}
	}
	sort(res.begin(), res.end(), compare_by_col1);
	map<string, any> resm = parquet_map(res);
	return resm;
}

void
ipt_search(char *signal, char *start_key, char *end_key, char *dir, char *footkey, char *colkey)
{
	map<string, any> resm = pt_search(signal, start_key, end_key, dir, footkey, colkey);
	char foname[128];
	sprintf(foname, "search-%s-%s-%s.parquet", signal, start_key, end_key);
	write_parquet(foname, footkey, colkey, resm);
}

map<string, any>
pt_fuzz(char *signal, char *start_key, char *end_key, char *dir, char *footkey, char *colkey)
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
			map<string, any> lm = read_parquet((char *)fullpath.c_str(), footkey, colkey);
			vector<pair<int64_t, string>> rm = fuzz_ts_rangeof(lm, start_key, end_key, lastts);
			res.insert(res.end(), rm.begin(), rm.end());
		}
	}
	map<string, any> resm = parquet_map(res);
	return resm;
}

void
ipt_fuzz(char *signal, char *start_key, char *end_key, char *dir, char *footkey, char *colkey)
{
	map<string, any> resm = pt_fuzz(signal, start_key, end_key, dir, footkey, colkey);
	char foname[128];
	sprintf(foname, "fuzz-%s-%s-%s.parquet", signal, start_key, end_key);
	write_parquet(foname, footkey, colkey, resm);
}

void
ipt_schema(char *fname, char *schemafile, char *deli)
{
	vector<string> schema_vec;
	map<string, any> m = read_parquet_schema(fname, schema_vec, schemafile);
	list<int64_t> lk = any_cast<list<int64_t>>(m["key"]);
	vector<vector<string>> larr = any_cast<vector<vector<string>>>(m["schemadata"]);

	// Print header line
	// printf("ts");
	// for (string& ele: schema_vec) {
	// 	printf(", ");
	// 	printf("%s", ele.c_str());
	// }
	// printf("\n");

	list<int64_t>::iterator lk_it = lk.begin();
	for (int i=0; i<larr[0].size(); ++i) {
		vector<string> row;
		//printf("%lld", *lk_it);
		for (int j=0; j<larr.size(); ++j) {
			// printf(", ");
			vector<string>& col = larr[j];
			string& ele = col[i];
			// for (int n=0; n<ele.size(); ++n)
			// 	printf("%02x", (uint8_t)ele.c_str()[n]);
			// if (ele.size() == 0)
			// 	printf("-");
			row.push_back(ele);
		}
		// if (!deli)
		// 	printf("\n");
		// else
		// 	printf("%s", deli);

		// Print sorted datas
		vector<pair<string, int>> row_sorted = schema_sort(row);
		for (pair<string, int>& p: row_sorted) {
			int idx = p.second;
			string data = p.first;
			int64_t ts = *lk_it + (uint64_t)(uint8_t)data.data()[0];
			if (idx >= schema_vec.size()) {
				fprintf(stderr, "number of parquet columns is LARGE than schema columss (%d)\n",
						idx-schema_vec.size()+1);
				break;
			}
			string busid = schema_vec[idx].substr(0, 2);
			string canid = schema_vec[idx].substr(2, 6);
			int pldlen = (int)(((uint16_t)(data.data()[1]) << 8) + (uint16_t)data.data()[2]);
			string pld = data.substr(3);
			printf("%lld, %s, %s, ", ts, busid.c_str(), canid.c_str());
			for (int n=0; n<pldlen; ++n)
				printf("%02x", (uint8_t)pld.c_str()[n]);
			printf("\n");
		}

		lk_it++;
	}
}
