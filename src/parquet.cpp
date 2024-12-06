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
#include <log.h>

#include <arrow/io/file.h>
#include <arrow/api.h>
#include <arrow/io/api.h>

#include <parquet/api/reader.h>
#include <parquet/encryption/encryption.h>
#include <parquet/arrow/reader.h>
#include <parquet/file_reader.h>
#include <parquet/exception.h>

#include <mqtt.h>
#include <nng/nng.h>

using namespace std;

static string path_int64 = "key";
static string path_str   = "data";

static map<string, any>
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
		ptlog("%s:num_row_groups=[%d],num_columns=[%d]\n", fname, num_row_groups, num_columns);

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
			ptlog("col1n=%d,col2n=%d\n", col1n, col2n);
			assert(col1n == col2n);

			lm[path_int64] = col1;
			lm[path_str]   = col2;
			return lm;
		}
	} catch (const std::exception &e) {
		ptlog("exception_msg=[%s]", e.what());
	}
	return lm;
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

void
pt_binary(char *col, int argc, char **argv)
{
	if (col == NULL) {
		ptlog("null argument col");
		return;
	}
	for (int i=0; i<argc; ++i) {
		if (argv[i] == NULL) {
			ptlog("null argument No.%d filename", i);
			return;
		}
	}

	for (int i=0; i<argc; ++i) {
		map<string, any> lm = read_parquet(argv[i], NULL, NULL, NULL);
		showparquet(lm, col);
	}
}

void
pt_decrypt(char *col, char *footkey, char *col1key, char *col2key, int argc, char **argv)
{
	if (col == NULL) {
		ptlog("null argument col");
		return;
	}
	for (int i=0; i<argc; ++i) {
		if (argv[i] == NULL) {
			ptlog("null argument No.%d filename", i);
			return;
		}
	}

	for (int i=0; i<argc; ++i) {
		map<string, any> lm = read_parquet(argv[i], footkey, col1key, col2key);
		showparquet(lm, col);
	}
}

void
pt_decreplay(char *footkey, char *col1key, char *col2key, char *interval, char *url, char *topic, int argc, char **argv)
{
	nng_socket sock;
	mqtt_connect(&sock, url);
	int timepast = 0; // ms

	for (int i=0; i<argc; ++i) {
		map<string, any> lm = read_parquet(argv[i], footkey, col1key, col2key);
		if (lm.end() == lm.find(path_str)) {
			ptlog("No data found");
			continue;
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
				ptlog("sent %d msgs in %s", cnt, argv[i]);
		}
	}
}
void
pt_replay(char *interval, char *url, char *topic, int argc, char **argv)
{
	pt_decreplay(NULL, NULL, NULL, interval, url, topic, argc, argv);
}
