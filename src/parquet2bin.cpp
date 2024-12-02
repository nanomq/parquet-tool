#include <stdio.h>
#include <stdlib.h>
#include <cassert>
#include <memory>
#include <string>
#include <map>
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

using namespace std;

const char* kFooterEncryptionKey = "0123456789012345";  // 128bit/16
const char* kColumnEncryptionKey1 = "0123456789012345";
const char* kColumnEncryptionKey2 = "0123456789012345";

int
parquet2bin(char* fname)
{
    std::vector<std::shared_ptr<parquet::FileDecryptionProperties> > vector_of_decryption_configurations;

    // Decryption configuration 3: Decrypt using explicit column and footer keys.
    std::string path_int64 = "key";
    std::string path_str = "data";
    std::map<std::string, std::shared_ptr<parquet::ColumnDecryptionProperties> >   decryption_cols;
    parquet::ColumnDecryptionProperties::Builder decryption_col_builder31(path_int64);
    parquet::ColumnDecryptionProperties::Builder decryption_col_builder32(path_str);

    decryption_cols[path_int64] = decryption_col_builder31.key(kColumnEncryptionKey1)->build();
    decryption_cols[path_str] = decryption_col_builder32.key(kColumnEncryptionKey2)->build();

    parquet::FileDecryptionProperties::Builder file_decryption_builder_3;
    vector_of_decryption_configurations.push_back(file_decryption_builder_3.footer_key(kFooterEncryptionKey)->column_keys(decryption_cols)->build());

    parquet::ReaderProperties reader_properties = parquet::default_reader_properties();

    // Add the current decryption configuration to ReaderProperties.
    // reader_properties.file_decryption_properties(vector_of_decryption_configurations[0]->DeepClone());

    // Create a ParquetReader instance
    //std::unique_ptr<parquet::ParquetFileReader> parquet_reader = parquet::ParquetFileReader::OpenFile("yingyun.parquet", false, reader_properties);
    std::string exception_msg = "";
    try {
        std::unique_ptr<parquet::ParquetFileReader> parquet_reader = parquet::ParquetFileReader::OpenFile(fname, false, reader_properties);
        // Get the File MetaData
        std::shared_ptr<parquet::FileMetaData> file_metadata = parquet_reader->metadata();
        int num_row_groups = file_metadata->num_row_groups();  //Get the number of RowGroups
        int num_columns = file_metadata->num_columns();   //Get the number of Columns
        assert(num_columns == 2);
        //printf("num_row_groups=[%d],num_columns=[%d]\n", num_row_groups, num_columns);
        for (int r = 0; r < num_row_groups; ++r) {
            std::shared_ptr<parquet::RowGroupReader> row_group_reader = parquet_reader->RowGroup(r); //Get the RowGroup Reader
            string strCont;
            int64_t values_read = 0;
            int64_t rows_read = 0;
            int16_t definition_level;
            int16_t repetition_level;
            std::shared_ptr<parquet::ColumnReader> column_reader;

            // Get the Column Reader for the Int64 column
            column_reader = row_group_reader->Column(0);
            parquet::Int64Reader* int64_reader = static_cast<parquet::Int64Reader*>(column_reader.get());
            int num1 = 0, num2 = 0;
            std::list<int64_t> colum1;
            std::list<std::string> colum2;

            int i = 0;
            while (int64_reader->HasNext()) {
                int64_t value;
                rows_read = int64_reader->ReadBatch(1, &definition_level, &repetition_level, &value, &values_read);
                if (1 == rows_read && 1  == values_read)  {
                    colum1.push_back(std::move(value));
                }
                i++;
            }
            num1 = i;

            // Get the Column Reader for the ByteArray column
            column_reader = row_group_reader->Column(1);
            parquet::ByteArrayReader* ba_reader =  static_cast<parquet::ByteArrayReader*>(column_reader.get());

            i = 0;
            while (ba_reader->HasNext()) {
                parquet::ByteArray value;
                rows_read =  ba_reader->ReadBatch(1, &definition_level, nullptr, &value, &values_read);
                if (1 == rows_read && 1  == values_read)  {
                    string strTemp = string((char*)value.ptr,value.len);
                    colum2.push_back(std::move(strTemp));
                }
                i++;
            }
            num2 = i;
            //printf("num1=%d,num2=%d\n",num1, num2);

            if (num1 == num2) {
                std::list<int64_t>::iterator iter1 = colum1.begin();
                std::list<std::string>::iterator iter2 = colum2.begin();
                while (colum1.end() != iter1 && colum2.end() != iter2)
                {
                    // printf("%s\n", iter2->c_str());
                    // printf("%lld %s\n", *iter1, iter2->c_str());
                    // printf("%lld:     ", *iter1);
                    for (int i = 0; i < iter2->length(); i++) {

                        unsigned char c = iter2->c_str()[i];
                        //printf("0x%x ", c);
                        printf("%c", c);

                    }

                    //std::cout <<  std::endl;
                    //std::cout <<  std::endl;
                    // std::cout << *iter1 << " " << *iter2 << std::endl;
                    iter1++;
                    iter2++;
                }
            }
        }
    } catch (const std::exception& e) {
            exception_msg = e.what();
            printf("exception_msg=[%s]\n", exception_msg.c_str());
    }
}
