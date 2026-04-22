#ifndef PARQUET_TOOL_PARQUET_H
#define PARQUET_TOOL_PARQUET_H

#include <map>
#include <string>
#include <any>
#include <cstdint>

using namespace std;

// Public
map<string, any> pt_cat(char *col, char *fname, char *deli, char *footkey, char *colkey);
map<string, any> pt_search(char *signal, char *start_key, char *end_key, char *dir, char *footkey, char *colkey);
map<string, any> pt_fuzz(char *signal, char *start_key, char *end_key, char *dir, char *footkey, char *colkey);

// Internal wrappered API for entry.cpp
void ipt_metadata(char *fname, char *footkey);
void ipt_cat(char *col, char *fname, char *deli, char *footkey, char *colkey);
void ipt_search(char *signal, char *start_key, char *end_key, char *dir, char *footkey, char *colkey);
void ipt_fuzz(char *signal, char *start_key, char *end_key, char *dir, char *footkey, char *colkey);
void ipt_replay(char *interval, char *url, char *topic, char *file, char *footkey, char *colkey);
void ipt_schema(char *fname, char *schemafile, char *deli);

// Expose for testing
map<string, any> read_parquet(char *fname, const char *footkey, const char *colkey);
map<string, any> read_parquet_schema(char *fname, const char **schema, uint16_t schema_len);
int write_parquet(char *fname, const char *footkey, const char *colkey, map<string, any> lm);

#endif
