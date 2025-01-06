#ifndef PARQUET_TOOL_PARQUET_H
#define PARQUET_TOOL_PARQUET_H

#include <map>
#include <string>
#include <any>

using namespace std;

// Public
map<string, any> pt_cat(char *col, char *fname, char *deli, char *footkey, char *col1key, char *col2key);
map<string, any> pt_search(char *signal, char *start_key, char *end_key, char *dir, char *footkey, char *col1key, char *col2key);
map<string, any> pt_fuzz(char *signal, char *start_key, char *end_key, char *dir, char *footkey, char *col1key, char *col2key);

// Internal wrappered API for entry.cpp
void ipt_cat(char *col, char *fname, char *deli, char *footkey, char *col1key, char *col2key);
void ipt_search(char *signal, char *start_key, char *end_key, char *dir, char *footkey, char *col1key, char *col2key);
void ipt_fuzz(char *signal, char *start_key, char *end_key, char *dir, char *footkey, char *col1key, char *col2key);
void ipt_replay(char *interval, char *url, char *topic, char *file, char *footkey, char *col1key, char *col2key);

// Expose for testing
map<string, any> read_parquet(char *fname,
	const char *footkey, const char *col1key, const char *col2key);
int write_parquet(char *fname, const char *footkey,
	const char *col1key, const char *col2key, map<string, any> lm);

#endif
