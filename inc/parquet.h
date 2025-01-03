#ifndef PARQUET_TOOL_PARQUET_H
#define PARQUET_TOOL_PARQUET_H

#include <map>
#include <string>
#include <any>

// Public

void pt_cat(char *col, char *fname, char *footkey, char *col1key, char *col2key);
void pt_search(char *signal, char *start_key, char *end_key, char *dir, char *footkey, char *col1key, char *col2key);
void pt_fuzz(char *signal, char *start_key, char *end_key, char *dir, char *footkey, char *col1key, char *col2key);
void pt_replay(char *interval, char *url, char *topic, char *file, char *footkey, char *col1key, char *col2key);

// Expose for testing
using namespace std;
map<string, any> read_parquet(char *fname,
	const char *footkey, const char *col1key, const char *col2key);
int write_parquet(char *fname, const char *footkey,
	const char *col1key, const char *col2key, map<string, any> lm);

#endif
