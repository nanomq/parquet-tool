#ifndef PARQUET_TOOL_SCHEMA_H
#define PARQUET_TOOL_SCHEMA_H

#include <vector>
#include <string>
#include <utility>

using namespace std;
vector<string> read_schemafile_to_vec(char *fname);
string schema_cat(vector<string>& schema_vec, int64_t ts, vector<string>& src);
vector<pair<string, int>> schema_sort(vector<string> row);

#endif
