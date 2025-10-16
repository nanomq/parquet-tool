#ifndef PARQUET_TOOL_SCHEMA_H
#define PARQUET_TOOL_SCHEMA_H

#include <vector>
#include <string>

using namespace std;
vector<string> read_schemafile_to_vec(char *fname);
string schema_cat(vector<string> schema_vec, vector<string> src);

#endif
