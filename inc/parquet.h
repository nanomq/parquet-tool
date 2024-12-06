#ifndef PARQUET_TOOL_PARQUET_H
#define PARQUET_TOOL_PARQUET_H

void pt_binary(char *col, int argc, char **argv);
void pt_decrypt(char *col, char *footkey, char *col1key, char *col2key, int argc, char **argv);

#endif
