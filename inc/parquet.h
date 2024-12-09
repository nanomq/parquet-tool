#ifndef PARQUET_TOOL_PARQUET_H
#define PARQUET_TOOL_PARQUET_H

void pt_binary(char *col, int argc, char **argv);
void pt_decrypt(char *col, char *footkey, char *col1key, char *col2key, int argc, char **argv);
void pt_replay(char *interval, char *url, char *topic, int argc, char **argv);
void pt_decreplay(char *footkey, char *col1key, char *col2key, char *interval, char *url, char *topic, int argc, char **argv);
void pt_search(char *col, char *signal, char *start_key, char *end_key, char *dir);
void pt_decsearch(char *footkey, char *col1key, char *col2key, char *col, char *signal, char *start_key, char *end_key, char *dir);

#endif
