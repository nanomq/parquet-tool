#include <stdio.h>
#include <cstring>
#include <cstdlib>

#define PARQUET_TOOL_VERSION "0.0.1"

void
help(char *cmd, const char *ver)
{
	printf("Usage: %s <OPTION>\n", cmd);
	printf("Parquet tool version %s\n\n", ver);
	printf("Available options: sort, search, binary, decrypt, version\n");
	printf("Examples:\n");
	printf("%s sort ts /tmp\n", cmd);
	printf("%s search [starttime] [endtime] /tmp\n", cmd);
	printf("%s binary /tmp/foo.parquet /tmp/bar.parquet\n", cmd);
	printf("%s decrypt <footkey> <col1key> <col2key> /tmp/foo.parquet\n", cmd);
	printf("\n");
	exit(0);
}

int
main(int argc, char** argv) 
{
	char *cmd = argv[0];
	char *opt = NULL;
	if (argc < 2) {
		help(cmd, PARQUET_TOOL_VERSION);
	}
	opt = argv[1];
	if (0 == strcmp(opt, "sort")) {
	}
	else if (0 == strcmp(opt, "sort")) {
	}
	else if (0 == strcmp(opt, "binary")) {
	}
	else if (0 == strcmp(opt, "decrypt")) {
	}
	else if (0 == strcmp(opt, "version")) {
		printf("%s", PARQUET_TOOL_VERSION);
	}
}
