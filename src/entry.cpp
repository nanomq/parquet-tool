#include <stdio.h>
#include <cstring>
#include <cstdlib>

#include <file.h>

#define PARQUET_TOOL_VERSION "0.0.1"

void
help(char *cmd, const char *ver)
{
	printf("Usage: %s <OPTION>\n", cmd);
	printf("Parquet tool version %s\n\n", ver);
	printf("Available options: sort, search, binary, decrypt, replay, version\n");
	printf("Examples:\n");
	printf("%s sort ts /tmp\n", cmd);
	printf("%s search 0 1000 /tmp\n", cmd);
	printf("%s binary data /tmp/foo.parquet /tmp/bar.parquet\n", cmd);
	printf("%s decrypt /tmp/foo.parquet\n", cmd);
	printf("%s replay 10 mqtt-tcp://127.0.0.1:1883 /tmp/foo.parquet\n", cmd);
	printf("\n");

	printf(":parquet-tool sort ts|signal <DIR>\n");
	printf(":sort parquet files in <DIR> with ts or signal\n");
	printf(":--------------------------------------------------\n");
	printf(":parquet-tool search <START-KEY> <END-KEY> <DIR>\n");
	printf(":search parquet files in <DIR> in range of <START-KEY> to <END-KEY>\n");
	printf(":--------------------------------------------------\n");
	printf(":parquet-tool binary key|data|both <FILE...>\n");
	printf(":print keys or data or both of them in <FILE...> in binary\n");
	printf(":--------------------------------------------------\n");
	printf(":parquet-tool decrypt <FOOT-KEY> <COL1-KEY> <COL2-KEY> <FILE...>\n");
	printf(":decrypt <FILE...> with <FOOT-KEY> <COL1-KEY> <COL2-KEY>.\n");
	printf(":filename of decrypted files will be like dec.foo.parquet\n");
	printf(":--------------------------------------------------\n");
	printf(":parquet-tool replay <INTERVAL> <MQTT-URL> <FILE...>\n");
	printf(":replay datas in <FILE...> to <MQTT-URL> mqtt broker in <INTERVAL>ms\n");
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
	if (0 == strcmp(opt, "sort") && argc == 4) {
		pt_sort(argv[2], argv[3]);
	} else if (0 == strcmp(opt, "search")) {
	} else if (0 == strcmp(opt, "binary")) {
	} else if (0 == strcmp(opt, "decrypt")) {
	} else if (0 == strcmp(opt, "replay")) {
	} else if (0 == strcmp(opt, "version")) {
		printf("%s", PARQUET_TOOL_VERSION);
	} else {
		printf("number of args wrong\n");
	}
	return 0;
}
