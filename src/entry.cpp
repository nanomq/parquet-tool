#include <stdio.h>
#include <cstring>
#include <cstdlib>

#include <log.h>
#include <file.h>
#include <parquet.h>

#define PARQUET_TOOL_VERSION "0.0.1"

void
help(char *cmd, const char *ver)
{
	printf("Usage: %s <OPTION>\n", cmd);
	printf("Parquet tool version %s\n\n", ver);
	printf("Available options: sort, search, binary, decrypt, replay, version\n");
	printf("Examples:\n");
	printf("%s sort ts /tmp\n", cmd);
	printf("%s ls 0 1000 /tmp\n", cmd);
	printf("%s search data canspi 0 1000 /tmp\n", cmd);
	printf("%s binary key /tmp/foo.parquet /tmp/bar.parquet\n", cmd);
	printf("%s decrypt both 0123456789012345 0123456789012345 0123456789012345 /tmp/foo.parquet\n", cmd);
	printf("%s replay 10 mqtt-tcp://127.0.0.1:1883 topic /tmp/foo.parquet\n", cmd);
	printf("\n");

	printf(":parquet-tool sort ts|signal <DIR>\n");
	printf(":sort parquet files in <DIR> with ts or signal\n");
	printf(":--------------------------------------------------\n");
	printf(":parquet-tool ls <START-KEY> <END-KEY> <DIR>\n");
	printf(":list parquet files in <DIR> in range of <START-KEY> to <END-KEY>\n");
	printf(":--------------------------------------------------\n");
	printf(":parquet-tool search data|both <SIGNAL> <START-KEY> <END-KEY> <DIR>\n");
	printf(":print data or both in parquet files in <DIR> in range of <START-KEY> to <END-KEY>\n");
	printf(":--------------------------------------------------\n");
	printf(":parquet-tool decsearch data|both <SIGNAL> <FOOT-KEY> <COL1-KEY> <COL2-KEY> <START-KEY> <END-KEY> <DIR>\n");
	printf(":Combine decrypt and search\n");
	printf(":--------------------------------------------------\n");
	printf(":parquet-tool binary key|data|both <FILE...>\n");
	printf(":print keys or data or both of them in <FILE...> in binary\n");
	printf(":--------------------------------------------------\n");
	printf(":parquet-tool decrypt key|data|both <FOOT-KEY> <COL1-KEY> <COL2-KEY> <FILE...>\n");
	printf(":decrypt key or data or both in<FILE...> with <FOOT-KEY> <COL1-KEY> <COL2-KEY>\n");
	printf(":--------------------------------------------------\n");
	printf(":parquet-tool replay <INTERVAL> <MQTT-URL> <TOPIC> <FILE...>\n");
	printf(":replay datas in <FILE...> to <MQTT-URL> mqtt broker in <INTERVAL>ms\n");
	printf(":--------------------------------------------------\n");
	printf(":parquet-tool decreplay <FOOT-KEY> <COL1-KEY> <COL2-KEY> <INTERVAL> <MQTT-URL> <TOPIC> <FILE...>\n");
	printf(":Combine decrypt and replay\n");
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
	ptlog_init();
	opt = argv[1];
	if (0 == strcmp(opt, "sort") && argc == 4) {
		pt_sort(argv[2], argv[3]);
	} else if (0 == strcmp(opt, "ls") && argc == 5) {
		pt_ls(argv[2], argv[3], argv[4]);
	} else if (0 == strcmp(opt, "search") && argc == 7) {
		pt_search(argv[2], argv[3], argv[4], argv[5], argv[6]);
	} else if (0 == strcmp(opt, "dersearch") && argc == 10) {
		pt_decsearch(argv[2], argv[3], argv[4], argv[5],
			argv[6], argv[7], argv[8], argv[9]);
	} else if (0 == strcmp(opt, "binary") && argc > 3) {
		pt_binary(argv[2], argc-3, argv + 3);
	} else if (0 == strcmp(opt, "decrypt") && argc > 6) {
		pt_decrypt(argv[2], argv[3], argv[4], argv[5], argc-6, argv + 6);
	} else if (0 == strcmp(opt, "replay") && argc > 5) {
		pt_replay(argv[2], argv[3], argv[4], argc-5, argv + 5);
	} else if (0 == strcmp(opt, "decreplay") && argc > 8) {
		pt_decreplay(argv[2], argv[3], argv[4], argv[5], argv[6], argv[7], argc-8, argv + 8);
	} else if (0 == strcmp(opt, "version")) {
		printf("%s", PARQUET_TOOL_VERSION);
	} else {
		ptlog("number of args wrong or invalid option\n");
		help(cmd, PARQUET_TOOL_VERSION);
	}
	return 0;
}
