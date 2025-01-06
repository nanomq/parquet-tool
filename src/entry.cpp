#include <stdio.h>
#include <unistd.h>
#include <cstring>
#include <cstdlib>

#include <log.h>
#include <file.h>
#include <parquet.h>

#define PARQUET_TOOL_VERSION "0.0.2"

void
help_ls()
{
	printf(":parquet-tool ls -r START,END -d DIR\n");
	printf(":list parquet files in <DIR> in range of <START> to <END>\n");
	printf(":\n");
	printf(": -r range\n");
	printf(": -d directory\n");
	exit(0);
}

void
entry_ls(int argc, char **argv)
{
	char c;
	char *range = NULL;
	char *dir   = NULL;
	char *start_key, *end_key;
	while ((c = getopt(argc, argv, ":r:d:")) != -1) {
		switch (c) {
		case 'r':
			range = strdup(optarg);
			break;
		case 'd':
			dir = optarg;
			break;
		}
	}
	if (range == NULL) {
		pterr("null argument range");
		help_ls();
	}
	if (dir == NULL) {
		pterr("null argument dir");
		help_ls();
	}
	start_key = range;
	end_key   = (char *)strstr(range, ",");
	if (end_key == NULL) {
		pterr("null argument end_key");
		help_ls();
	}
	end_key[0] = '\0';
	end_key ++;
	pt_ls(start_key, end_key, dir);
	if (range)
		free(range);
}

void
help_sort()
{
	printf(":parquet-tool sort -k ts|signal -d DIR\n");
	printf(":sort parquet files in <DIR> with ts or signal\n");
	printf(":\n");
	printf(": -k key\n");
	printf(": -d directory\n");
	exit(0);
}

void
entry_sort(int argc, char **argv)
{
	char c;
	char *key = NULL;
	char *dir = NULL;
	while ((c = getopt(argc, argv, ":k:d:")) != -1) {
		switch (c) {
		case 'k':
			key = optarg;
			break;
		case 'd':
			dir = optarg;
			break;
		}
	}
	if (key == NULL) {
		pterr("null argument key");
		help_sort();
	}
	if (dir == NULL) {
		pterr("null argument dir");
		help_sort();
	}
	pt_sort(key, dir);
}

void
help_cat()
{
	printf(":parquet-tool cat -c key|data|both -f FILE\n");
	printf(":print key or data or both of them in <FILE>\n");
	printf(":\n");
	printf(": -c column\n");
	printf(": -f file\n");
	printf(": -m delimiter (\'n by default)\n");
	printf(": -x footer key\n");
	printf(": -y column1 key\n");
	printf(": -z column2 key\n");
	exit(0);
}

void
entry_cat(int argc, char **argv)
{
	char c;
	char *col  = NULL;
	char *file = NULL;
	char *deli = NULL;
	char *fk   = NULL;
	char *c1k  = NULL;
	char *c2k  = NULL;
	while ((c = getopt(argc, argv, ":c:f:m:x:y:z:")) != -1) {
		switch (c) {
		case 'c':
			col = optarg;
			break;
		case 'f':
			file = optarg;
			break;
		case 'm':
			deli = optarg;
			break;
		case 'x':
			fk = optarg;
			break;
		case 'y':
			c1k = optarg;
			break;
		case 'z':
			c2k = optarg;
			break;
		}
	}
	if (col == NULL) {
		pterr("null argument column");
		help_cat();
	}
	if (file == NULL) {
		pterr("null argument file");
		help_cat();
	}
	pt_cat(col, file, deli, fk, c1k, c2k);
}

void
help_search()
{
	printf(":parquet-tool search -s SIGNAL -r START,END -d DIR\n");
	printf(":search records in range of <START> to <END> in parquet files in <DIR>\n");
	printf(":\n");
	printf(": -s signal\n");
	printf(": -r range\n");
	printf(": -d directory\n");
	printf(": -x footer key\n");
	printf(": -y column1 key\n");
	printf(": -z column2 key\n");
	exit(0);
}

void
entry_search(int argc, char **argv)
{
	char c;
	char *sig   = NULL;
	char *range = NULL;
	char *dir   = NULL;
	char *fk    = NULL;
	char *c1k   = NULL;
	char *c2k   = NULL;
	char *start_key, *end_key;
	while ((c = getopt(argc, argv, ":s:r:d:x:y:z:")) != -1) {
		switch (c) {
		case 's':
			sig = optarg;
			break;
		case 'r':
			range = strdup(optarg);
			break;
		case 'd':
			dir = optarg;
			break;
		case 'x':
			fk = optarg;
			break;
		case 'y':
			c1k = optarg;
			break;
		case 'z':
			c2k = optarg;
			break;
		}
	}
	if (sig == NULL) {
		pterr("null argument signal");
		help_search();
	}
	if (range == NULL) {
		pterr("null argument range");
		help_search();
	}
	if (dir == NULL) {
		pterr("null argument dir");
		help_search();
	}
	start_key = range;
	end_key   = (char *)strstr(range, ",");
	if (end_key == NULL) {
		pterr("null argument end_key");
		help_search();
	}
	end_key[0] = '\0';
	end_key ++;
	pt_search(sig, start_key, end_key, dir, fk, c1k, c2k);
}

void
help_fuzz()
{
	printf(":parquet-tool fuzz -s SIGNAL -r START,END -d DIR\n");
	printf(":fuzz search records in range of <START> to <END> in parquet files in <DIR>\n");
	printf(":\n");
	printf(": -s signal\n");
	printf(": -r range\n");
	printf(": -d directory\n");
	printf(": -x footer key\n");
	printf(": -y column1 key\n");
	printf(": -z column2 key\n");
	exit(0);
}

void
entry_fuzz(int argc, char **argv)
{
	char c;
	char *sig   = NULL;
	char *range = NULL;
	char *dir   = NULL;
	char *fk    = NULL;
	char *c1k   = NULL;
	char *c2k   = NULL;
	char *start_key, *end_key;
	while ((c = getopt(argc, argv, ":s:r:d:x:y:z:")) != -1) {
		switch (c) {
		case 's':
			sig = optarg;
			break;
		case 'r':
			range = strdup(optarg);
			break;
		case 'd':
			dir = optarg;
			break;
		case 'x':
			fk = optarg;
			break;
		case 'y':
			c1k = optarg;
			break;
		case 'z':
			c2k = optarg;
			break;
		}
	}
	if (sig == NULL) {
		pterr("null argument signal");
		help_fuzz();
	}
	if (range == NULL) {
		pterr("null argument range");
		help_fuzz();
	}
	if (dir == NULL) {
		pterr("null argument dir");
		help_fuzz();
	}
	start_key = range;
	end_key   = (char *)strstr(range, ",");
	if (end_key == NULL) {
		pterr("null argument end_key");
		help_fuzz();
	}
	end_key[0] = '\0';
	end_key ++;
	pt_fuzz(sig, start_key, end_key, dir, fk, c1k, c2k);
}

void
help_replay()
{
	printf(":parquet-tool replay -i INTERVAL -u MQTT-URL -t TOPIC -f FILE\n");
	printf(":replay datas in FILE to <MQTT-URL> mqtt broker in <INTERVAL>ms\n");
	printf(":\n");
	printf(": -i interval\n");
	printf(": -u url of mqtt broker\n");
	printf(": -t topic\n");
	printf(": -f file\n");
	printf(": -x footer key\n");
	printf(": -y column1 key\n");
	printf(": -z column2 key\n");
	exit(0);
}

void
entry_replay(int argc, char **argv)
{
	char c;
	char *itv   = NULL;
	char *url   = NULL;
	char *topic = NULL;
	char *file  = NULL;
	char *fk    = NULL;
	char *c1k   = NULL;
	char *c2k   = NULL;
	char *start_key, *end_key;
	while ((c = getopt(argc, argv, ":i:u:t:f:x:y:z:")) != -1) {
		switch (c) {
		case 'i':
			itv = optarg;
			break;
		case 'u':
			url = optarg;
			break;
		case 't':
			topic = optarg;
			break;
		case 'f':
			file = optarg;
			break;
		case 'x':
			fk = optarg;
			break;
		case 'y':
			c1k = optarg;
			break;
		case 'z':
			c2k = optarg;
			break;
		}
	}
	if (itv == NULL) {
		pterr("null argument interval");
		help_replay();
	}
	if (url == NULL) {
		pterr("null argument url");
		help_replay();
	}
	if (topic == NULL) {
		pterr("null argument topic");
		help_replay();
	}
	if (file == NULL) {
		pterr("null argument file");
		help_replay();
	}
	pt_replay(itv, url, topic, file, fk, c1k, c2k);
}

void
help(char *cmd, const char *ver)
{
	printf("Usage: %s <CMD>\n", cmd);
	printf("Parquet tool version %s\n\n", ver);
	printf("Available commands: ls, sort, cat, search, fuzz, replay, version\n\n");
	printf("Examples:\n");
	printf("%s ls -r 0,1000 -d /tmp\n", cmd);
	printf("%s sort -k ts -d /tmp\n", cmd);
	printf("%s cat -c key -f /tmp/foo.parquet\n", cmd);
	printf("%s search -s canspi -r 0,1000 -d /tmp\n", cmd);
	printf("%s fuzz -s canspi -r 0,1000 -d /tmp\n", cmd);
	printf("%s replay -i 10 -u mqtt-tcp://127.1:1883 -t topic -f /tmp/foo.parquet\n", cmd);
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
	if (0 == strcmp(opt, "ls")) {
		entry_ls(argc, argv);
	} else if (0 == strcmp(opt, "sort")) {
		entry_sort(argc, argv);
	} else if (0 == strcmp(opt, "cat")) {
		entry_cat(argc, argv);
	} else if (0 == strcmp(opt, "search")) {
		entry_search(argc, argv);
	} else if (0 == strcmp(opt, "fuzz")) {
		entry_fuzz(argc, argv);
	} else if (0 == strcmp(opt, "replay")) {
		entry_replay(argc, argv);
	} else if (0 == strcmp(opt, "version")) {
		printf("%s", PARQUET_TOOL_VERSION);
	} else {
		ptlog("number of args wrong or invalid cmd\n");
		help(cmd, PARQUET_TOOL_VERSION);
	}
	return 0;
}
