#ifndef PARQUET_TOOL_LOG_H
#define PARQUET_TOOL_LOG_H

#include <cstdio>

bool is_quiet_mode();
void ptlog_init();

#if 0

#define pterr(format, ...) \
	do {                    \
	} while (0)

#define ptlog(format, ...) \
	do {                    \
	} while (0)

#else

#define pterr(format, arg...)                                                 \
	do { \
		fprintf(stderr, "%s:%d(%s) " format "\n", __FILE__, __LINE__, \
		    __FUNCTION__, ##arg);                                     \
	} while (0)

#define ptlog(format, arg...)                                                 \
	do { \
		if (!is_quiet_mode()) { \
			fprintf(stdout, "%s:%d(%s) " format "\n", __FILE__, __LINE__, \
			    __FUNCTION__, ##arg);                                     \
		} \
	} while (0)

#endif

#endif
