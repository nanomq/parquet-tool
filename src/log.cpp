#include <stdlib.h>
#include <string.h>
#include <log.h>

bool ptlog_quiet = false;

bool
is_quiet_mode()
{
	return ptlog_quiet;
}

void
ptlog_init()
{
	char *value = getenv("QUIET");
	printf("QUIET:[%s]\n", value);
	if (value && 0 == strcmp(value, "1"))
		ptlog_quiet = true;
}
