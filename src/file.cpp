#include <stdio.h>
#include <dirent.h>

#include <vector>
#include <string>
#include <cstring>

#include <log.h>

using namespace std;

static vector<string>
listdir(const char *path, const char *filter)
{
	DIR                *d;
	struct dirent      *dir;
	std::vector<string> fv;

	d = opendir(path);
	if (d) {
		while ((dir = readdir(d)) != NULL) {
			if (!filter || !strstr(dir->d_name, filter))
				continue;
			fv.push_back(string(dir->d_name));
		}
		closedir(d);
	}
	return fv;
}

static void
sortby(vector<string> fv, char *key)
{
}

void
pt_sort(char *key, char *dir)
{
	if (key == NULL)
		ptlog("null argument key");
	if (0 != strcmp(key, "ts") && 0 != strcmp(key, "size"))
		ptlog("invalid argument key.");
	if (dir == NULL)
		ptlog("null argument dir");

	vector<string> fv = listdir((const char *)dir, "parquet");
	sortby(fv, key);
}

