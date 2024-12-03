#include <stdio.h>
#include <dirent.h>

#include <map>
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
	map<string, vector<string>> fm;
	//{prefix}_{signal}_{md5}-{start_key}~{end_key}.parquet
	const char *prefix = "nanomq_";
	for (string& fname: fv) {
		if (0 != strncmp(prefix, fname.c_str(), strlen(prefix))) {
			continue;
		}
		if (0 == strcmp(key, "ts")) {
		}
		else if (0 == strcmp(key, "signal")) {
			size_t signal_start = strlen(prefix);
			size_t signal_end   = fname.find("_", strlen(prefix));
			string signal = fname.substr(signal_start, signal_end - signal_start);
			if (fm.find(signal) == fm.end()) {
				vector<string> v;
				v.push_back(fname);
				fm[signal] = v;
			} else {
				fm[signal].push_back(fname);
			}
		}
	}
	for (auto const& x : fm) {
		ptlog("signal: %s", x.first.c_str());
		for (string s: x.second) {
			ptlog("\t%s", s.c_str());
		}
	}
}

void
pt_sort(char *key, char *dir)
{
	if (key == NULL)
		ptlog("null argument key");
	if (0 != strcmp(key, "ts") && 0 != strcmp(key, "signal"))
		ptlog("invalid argument key.");
	if (dir == NULL)
		ptlog("null argument dir");

	vector<string> fv = listdir((const char *)dir, "parquet");
	sortby(fv, key);
}

