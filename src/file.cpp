#include <stdio.h>
#include <dirent.h>

#include <map>
#include <vector>
#include <string>
#include <cstring>
#include <algorithm>

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

static string
fname_start_key(string fname)
{
	size_t start_key_start = fname.find("-") + 1;
	size_t start_key_end   = fname.find("~");
	return fname.substr(start_key_start, start_key_end - start_key_start);
}

static string
fname_end_key(string fname)
{
	size_t end_key_start = fname.find("~") + 1;
	size_t end_key_end   = fname.find(".", end_key_start);
	return fname.substr(end_key_start, end_key_end - end_key_start);
}

static bool
compare_by_ts(string a, string b) {
	return (stoll(fname_start_key(a)) < stoll(fname_start_key(b)));
}

static void
show_signalmap(const map<string, vector<string>>& fm)
{
	for (const auto& x : fm) {
		const vector<string>& v = x.second;
		ptlog("signal: %s", x.first.c_str());
		for (string s: v) {
			ptlog("\t%s", s.c_str());
		}
		ptlog("--------------------------------------------------");
	}
}

static map<string, vector<string>>
sortby(vector<string> fv, char *key)
{
	map<string, vector<string>> fm;
	//{prefix}_{signal}_{md5}-{start_key}~{end_key}.parquet
	const char *prefix = "nanomq_";
	for (string& fname: fv) {
		if (0 != strncmp(prefix, fname.c_str(), strlen(prefix))) {
			continue;
		}
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

	for (auto& x : fm) {
		vector<string>& v = x.second;
		if (0 == strcmp(key, "ts")) {
			sort(v.begin(), v.end(), compare_by_ts);
		} else if (0 == strcmp(key, "signal")) {
			// nothing to do
		}
	}
	return fm;
}

static map<string, vector<string>>
search_range(map<string, vector<string>> fm, char *start_key, char *end_key)
{
	string sk = string(start_key);
	string ek = string(end_key);

	for (auto& x: fm) {
		vector<string>& v = x.second;
		auto it = std::remove_if(v.begin(), v.end(),
			[&](const string& s) {
				string fsk = fname_start_key(s);
				string fek = fname_end_key(s);
			    if (stoll(fek) < stoll(sk) || stoll(fsk) > stoll(ek)) {
				    return true;
			    }
			    return false;
		    });

		v.erase(it, v.end());
	}
	return fm;
}

void
pt_sort(char *key, char *dir)
{
	if (key == NULL) {
		ptlog("null argument key");
		return;
	}
	if (0 != strcmp(key, "ts") && 0 != strcmp(key, "signal")) {
		ptlog("invalid argument key.");
		return;
	}
	if (dir == NULL) {
		ptlog("null argument dir");
		return;
	}

	vector<string> fv = listdir((const char *)dir, "parquet");
	map<string, vector<string>> sigmap = sortby(fv, key);
	show_signalmap(sigmap);
}

void
pt_search(char *start_key, char *end_key, char *dir)
{
	if (start_key == NULL) {
		ptlog("null argument start_key");
		return;
	}
	if (end_key == NULL) {
		ptlog("null argument end_key");
		return;
	}
	if (dir == NULL) {
		ptlog("null argument dir");
		return;
	}
	if (stoll(start_key) > stoll(end_key)) {
		ptlog("Invalid start_key or end_key, start_key should LessEqual than end_key");
		return;
	}

	char *key = (char *)"ts";
	vector<string> fv = listdir((const char *)dir, "parquet");
	map<string, vector<string>> sigmap = sortby(fv, key);
	map<string, vector<string>> sigrangemap = search_range(sigmap, start_key, end_key);
	show_signalmap(sigrangemap);
}
