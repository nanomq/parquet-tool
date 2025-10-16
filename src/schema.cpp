#include <schema.h>
#include <string.h>
#include <cstring>
#include <vector>
#include <fstream>

using namespace std;

vector<string>
read_schemafile_to_vec(char *fname)
{
	vector<string> vec;
	ifstream ifs;
	ifs.open(fname);
	if (!ifs.is_open())
		return vec;

	string line;
	string deli = "\t";
	getline(ifs, line);

	size_t start = 0;
	size_t end = line.find(deli);
	while (end != string::npos) {
		vec.push_back(line.substr(start, end - start));
		start = end + deli.length();
		end = line.find(deli, start);
	}

	ifs.close();
	return vec;
}

string
schema_cat(vector<string>& schema_vec, vector<string>& src)
{
	string res = "";
	return res;
}
