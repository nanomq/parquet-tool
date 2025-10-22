#include <schema.h>
#include <string.h>
#include <cstring>
#include <cstdint>
#include <vector>
#include <fstream>
#include <utility>
#include <algorithm>

#include <log.h>

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

static inline void
memcpy_bigendian(void *dst, void *src, uint32_t len)
{
	for (uint32_t i = 0; i < len; i++) {
		((uint8_t *)dst)[i] = ((uint8_t *)src)[len - i - 1];
	}
	return;
}

static inline void
append_bigendian(string& src, string& sub)
{
	for (int i=sub.size()-1; i>=0; --i)
		src.push_back(sub[i]);
}

static inline void
append_bigendian_i64(string& src, int64_t n)
{
	uint8_t *arr = (uint8_t *)&n;
	for (int i=sizeof(int64_t)-1; i>=0; --i)
		src.push_back((char)arr[i]);
}

static inline void
append_bigendian_u16(string& src, uint16_t n)
{
	uint8_t *arr = (uint8_t *)&n;
	for (int i=sizeof(uint16_t)-1; i>=0; --i)
		src.push_back((char)arr[i]);
}

static inline void
append_bigendian_u8(string& src, uint8_t n)
{
	uint8_t *arr = (uint8_t *)&n;
	for (int i=sizeof(uint8_t)-1; i>=0; --i)
		src.push_back((char)arr[i]);
}

static uint16_t
hexstring_to_uint16(string& hex)
{
	const char *hexstr = hex.c_str();
	uint16_t value = 0;

	while (*hexstr) {
		char c = *hexstr++;
		uint8_t digit;

		if (isdigit(c)) {
			digit = c - '0';
		} else if (c >= 'a' && c <= 'f') {
			digit = c - 'a' + 10;
		} else if (c >= 'A' && c <= 'F') {
			digit = c - 'A' + 10;
		} else {
			return 0;
		}

		value = (value << 4) | digit;
	}

	return value;
}

static string
uint16_to_hexstring(uint16_t num)
{
	string res;
	char hex_string[6];
	snprintf(hex_string, 6, "%04x", num);
	res.append(hex_string);
	return res;
}

string
schema_cat(vector<string>& schema_vec, int64_t ts, vector<string>& src)
{
	string res;
	append_bigendian_i64(res, ts);
	uint16_t row_len = 0;
	for (string& s: src)
		row_len += s.size();
	append_bigendian_u16(res, row_len);
	for (int i=0; i<schema_vec.size(); ++i) {
		append_bigendian_u8(res, 0x55);
		uint16_t packet_type_id = hexstring_to_uint16(schema_vec[i]);
		append_bigendian_u16(res, packet_type_id);
		uint8_t  packet_sz = (uint8_t) src[i].length();
		append_bigendian_u8(res, packet_sz);
		append_bigendian(res, src[i]);
	}
	return res;
}

bool cmp_schema_tsdiff(const pair<string, int>& a, const pair<string, int>& b) {
	return a.first.data()[0] < b.first.data()[0];
}

vector<pair<string, int>>
schema_sort(vector<string> row)
{
	vector<pair<string, int>> res;
	for (int i=0; i<row.size(); ++i) {
		string r = row[i];
		if (r.size()) {
			size_t pos = 0;
			while (pos < r.size()) {
				if (pos + 3 >= r.size()) {
					break;
				}
				pos += 1; // tsdiff
				size_t len_i16 = (size_t)(((uint16_t)(r.data()[pos + 0]) << 8) + (uint16_t)r.data()[pos + 1]);
				if (pos + 2 + len_i16 > r.size()) {
					pterr("Invalid schemaed payload was found:");
					for (int z=0; z<r.size(); ++z)
						fprintf(stderr, "%02x", (uint8_t)r.data()[z]);
					fprintf(stderr, "\n");
				}

				string pld = r.substr(pos - 1, pos + 2 + len_i16);

				auto p = make_pair(pld, i);
				res.push_back(p);

				pos += (2 + len_i16);
			}
		}
	}
	sort(res.begin(), res.end(), cmp_schema_tsdiff);
	return res;
}
