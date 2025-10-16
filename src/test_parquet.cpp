#include <assert.h>
#include <parquet.h>
#include <map>
#include <string>
#include <any>
#include <list>
#include <vector>

#include <stdio.h>

using namespace std;

void
test_r_schema_parquet()
{
	char *output = (char *)"./test_schema.parquet";
	map<string, any> m = read_parquet_schema(output, NULL, 0);
	list<int64_t> lk = any_cast<list<int64_t>>(m["key"]);
	vector<vector<string>> larr = any_cast<vector<vector<string>>>(m["schemadata"]);
	list<int64_t>::iterator lk_it = lk.begin();
	for (int i=0; i<larr.size(); ++i) {
		vector<string>& pld = larr[i];
		printf("%lld", *lk_it);
		lk_it++;
		for (auto it = pld.begin(); it != pld.end(); it ++) {
			printf(", ");
			string& ele = *it;
			for (int n=0; n<ele.size(); ++n)
				printf("%02x", (uint8_t)ele.c_str()[n]);
			if (ele.size() == 0)
				printf("-");
		}
		printf("\n");
	}
}

void
test_rw_parquet()
{
	char *output = (char *)"./test.parquet";
	const char *footkey = NULL;
	const char *col1key = NULL;
	const char *col2key = NULL;
	map<string, any> m1;
	list<int64_t> lk;
	list<string> lp;
	lk.push_back(6); lp.push_back("aaa");
	lk.push_back(7); lp.push_back("bbb");
	lk.push_back(8); lp.push_back("ccc");
	m1["key"] = lk; m1["data"] = lp;

	assert(0 == write_parquet(output, footkey, col1key, col2key, m1));

	map<string, any> m2 = read_parquet(output, footkey, col1key, col2key);
	list<int64_t> lk2 = any_cast<list<int64_t>>(m2["key"]);
	list<string>  lp2 = any_cast<list<string>>(m2["data"]);
	for (auto it = lk.begin(), it2 = lk2.begin(); it != lk.end(); it ++, it2 ++)
		assert(*it == *it2);
	for (auto it = lp.begin(), it2 = lp2.begin(); it != lp.end(); it ++, it2 ++)
		assert(0 == it->compare(it2->c_str()));
}

void
test_search()
{
	char *output = (char *)"./testdir/nanomq_test-6~10_555.parquet";
	char *footkey = NULL;
	char *col1key = NULL;
	char *col2key = NULL;
	map<string, any> m1;
	list<int64_t> lk;
	list<string> lp;
	lk.push_back(6); lp.push_back("aaa");
	lk.push_back(7); lp.push_back("bbb");
	lk.push_back(20); lp.push_back("xxx");
	lk.push_back(8); lp.push_back("ccc");
	m1["key"] = lk; m1["data"] = lp;

	assert(0 == write_parquet(output, footkey, col1key, col2key, m1));

	char *lower = (char *)"6";
	char *upper = (char *)"9";
	map<string, any> m2 = pt_search((char*)"test", lower, upper, (char*)"./testdir/", footkey, col1key, col2key);
	list<int64_t> lk2 = any_cast<list<int64_t>>(m2["key"]);
	for (auto it2 = lk2.begin(); it2 != lk2.end(); it2 ++) {
		assert(stoll(lower) <= *it2);
		assert(*it2 <= stoll(upper));
	}
}

void
test_fuzz()
{
	char *output = (char *)"./testdir/nanomq_test-6~10_555.parquet";
	char *footkey = NULL;
	char *col1key = NULL;
	char *col2key = NULL;
	map<string, any> m1;
	list<int64_t> lk;
	list<string> lp;
	lk.push_back(6); lp.push_back("aaa");
	lk.push_back(8); lp.push_back("bbb");
	lk.push_back(7); lp.push_back("xxx");
	lk.push_back(9); lp.push_back("ccc");
	m1["key"] = lk; m1["data"] = lp;

	assert(0 == write_parquet(output, footkey, col1key, col2key, m1));

	map<string, any> m2 = pt_fuzz((char*)"test", (char*)"5", (char*)"10", (char*)"./testdir/", footkey, col1key, col2key);
	list<int64_t> lk2 = any_cast<list<int64_t>>(m2["key"]);
	int lastkey = 0;
	for (auto it2 = lk2.begin(); it2 != lk2.end(); it2 ++) {
		if (lastkey == 0) {
			lastkey = *it2;
		} else {
			assert(lastkey < *it2);
			lastkey = *it2;
		}
	}
}

int
main()
{
	test_r_schema_parquet();
	test_rw_parquet();
	test_search();
	test_fuzz();
}
