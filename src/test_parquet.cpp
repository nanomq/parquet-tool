#include <assert.h>
#include <parquet.h>
#include <map>
#include <string>
#include <any>
#include <list>

using namespace std;

int
main()
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
