#include "../src/utils/utils.h"
#include <cstdio>
#include <boost/chrono.hpp>

int main(int argc, const char** args) {

	int64_t startOffset = 0;
	int64_t n = 10000000000;
	int incr = 10;
	char test[8];

	boost::chrono::system_clock::time_point start =
			boost::chrono::system_clock::now();
	for (int64_t i = 0; i < n; i += incr) {
		int64_t key = (int64_t) (i) + startOffset;
		Utils::encode_vlongWithHeader1(test, key);
		int offset = 0;
		int64_t j = Utils::decode_vlongWithHeader1(test, 8, &offset);
		if (j != key) {
			cout << "Wrong j=" << j << " key=" << key << endl;
			break;
		}
	}
	boost::chrono::duration<double> sec = boost::chrono::system_clock::now()
			- start;
	cout << "Duration encode vlong1 " << sec.count() << endl;

	start = boost::chrono::system_clock::now();
	for (int64_t i = 0; i < n; i += incr) {
		int64_t key = (int64_t) (i) + startOffset;
		Utils::encode_vlongWithHeader0(test, key);
		int offset = 0;
		int64_t j = Utils::decode_vlongWithHeader0(test, 8, &offset);
		if (j != key) {
			cout << "Wrong j=" << j << " key=" << key << endl;
			break;
		}
	}
	sec = boost::chrono::system_clock::now() - start;
	cout << "Duration encode vlong0 " << sec.count() << endl;

	start = boost::chrono::system_clock::now();
	for (int64_t i = 0; i < n; i += incr) {
		int64_t key = (int64_t) (i) + startOffset;
		Utils::encode_longWithHeader0(test, key);
		int64_t j = Utils::decode_longWithHeader(test);
		if (j != key) {
			cout << "Wrong j=" << j << " key=" << key << endl;
			break;
		}
	}
	sec = boost::chrono::system_clock::now() - start;
	cout << "Duration encode long0 " << sec.count() << endl;

	start = boost::chrono::system_clock::now();
	for (int64_t i = 0; i < n; i += incr) {
		int64_t key = (int64_t) (i) + startOffset;
		Utils::encode_longWithHeader1(test, key);
		int64_t j = Utils::decode_longWithHeader(test);
		if (j != key) {
			cout << "Wrong j=" << j << " key=" << key << endl;
			break;
		}
	}
	sec = boost::chrono::system_clock::now() - start;
	cout << "Duration encode long1 " << sec.count() << endl;
}
