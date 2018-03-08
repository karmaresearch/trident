#include <iostream>
#include <boost/chrono.hpp>
#include <cstdlib>
#include "../src/tree/root.h"
#include "../src/tree/stringbuffer.h"
#include "../src/utils/propertymap.h"
#include <sstream>
#include <fstream>

using namespace std;
namespace timens = boost::chrono;

void fillArray(char **array, int *size, int sizeArray, int sizeString) {
	const char *URIs[] = { "<http://very_long_domain1.com/",
			"<http://long_domain2.com/", "<http://short_domain1.com/" };
	int lengthURIs[3];
	for (int i = 0; i < 3; ++i) {
		lengthURIs[i] = strlen(URIs[i]);
	}

	for (int i = 0; i < sizeArray; ++i) {
		bool isUri = rand() % 2;
		int len = rand() % sizeString + 1;
		int lengthURI;
		const char *URI = NULL;
		if (isUri) {
			int idxUri = rand() % 3;
			URI = URIs[idxUri];
			lengthURI = lengthURIs[idxUri];
			len += lengthURI + 1;
		}
		char *string = new char[len];

		int j = 0;
		int end = len - 1;
		if (isUri) {
			strcpy(string, URI);
			string[len - 2] = '>';
			j = lengthURI;
			end = len - 2;
		}

		for (; j < end; ++j) {
			string[j] = rand() % 25 + 65;
		}
		string[len - 1] = '\0';
		size[i] = len - 1;
		array[i] = string;
	}
}

void init(int64_t counters[16]) {
	for (int i = 0; i < 16; ++i) {
		counters[i] = 0;
	}
}

void print(int64_t counters[16]) {
	for (int i = 0; i < 16; ++i) {
		cout << counters[i] << " ";
	}
	cout << endl;
}

int main(int argc, const char** argv) {

	int n = 20000000;
	int x = 50;
	char **array = new char*[n];
	int *sizeArray = new int[n];
	fillArray(array, sizeArray, n, x);
	cout << "Finished filling the array" << endl;

	cout << "Testing speed of the dbj2 hash function" << endl;
	int64_t counters[16];
	init(counters);
	boost::chrono::system_clock::time_point start =
			boost::chrono::system_clock::now();
	int64_t sum = 0;
	for (int i = 0; i < n; ++i) {
		counters[abs(Hashes::dbj2(array[i])) % 16]++;
	}
	boost::chrono::duration<double> sec = boost::chrono::system_clock::now()
			- start;
	cout << "Time: " << sec.count() * 1000 << "ms sum=" << sum << endl;
	print(counters);

	cout << "Testing speed of the fnv1a hash function" << endl;
	init(counters);
	start = boost::chrono::system_clock::now();
	for (int i = 0; i < n; ++i) {
		counters[abs(Hashes::fnv1a(array[i])) % 16]++;
	}
	sec = boost::chrono::system_clock::now() - start;
	cout << "Time: " << sec.count() * 1000 << "ms sum=" << sum << endl;
	print(counters);

	cout << "Testing speed of the murmum3 hash function" << endl;
	init(counters);
	start = boost::chrono::system_clock::now();
	for (int i = 0; i < n; ++i) {
		int hashCode = Hashes::murmur3(array[i], sizeArray[i]);
		counters[abs(hashCode) % 16]++;
	}
	sec = boost::chrono::system_clock::now() - start;
	cout << "Time: " << sec.count() * 1000 << "ms sum=" << sum << endl;
	print(counters);

	return 0;
}
