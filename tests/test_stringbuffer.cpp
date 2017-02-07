#include "../src/tree/stringbuffer.h"
#include <cstdlib>
#include <vector>
#include <algorithm>

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

char **stringArray = NULL;
int *sizeArray = NULL;
long *positions = NULL;
StringBuffer *b = NULL;
int n = 50000000;
vector<int> v(n);

bool cmp1(int idx1, int idx2) {
	return strcmp(stringArray[idx1], stringArray[idx2]) < 0;
}

bool cmp2(int idx1, int idx2) {
	bool result = b->cmp(positions[idx1], stringArray[v[idx2]], strlen(stringArray[v[idx2]])) < 0;
	return result;
}

int main(int argc, const char** argv) {
	b = new StringBuffer(string("/Users/jacopo/Desktop/"), false, 1000,
			1 * 1024 * 1024);

	//1- Create an array of n strings of length between 1 and x.
	//Store and re-read them.
	int x = 20;
	stringArray = new char*[n];
	sizeArray = new int[n];
	positions = new long[n];
	fillArray(stringArray, sizeArray, n, x);
	cout << "Finished filling the array" << endl;

	for (int i = 0; i < n; ++i) {
		v.push_back(i);
	}
	std::sort(v.begin(), v.end(), cmp1);
	cout << "Sorted array" << endl;

	//2- Add all of them
	for (int i = 0; i < n; ++i) {
		positions[i] = b->getSize();
		int idx = v[i];
		if (i % 10000 == 0)
			cout << "Insert " << i << "th record" << endl;
		b->append(stringArray[idx], sizeArray[idx]);
	}

	cout << "Query the strings" << endl;

	//3- Query all of them
	for (int i = 0; i < n; ++i) {
		int sizeEl;
		int idx = v[i];
		char *el = b->get(positions[i], sizeEl);
		char *origString = stringArray[idx];
		if (strlen(origString) != sizeEl) {
			cerr << "Error string " << i << " " << origString << " " << el
					<< endl;
			i = n;
			break;
		}
		for (int j = 0; j < sizeEl; ++j) {
			if (el[j] != origString[j]) {
				cerr << "Error string " << i << " " << origString << " " << el
						<< endl;
				i = n;
				break;
			}
		}
	}

	delete b;

	//4- Repeat the test in readonly mode
	cout << "Repeat the query the strings in read-only mode" << endl;

	b = new StringBuffer(string("/Users/jacopo/Desktop/"), true, 1000,
			1 * 1024 * 1024);
	for (int i = 0; i < n; ++i) {
		int sizeEl;
		int idx = v[i];
		char *el = b->get(positions[i], sizeEl);
		char *origString = stringArray[idx];
		if (strlen(origString) != sizeEl) {
			cerr << "Error string " << i << " " << origString << " " << el
					<< endl;
			i = n;
			break;
		}
		for (int j = 0; j < sizeEl; ++j) {
			if (el[j] != origString[j]) {
				cerr << "Error string " << i << " " << origString << " " << el
						<< endl;
				i = n;
				break;
			}
		}
	}

	//5- Compare the values
	cout << "Sorting the strings" << endl;
	std::vector<int> indices2(n);
	for (int i = 0; i < n; ++i) {
		indices2[i] = i;
	}
	std::sort(indices2.begin(), indices2.end(), cmp2);

	cout << "Check elements are the same" << endl;
	for (int i = 0; i < n; ++i) {
		if (indices2[i] != i) {
			cout << "Error" << endl;
		}

//		const char *s1 = stringArray[v[i]];
//		const char *s2 = stringArray[indices2[i]];
//		int s = sizeArray[indices2[i]];
//		for (int j = 0; j < s; ++j) {
//			if (s1[j] != s2[j]) {
//				cerr << "Error" << endl;
//				exit(1);
//			}
//		}
	}

	cout << "Delete KB";
	delete b;
	cout << "... done" << endl;

	//Delete everything
	for (int i = 0; i < n; ++i) {
		delete[] stringArray[i];
	}
	delete[] positions;
	delete[] stringArray;
	delete[] sizeArray;
}

