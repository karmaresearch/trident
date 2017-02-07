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

int main(int argc, const char** argv) {

	int n = 20000000;
	int x = 20;
	char **array = new char*[n];
	int *sizeArray = new int[n];
//	fillArray(array, sizeArray, n, x);
	cout << "Finished filling the array" << endl;

	//Create the tree
	PropertyMap config;
	config.setBool(TEXT_KEYS, false);
	config.setBool(TEXT_VALUES, true);
	config.setBool(COMPRESSED_NODES, false);
	config.setInt(MAX_EL_PER_NODE, 2048);
	config.setInt(FILE_MAX_SIZE, 64 * 1024 * 1024);
	config.setLong(CACHE_MAX_SIZE, 1024 * 1024 * 1024);
	config.setInt(NODE_MIN_BYTES, 20048);
	config.setInt(MAX_NODES_IN_CACHE, 10000);

	config.setInt(LEAF_SIZE_FACTORY, 10000);
	config.setInt(LEAF_SIZE_PREALL_FACTORY, 1000);
	config.setInt(LEAF_MAX_INTERNAL_LINES, 1000);
	config.setInt(LEAF_MAX_PREALL_INTERNAL_LINES, 1000);
	config.setInt(NODE_KEYS_FACTORY_SIZE, 1);
	config.setInt(NODE_KEYS_PREALL_FACTORY_SIZE, 1);

	StringBuffer bb("/Users/jacopo/Desktop", false, false, 100, 1000000000);
	Root *root = new Root("/Users/jacopo/Desktop", &bb, false, config);

	//Populate the dictionary
	cout << "Start inserting" << endl;
	for (int i = 0; i < n; i+=4) {
		if (i % 1000 == 0) {
			cout << "Inserting " << i << endl;
		}
//		nTerm value = i;
//		root->insertIfNotExists((tTerm*) array[i], sizeArray[i], value);
		root->put(i,i);
	}

	delete root;
	root = new Root("/Users/jacopo/Desktop", &bb, true, config);

	//Get some numbers
	for (int i = 0; i < n; i+=4) {
//		long key = random() % n;
		long key = i;
		long value;
		if (root->get((nTerm)key, value)) {
			if (key != value) {
//				//Check if this key appears multiple times.
//				bool ok = false;
//				for(int j = n - 1; j > i; j--) {
//					if (strcmp(array[i], array[j]) == 0) {
//						ok = true;
//						break;
//					}
//				}
//				if (!ok) {
					cout << "Problem" << endl;
//				}
				break;
			}
		} else {
			cout << "Not found" << endl;
			break;
		}
	}

	//Delete everything
	cout << "Delete everything" << endl;
	for (int i = 0; i < n; ++i) {
		delete[] array[i];
	}
	delete[] sizeArray;
	delete root;

	return 0;
}
