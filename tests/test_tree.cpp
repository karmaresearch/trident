#include <iostream>
#include <chrono>
#include <cstdlib>

#include <trident/tree/root.h>
#include <trident/utils/propertymap.h>
#include <sstream>
#include <fstream>

using namespace std;

int main(int argc, const char** argv) {

	int n = 10000000;
    std::vector<std::pair<int64_t, int64_t>> pairs;
	for (int i = 0; i < n; ++i) {
        pairs.push_back(std::make_pair(i + 1024, 0));
	}
	cout << "Finished filling the array" << endl;

	//Create the tree
	PropertyMap config;
	config.setBool(TEXT_KEYS, false);
	config.setBool(TEXT_VALUES, true);
	config.setBool(COMPRESSED_NODES, false);
	config.setInt(MAX_EL_PER_NODE, 2048);
	config.setInt(FILE_MAX_SIZE, 1 * 1024 * 1024);
	config.setLong(CACHE_MAX_SIZE, 4 * 1024 * 1024);
	config.setInt(NODE_MIN_BYTES, 1);
	config.setInt(MAX_NODES_IN_CACHE, 10);

	config.setInt(LEAF_SIZE_FACTORY, 2050);
	config.setInt(LEAF_SIZE_PREALL_FACTORY, 2050);
	config.setInt(LEAF_ARRAYS_FACTORY_SIZE, 10);
	config.setInt(LEAF_ARRAYS_PREALL_FACTORY_SIZE, 2050);
	config.setInt(LEAF_MAX_INTERNAL_LINES, 2050);
	config.setInt(LEAF_MAX_PREALL_INTERNAL_LINES, 5000);
	config.setInt(NODE_KEYS_FACTORY_SIZE, 10);
	config.setInt(NODE_KEYS_PREALL_FACTORY_SIZE, 2050);
    config.setInt(MAX_N_OPENED_FILES, 2048);

	Root *root = new Root("/Users/jacopo/Desktop/tree", NULL, false, config);

	//Populate the tree
	std::chrono::system_clock::time_point start =
				std::chrono::system_clock::now();
	cout << "Start inserting" << endl;
	for (int i = 0; i < n; i++) {
		if (i % 1000000 == 0) {
			cout << "Inserting " << pairs[i].first << endl;
		}
		root->put(pairs[i].first, pairs[i].second);
	}

    //Add the first 1024
    for(int i = 0; i < 1024; ++i) {
        cout << "Inserting " << i << endl;
        root->put((int64_t)i, (int64_t)0);
    }

	std::chrono::duration<double> sec = std::chrono::system_clock::now()
				- start;
		cout << "Duration insertion " << sec.count() * 1000 << endl;

	delete root;
	return 0;
}
