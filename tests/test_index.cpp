#include <iostream>
#include <boost/chrono.hpp>
#include <cstdlib>
#include "../src/tree/root.h"
#include "../src/tree/coordinates.h"
#include "../src/utils/propertymap.h"
#include <sstream>
#include <fstream>

using namespace std;
namespace timens = boost::chrono;

int main(int argc, const char** argv) {

	//Create the tree
	PropertyMap config;
	config.setBool(TEXT_KEYS, false);
	config.setBool(TEXT_VALUES, false);
	config.setBool(COMPRESSED_NODES, false);
	config.setInt(MAX_EL_PER_NODE, 2048);
	config.setInt(FILE_MAX_SIZE, 64 * 1024 * 1024);
	config.setLong(CACHE_MAX_SIZE, 1024 * 1024 * 1024);
	config.setInt(NODE_MIN_BYTES, 1);
	config.setInt(MAX_NODES_IN_CACHE, 100);

	config.setInt(LEAF_SIZE_FACTORY, 2050);
	config.setInt(LEAF_SIZE_PREALL_FACTORY, 2050);
	config.setInt(LEAF_ARRAYS_FACTORY_SIZE, 10);
	config.setInt(LEAF_ARRAYS_PREALL_FACTORY_SIZE, 2050);
	config.setInt(LEAF_MAX_INTERNAL_LINES, 2050);
	config.setInt(LEAF_MAX_PREALL_INTERNAL_LINES, 5000);
	config.setInt(NODE_KEYS_FACTORY_SIZE, 10);
	config.setInt(NODE_KEYS_PREALL_FACTORY_SIZE, 2050);

	Root *root = new Root("/Users/jacopo/Desktop/kb/tree", NULL, false, config);
	TreeItr *itr = root->itr();
	TermCoordinates t;
	int64_t count = 0;
	int keys = 0;
	while (itr->hasNext()) {
		keys = (keys+1) % 100;
		int64_t k = itr->next(&t);
		if (keys == 0) {
			cout << k << endl;
		}
		++count;
	}
	cout << count << endl;

	delete itr;
	delete root;
	return 0;
}
