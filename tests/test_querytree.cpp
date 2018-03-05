#include <iostream>
#include <boost/chrono.hpp>
#include <cstdlib>
#include "../src/tree/root.h"
#include "../src/tree/coordinates.h"
#include "../src/utils/propertymap.h"
#include "../src/kb/kbconfig.h"
#include "../src/kb/memoryopt.h"
#include <sstream>
#include <fstream>

using namespace std;
namespace timens = boost::chrono;

int main(int argc, const char** argv) {

	KBConfig config;
	MemoryOptimizer::optimizeForReading(1, config);

	PropertyMap map;
    	map.setBool(TEXT_KEYS, false);
    	map.setBool(TEXT_VALUES, false);
    	map.setBool(COMPRESSED_NODES, false);
    	map.setInt(LEAF_SIZE_PREALL_FACTORY,
               config.getParamInt(TREE_MAXPREALLLEAVESCACHE));
    	map.setInt(LEAF_SIZE_FACTORY, config.getParamInt(TREE_MAXLEAVESCACHE));
    	map.setInt(MAX_NODES_IN_CACHE, config.getParamInt(TREE_MAXNODESINCACHE));
    	map.setInt(NODE_MIN_BYTES, config.getParamInt(TREE_NODEMINBYTES));
    	map.setLong(CACHE_MAX_SIZE, config.getParamLong(TREE_MAXSIZECACHETREE));
    	map.setInt(FILE_MAX_SIZE, config.getParamInt(TREE_MAXFILESIZE));
    	map.setInt(MAX_N_OPENED_FILES, config.getParamInt(TREE_MAXNFILES));
    	map.setInt(MAX_EL_PER_NODE, config.getParamInt(TREE_MAXELEMENTSNODE));

    	map.setInt(LEAF_MAX_PREALL_INTERNAL_LINES,
               config.getParamInt(TREE_MAXPREALLINTERNALLINES));
    	map.setInt(LEAF_MAX_INTERNAL_LINES,
               config.getParamInt(TREE_MAXINTERNALLINES));
    	map.setInt(LEAF_ARRAYS_FACTORY_SIZE, config.getParamInt(TREE_FACTORYSIZE));
    	map.setInt(LEAF_ARRAYS_PREALL_FACTORY_SIZE,
               config.getParamInt(TREE_ALLOCATEDELEMENTS));

    	map.setInt(NODE_KEYS_FACTORY_SIZE,
               config.getParamInt(TREE_NODE_KEYS_FACTORY_SIZE));
    	map.setInt(NODE_KEYS_PREALL_FACTORY_SIZE,
               config.getParamInt(TREE_NODE_KEYS_PREALL_FACTORY_SIZE));

	string path(argv[1]);
	Root *root = new Root(path, NULL, true, map);

	boost::chrono::system_clock::time_point start =
				boost::chrono::system_clock::now();	
	int64_t nElements = 0;
	TermCoordinates currentValue;
	for(int i = 0; i < 1000000; ++i) {
		if (root->get(i*100,&currentValue)) {
			nElements += currentValue.getNElements(0);	
		}
	}
	boost::chrono::duration<double> sec = boost::chrono::system_clock::now()
				- start;
	cout << "Duration " << sec.count() * 1000 << " " << nElements << endl;
	delete root;
	return 0;
}
