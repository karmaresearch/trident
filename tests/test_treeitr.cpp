#include <trident/kb/kbconfig.h>
#include <trident/kb/inserter.h>
#include <trident/kb/memoryopt.h>
#include <trident/loader.h>
#include <trident/tree/treeitr.h>

#include <iostream>
#include <cstdlib>
#include <sstream>
#include <fstream>
#include <vector>
#include <string>

using namespace std;
namespace timens = boost::chrono;

int main(int argc, const char** argv) {
        //Initialize the tree
    KBConfig config;
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

        Root root(string(argv[1]), NULL, true, map);
    TreeItr *itr = root.itr();
    TermCoordinates coord;
    while (itr->hasNext()) {
        long key = itr->next(&coord);
        std::cout << key << endl;
    }

    delete itr;
}
