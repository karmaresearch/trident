#include <kognac/lz4io.h>

#include <boost/chrono.hpp>

#include <iostream>
#include <cstdlib>
#include <sstream>
#include <fstream>
#include <vector>
#include <string>

#include <kognac/compressor.h>
#include <trident/kb/dictmgmt.h>
#include <trident/kb/kbconfig.h>
#include <trident/kb/inserter.h>
#include <trident/kb/memoryopt.h>
#include <trident/loader.h>
#include <trident/tree/stringbuffer.h>

using namespace std;
namespace timens = boost::chrono;

int main(int argc, const char** argv) {
    {
        KBConfig config;
        config.setParamInt(DICTPARTITIONS, 1);
        config.setParamInt(NINDICES, 6);
        config.setParamBool(AGGRINDICES, false);
        config.setParamBool(USEFIXEDSTRAT, false);
        config.setParamInt(FIXEDSTRAT, 96);
        config.setParamInt(THRESHOLD_SKIP_TABLE, 64);
        MemoryOptimizer::optimizeForWriting(10000000000l, config);
        KB kb(argv[1], false, false, true, config);

        nTerm *maxValues = new nTerm[1];
        Loader::insertDictionary(0, kb.getDictMgmt(), argv[2],
                true, true, false, maxValues);
        kb.closeMainDict();
    }
}
