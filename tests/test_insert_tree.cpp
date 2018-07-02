#include <kognac/compressor.h>
#include <trident/kb/dictmgmt.h>
#include <trident/kb/kbconfig.h>
#include <trident/kb/inserter.h>
#include <trident/kb/memoryopt.h>
#include <trident/loader.h>
#include <trident/tree/stringbuffer.h>

#include <kognac/logs.h>

#include <fstream>

using namespace std;

int main(int argc, const char** argv) {

    Logger::setMinLevel(DEBUGL);
	//First test all 6 permutations
	char *supportBuffer;
	supportBuffer = new char[23];
	for(int i = 0; i < 6; ++i) {
		string perm = string(argv[2]) + string("/tmpTree") + to_string(i);
		ifstream s;
		s.open(perm);
		LOG(DEBUGL) << perm;
		int64_t prevKey = -1;
		while (s.read(supportBuffer, 23)) {
			short file = Utils::decode_short((const char*) supportBuffer + 16);
       			int64_t key = Utils::decode_long(supportBuffer);
        		int64_t nElements = Utils::decode_long(supportBuffer + 8);
        		int pos = Utils::decode_int(supportBuffer + 18);
        		char strat = supportBuffer[22];
			if (key <= prevKey) {
				LOG(DEBUGL) << "key=" << key << " prevkey=" << prevKey;
			}	
			prevKey = key;
		}
		s.close();
	}

    KBConfig config;
    config.setParamInt(DICTPARTITIONS, 1);
    config.setParamInt(NINDICES, 6);
    config.setParamBool(AGGRINDICES, false);
    config.setParamBool(USEFIXEDSTRAT, false);
    config.setParamInt(FIXEDSTRAT, 96);
    config.setParamInt(THRESHOLD_SKIP_TABLE, 64);
    MemoryOptimizer::optimizeForWriting(10000000000l, config);
    KB kb(argv[1], false, false, true, config);

    Loader loader;
    loader.testLoadingTree(argv[2], kb.insert(), 6);
}
