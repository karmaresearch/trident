#include <trident/kb/kbconfig.h>
#include <trident/kb/inserter.h>
#include <trident/kb/memoryopt.h>
#include <trident/loader.h>

#include <iostream>
#include <cstdlib>
#include <sstream>
#include <fstream>
#include <vector>
#include <string>

using namespace std;
namespace timens = boost::chrono;

class MyTreeInserter: public TreeInserter {
private:
public:
	MyTreeInserter() {
	}

	void addEntry(nTerm key, int64_t nElements, short file, int pos,
			char strategy) {
	}
};
int main(int argc, const char** argv) {
    KBConfig config;
    config.setParamInt(DICTPARTITIONS, 1);
    config.setParamInt(NINDICES, 6);
    config.setParamBool(AGGRINDICES, false);
    config.setParamBool(USEFIXEDSTRAT, false);
    config.setParamInt(FIXEDSTRAT, 96);
    config.setParamInt(THRESHOLD_SKIP_TABLE, 64);
    MemoryOptimizer::optimizeForWriting(10000000000l, config);
    KB kb(argv[1], false, false, true, config);
    MyTreeInserter treeInserter;

    Inserter *ins = kb.insert();
    for(int64_t i = 0; i < 1000000000; i += 2) {
        ins->insert(IDX_SPO, 0, i, i + 1, 1, NULL, NULL, false, false);
        if (i % 10000000 == 0)
            cout << "Idx " << i << endl;
    }
    ins->flush(IDX_SPO, NULL, &treeInserter, false, false);

    ins->stopInserts(IDX_SPO);
    delete ins;
}
