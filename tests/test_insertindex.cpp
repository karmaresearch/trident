//#include <kognac/compressor.h>

#include <trident/kb/dictmgmt.h>
#include <trident/kb/kbconfig.h>
#include <trident/kb/inserter.h>
#include <trident/kb/memoryopt.h>
#include <trident/loader.h>

#include <boost/chrono.hpp>

#include <iostream>
#include <cstdlib>
#include <sstream>
#include <fstream>
#include <vector>
#include <string>

using namespace std;
namespace timens = boost::chrono;

/*void sortAndInsert(int permutation, bool inputSorted, string inputDir,
                   string POSoutputDir, TreeWriter *treeWriter, Inserter *ins) {
    SimpleTripleWriter *posWriter = NULL;

    BOOST_LOG_TRIVIAL(debug) << "Start inserting...";
    long ps, pp, po; //Previous values. Used to remove duplicates.
    ps = pp = po = -1;
    long count = 0;
    long countInput = 0;
    FileMerger<Triple> merger(Utils::getFiles(inputDir));
    while (!merger.isEmpty()) {
        Triple t = merger.get();
        countInput++;
        if (count % 10000000 == 0) {
            BOOST_LOG_TRIVIAL(debug) << "... unique " << count << " total " << countInput;
        }

        if (t.o != po || t.p != pp || t.s != ps) {
            count++;
            ins->insert(permutation, t.s, t.p, t.o, posWriter, treeWriter);
            ps = t.s;
            pp = t.p;
            po = t.o;
        }
    }
    ins->flush(permutation, posWriter, treeWriter);

    if (permutation == 1) {
        delete posWriter;
    }

    BOOST_LOG_TRIVIAL(debug) << "...completed. Added " << count << " triples out of " << countInput;
}*/

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
    Inserter *ins = kb.insert();
    TreeWriter *treeWriter = new TreeWriter(string(argv[1]) + "/tmpTree");
    string input = argv[2];

    //Do only perm 0
    Loader::sortAndInsert(0, 6, false, input, NULL, treeWriter, ins,
                          false, false, false, NULL, 0, false, NULL);
    delete treeWriter;
}
