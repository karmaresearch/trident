#include <kognac/lz4io.h>
#include <kognac/filemerger.h>
#include <kognac/utils.h>
#include <kognac/logs.h>

#include <trident/kb/dictmgmt.h>
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

void sortAndInsert(int permutation, string inputDir,
        string POSoutputDir, TreeWriter *treeWriter, Inserter *ins) {

    LOG(DEBUGL) << "Start inserting...";
    int64_t ps, pp, po; //Previous values. Used to remove duplicates.
    ps = pp = po = -1;
    int64_t count = 0;
    int64_t countInput = 0;
    const int randThreshold = 0.01 * 100;

    auto inputmerge = Utils::getFiles(inputDir, true);
    FileMerger<Triple> merger(inputmerge, true);
    LZ4Writer *plainWriter = NULL;
    bool first = true;

    LOG(DEBUGL) << "Parallel insert";
    std::mutex m_buffers;
    std::condition_variable cond_buffers;
    std::vector<int64_t*> buffers;

    std::mutex m_exchange;
    std::condition_variable cond_exchange;
    std::list<std::pair<int64_t*, int>> exchangeBuffers;

    const int sizebuffer = 4 * 1000000 * 4;
    for (int i = 0; i < 3; ++i) { //Create three buffers
        buffers.push_back(new int64_t[sizebuffer]);
    }

    //Start one thread to merge the files in a single stream. Put the results
    //in a synchronized queue
    std::thread t = std::thread(
            std::bind(Loader::parallelmerge,
                &merger,
                sizebuffer,
                &buffers,
                &m_buffers,
                &cond_buffers,
                &exchangeBuffers,
                &m_exchange,
                &cond_exchange));

    //I read the stream and process it
    int countrandom = 0;
    int64_t prevKey = -1;

    do {
        std::unique_lock<std::mutex> le(m_exchange);
        while (exchangeBuffers.empty()) {
            cond_exchange.wait(le);
        }
        std::pair<int64_t*, int> b = exchangeBuffers.front();
        exchangeBuffers.pop_front();
        le.unlock();

        const int currentsize = b.second;
        //Do the processing
        int64_t *buf = b.first;

        for (int i = 0; i < currentsize; i += 4) {
            countInput++;
            const int64_t s = buf[i];
            const int64_t p = buf[i + 1];
            const int64_t o = buf[i + 2];
            const int64_t countt = buf[i + 3];
            if (count % 100000000 == 0) {
                LOG(DEBUGL) << "..." << count << "...";
            }

            if (o != po || p != pp || s != ps) {
                count++;
                ins->insert(permutation, s, p, o, countt, NULL,
                        treeWriter,
                        false,
                        false);
                ps = s;
                pp = p;
                po = o;

                prevKey = s;
            }
        }

        //Release the buffer
        std::unique_lock<std::mutex> l(m_buffers);
        buffers.push_back(b.first);
        l.unlock();
        cond_buffers.notify_one();

        //Exit the loop if it was the last buffer
        if (currentsize < sizebuffer) {
            break;
        }
    } while (true);

    //Exit
    t.join();
    while (!buffers.empty()) {
        int64_t *b = buffers.back();
        buffers.pop_back();
        delete[] b;
    }

    ins->flush(permutation, NULL, treeWriter, false, false);

    if (plainWriter != NULL) {
        delete plainWriter;
    }

    if (true) {
        Statistics *stat = ins->getStats(permutation);
        if (stat != NULL) {
            LOG(DEBUGL) << "Perm " << permutation << ": RowLayout" << stat->nListStrategies << " ClusterLayout " << stat->nGroupStrategies << " ColumnLayout " << stat->nList2Strategies;
            LOG(DEBUGL) << "Perm " << permutation << ": Exact " << stat->exact << " Approx " << stat->approximate;
            LOG(DEBUGL) << "Perm " << permutation << ": FirstElemCompr1 " << stat->nFirstCompr1 << " FirstElemCompr2 " << stat->nFirstCompr2;
            LOG(DEBUGL) << "Perm " << permutation << ": SecondElemCompr1 " << stat->nSecondCompr1 << " SecondElemCompr2 " << stat->nSecondCompr2;
            LOG(DEBUGL) << "Perm " << permutation << ": Diff " << stat->diff << " Nodiff " << stat->nodiff;
            LOG(DEBUGL) << "Perm " << permutation << ": Aggregated " << stat->aggregated << " NotAggr " << stat->notAggregated;
            LOG(DEBUGL) << "Perm " << permutation << ": NTables " << ins->getNTablesPerPartition(permutation);
            LOG(DEBUGL) << "Perm " << permutation << ": NSkippedTables " << ins->getNSkippedTables(permutation);
        }
    }

    LOG(DEBUGL) << "...completed. Added " << count << " triples out of " << countInput;
}

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

    sortAndInsert(0, input, "", treeWriter, ins);
    delete treeWriter;
}
