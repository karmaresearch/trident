#include <kognac/lz4io.h>
#include <kognac/filemerger.h>
#include <kognac/utils.h>

#include <trident/kb/dictmgmt.h>
#include <trident/kb/kbconfig.h>
#include <trident/kb/inserter.h>
#include <trident/kb/memoryopt.h>
#include <trident/loader.h>

#include <boost/chrono.hpp>
#include <boost/log/trivial.hpp>

#include <iostream>
#include <cstdlib>
#include <sstream>
#include <fstream>
#include <vector>
#include <string>

using namespace std;
namespace timens = boost::chrono;

void sortAndInsert(int permutation, string inputDir,
        string POSoutputDir, TreeWriter *treeWriter, Inserter *ins) {

    BOOST_LOG_TRIVIAL(debug) << "Start inserting...";
    long ps, pp, po; //Previous values. Used to remove duplicates.
    ps = pp = po = -1;
    long count = 0;
    long countInput = 0;
    const int randThreshold = 0.01 * 100;

    auto inputmerge = Utils::getFiles(inputDir, true);
    FileMerger<Triple> merger(inputmerge, true);
    LZ4Writer *plainWriter = NULL;
    bool first = true;

    BOOST_LOG_TRIVIAL(debug) << "Parallel insert";
    std::mutex m_buffers;
    std::condition_variable cond_buffers;
    std::vector<long*> buffers;

    std::mutex m_exchange;
    std::condition_variable cond_exchange;
    std::list<std::pair<long*, int>> exchangeBuffers;

    const int sizebuffer = 4 * 1000000 * 4;
    for (int i = 0; i < 3; ++i) { //Create three buffers
        buffers.push_back(new long[sizebuffer]);
    }

    //Start one thread to merge the files in a single stream. Put the results
    //in a synchronized queue
    boost::thread t = boost::thread(
            boost::bind(Loader::parallelmerge,
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
    long prevKey = -1;

    do {
        std::unique_lock<std::mutex> le(m_exchange);
        while (exchangeBuffers.empty()) {
            cond_exchange.wait(le);
        }
        std::pair<long*, int> b = exchangeBuffers.front();
        exchangeBuffers.pop_front();
        le.unlock();

        const int currentsize = b.second;
        //Do the processing
        long *buf = b.first;

        for (int i = 0; i < currentsize; i += 4) {
            countInput++;
            const long s = buf[i];
            const long p = buf[i + 1];
            const long o = buf[i + 2];
            const long countt = buf[i + 3];
            if (count % 100000000 == 0) {
                BOOST_LOG_TRIVIAL(debug) << "..." << count << "...";
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
        long *b = buffers.back();
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
            BOOST_LOG_TRIVIAL(debug) << "Perm " << permutation << ": RowLayout" << stat->nListStrategies << " ClusterLayout " << stat->nGroupStrategies << " ColumnLayout " << stat->nList2Strategies;
            BOOST_LOG_TRIVIAL(debug) << "Perm " << permutation << ": Exact " << stat->exact << " Approx " << stat->approximate;
            BOOST_LOG_TRIVIAL(debug) << "Perm " << permutation << ": FirstElemCompr1 " << stat->nFirstCompr1 << " FirstElemCompr2 " << stat->nFirstCompr2;
            BOOST_LOG_TRIVIAL(debug) << "Perm " << permutation << ": SecondElemCompr1 " << stat->nSecondCompr1 << " SecondElemCompr2 " << stat->nSecondCompr2;
            BOOST_LOG_TRIVIAL(debug) << "Perm " << permutation << ": Diff " << stat->diff << " Nodiff " << stat->nodiff;
            BOOST_LOG_TRIVIAL(debug) << "Perm " << permutation << ": Aggregated " << stat->aggregated << " NotAggr " << stat->notAggregated;
            BOOST_LOG_TRIVIAL(debug) << "Perm " << permutation << ": NTables " << ins->getNTablesPerPartition(permutation);
            BOOST_LOG_TRIVIAL(debug) << "Perm " << permutation << ": NSkippedTables " << ins->getNSkippedTables(permutation);
        }
    }

    BOOST_LOG_TRIVIAL(debug) << "...completed. Added " << count << " triples out of " << countInput;
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
