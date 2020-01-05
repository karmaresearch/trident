#include <sstream>
#include <fstream>
#include <iostream>
#include <vector>
#include <cstdlib>
#include <random>
#include <chrono>
#include <array>
#include <thread>

#include <trident/loader.h>
#include <kognac/multidisklz4writer.h>

using namespace std;

int main(int argc, const char** argv) {
    int maxReadingThreads = 2;
    int parallelProcesses = 8;
    int nindices = 1;
    int partsPerFiles = parallelProcesses / maxReadingThreads;
    uint64_t ntriples = 3500000000l;
    uint64_t estimatedSize = ntriples * 32;
    uint64_t max = ntriples / parallelProcesses;
    std::string inputDir = "/Users/jacopo/Desktop/test2";
    std::chrono::system_clock::time_point start = std::chrono::system_clock::now();


    //Init params
    string tmpDir = p.tmpDir;
    const string kbDir = p.kbDir;
    int dictionaries = p.dictionaries;
    string dictMethod = p.dictMethod;
    int nindices = p.nindices;
    bool createIndicesInBlocks = false;
    bool aggrIndices = false;
    bool canSkipTables = false;
    bool sample = true;
    double sampleRate = 0.01;
    bool storePlainList = p.storePlainList;
    string remoteLocation = p.remoteLocation;
    int64_t limitSpace = p.limitSpace;
    int parallelProcesses = p.parallelThreads;
    int maxReadingThreads = p.maxReadingThreads;
    string graphTransformation = p.graphTransformation;
    bool flatTree = p.flatTree;
    //End init params

    LOG(DEBUGL) << "Insert the triples in the indices...";
    string *sTreeWriters = new string[nindices];
    TreeWriter **treeWriters = new TreeWriter*[nindices];
    for (int i = 0; i < nindices; ++i) {
        sTreeWriters[i] = tmpDir + DIR_SEP + string("tmpTree" ) + to_string(i);
        treeWriters[i] = new TreeWriter(sTreeWriters[i]);
    }

    //Use aggregated indices
    string aggr1Dir = tmpDir + DIR_SEP + string("aggr1");
    string aggr2Dir = tmpDir + DIR_SEP + string("aggr2");
    if (aggrIndices && nindices > 1) {
        Utils::create_directories(aggr1Dir);
        if (nindices > 3)
            Utils::create_directories(aggr2Dir);
    }

    //if sample is requested, create a subdir
    string sampleDir = tmpDir + DIR_SEP + string("sampledir");
    SimpleTripleWriter *sampleWriter = NULL;
    if (sample) {
        Utils::create_directories(sampleDir);
        sampleWriter = new SimpleTripleWriter(sampleDir, "input", false);
    }

    //Create n threads where the triples are sorted and inserted in the knowledge base
    Inserter *ins = kb.insert();
    LOG(DEBUGL) << "Start sortAndInsert";

    if (nindices != 6) {
        LOG(ERRORL) << "Support only 6 indices (for now)";
        throw 1;
    }

    string outputDirs[6];
    for (int i = 0; i < 6; ++i) {
        outputDirs[i] = kbDir + DIR_SEP + "p" + to_string(i);
    }

    loadKB_handleGraphTransformations(kb, graphTransformation, permDirs,
            nindices, ins, relsOwnIDs, kbDir, storeDicts);

    createIndices(parallelProcesses, maxReadingThreads,
            ins, createIndicesInBlocks,
            aggrIndices,canSkipTables, storePlainList,
            permDirs, outputDirs, aggr1Dir, aggr2Dir, treeWriters, sampleWriter,
            sampleRate,
            remoteLocation,
            limitSpace,
            totalCount,
            nindices);

    if (nindices != 6)
        nindices = 6; //restore

    for (int i = 0; i < nindices; ++i) {
        treeWriters[i]->finish();
    }

    std::chrono::duration<double> sec = std::chrono::system_clock::now() - start;
    LOG(INFOL) << "Time (sec) " << sec.count();
}
