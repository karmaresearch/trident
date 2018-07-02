#include <kognac/lz4io.h>
#include <kognac/filemerger.h>
#include <kognac/utils.h>
#include <kognac/multidisklz4reader.h>
#include <kognac/multidisklz4writer.h>
#include <kognac/triple.h>
#include <kognac/logs.h>

#include <trident/kb/dictmgmt.h>
#include <trident/kb/kbconfig.h>
#include <trident/kb/inserter.h>
#include <trident/kb/memoryopt.h>
#include <trident/loader.h>

#include <chrono>
#include <iostream>
#include <cstdlib>
#include <sstream>
#include <fstream>
#include <vector>
#include <string>

using namespace std;
namespace timens = std::chrono;

void spread(MultiDiskLZ4Reader *reader,
        int idReader,
        MultiDiskLZ4Writer *writer,
        int nparts) {
    int currentP = 0;
    int64_t count = 0;
    while (!reader->isEOF(idReader)) {
        Triple t;
        t.readFrom(idReader, reader);
        t.writeTo(currentP, writer);
        currentP = (currentP + 1) % nparts;
        count++;
        if (count % 100000000 == 0) {
            LOG(DEBUGL) << "Processed " << count;
        }
    }
    for(int i = 0; i < nparts; ++i) {
        writer->setTerminated(i);
    }
}

void split(string input, int parallelProcesses, int maxReadingThreads) {
    std::vector<string> inputfiles = Utils::getFiles(input);
    if (inputfiles.size() != 8)
        throw 10;
    MultiDiskLZ4Reader **readers = new MultiDiskLZ4Reader*[inputfiles.size()];
    for(int i = 0; i < inputfiles.size(); ++i) {
        readers[i] = new MultiDiskLZ4Reader(1, 3, 1);
        readers[i]->start();
        std::vector<string> chunk;
        chunk.push_back(inputfiles[i]);
        readers[i]->addInput(0, chunk);
    }
    MultiDiskLZ4Writer **writers = new MultiDiskLZ4Writer*[maxReadingThreads];
    int partsPerWriter = parallelProcesses / maxReadingThreads;
    for(int i = 0; i < maxReadingThreads; ++i) {
        std::vector<string> outputfiles;
        for(int j = 0; j < partsPerWriter; ++j) {
            outputfiles.push_back(input + "/input" + to_string(i * partsPerWriter + j));
        }
        writers[i] = new MultiDiskLZ4Writer(outputfiles, 3, 4);
    }

    std::thread *threads = new std::thread[inputfiles.size()];
    for(int i = 0; i < inputfiles.size(); ++i) {
        threads[i] = std::thread(spread, readers[i], 0, writers[i], partsPerWriter);
    }
    for(int i = 0; i < inputfiles.size(); ++i) {
        threads[i].join();
    }

    for(int i = 0; i < inputfiles.size(); ++i) {
        delete readers[i];
    }
    for(int i = 0; i < maxReadingThreads; ++i) {
        delete writers[i];
    }
    delete[] writers;
    delete[] readers;
    delete[] threads;
    for(auto f : inputfiles) {
        fs::remove(fs::path(f));
    }
}

int main(int argc, const char** argv) {
    string input = argv[1];
    //string inputP1 = argv[2];
    //string inputP3 = argv[3];
    //string inputP4 = argv[4];
    string inputP2 = argv[2];
    string inputP5 = argv[3];

    //Split the 8 files in p0 in 72 files
    split(input, 72, 8);

    //Create the different permutations
    //ops
    //Loader::generateNewPermutation(inputP1, inputP0, 2, 1, 0, 72, 8);
    //sop
    //Loader::generateNewPermutation(inputP3, inputP0, 0, 2, 1, 72, 8);
    //osp
    //Loader::generateNewPermutation(inputP4, inputP0, 2, 0, 1, 72, 8);

    Loader::generateNewPermutation(inputP2, input, 1, 2, 0, 72, 8);
    Loader::generateNewPermutation(inputP5, input, 1, 0, 2, 72, 8);
}
