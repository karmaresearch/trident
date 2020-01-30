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

void populateInput(MultiDiskLZ4Writer *writer, int idx, uint64_t max) {
    uint64_t counter = 0;
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<int64_t> dis(0, 8000*1000*1000l);
    while (counter++ < max) {
        writer->writeLong(idx, dis(gen));
        writer->writeLong(idx, dis(gen));
        writer->writeLong(idx, dis(gen));
        if (counter % 1000000 == 0) {
            LOG(DEBUGL) << "Created " << counter << " records " << max;
        }
    }
    writer->setTerminated(idx);
}

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

    /*//Populate the files with new data
      MultiDiskLZ4Writer **writers = new MultiDiskLZ4Writer*[maxReadingThreads];
      int idx = 0;
      for (int i = 0; i < maxReadingThreads; ++i) {
      std::vector<string> outputchunk;
      for(int j = 0; j < partsPerFiles; ++j) {
      std::string filepath = outputdir + DIR_SEP + "input-" + to_string(idx++);
      outputchunk.push_back(filepath);
      }
      writers[i] = new MultiDiskLZ4Writer(outputchunk, 3, 4);
      }

    //Start threads
    std::thread *threads = new std::thread[parallelProcesses];
    for(int i = 0; i < parallelProcesses; ++i) {
    int idx1 = i % maxReadingThreads;
    int idx2 = i / maxReadingThreads;
    MultiDiskLZ4Writer *writer = writers[idx1];
    threads[i] = std::thread(populateInput, writer, idx2, max);
    }
    for(int i = 0; i < parallelProcesses; ++i) {
    threads[i].join();
    }

    for(int i = 0; i < maxReadingThreads; ++i) {
    delete writers[i];
    }
    delete[] writers;
    delete[] threads;*/


    int64_t mem = Utils::getSystemMemory() * 0.7 / nindices;
    int64_t nelements = mem / sizeof(L_Triple);
    LOG(DEBUGL) << "Triples I can store in main memory: " << nelements <<
        " size triple " << sizeof(L_Triple);
    std::vector<std::pair<string, char>> additionalPermutations; //This parameter is unused here. I use it somewhere else
    Loader::sortPermTest(inputDir, maxReadingThreads, parallelProcesses,
            true, estimatedSize, nelements, 16, false, additionalPermutations);
    LOG(DEBUGL) << "...completed.";

    std::chrono::duration<double> sec = std::chrono::system_clock::now() - start;
    LOG(INFOL) << "Time (sec) " << sec.count();


}
