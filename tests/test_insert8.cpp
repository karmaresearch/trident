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
#include <trident/kb/permsorter.h>

using namespace std;

int main(int argc, const char** argv) {
    int maxReadingThreads = 2;
    int parallelProcesses = 8;
    int nindices = 6;
    int partsPerFiles = parallelProcesses / maxReadingThreads;
    uint64_t ntriples = 3500000000l;
    uint64_t estimatedSize = ntriples * 15;
    uint64_t max = ntriples / parallelProcesses;

    int n = 1;
    if(*(char *)&n != 1) {
        LOG(ERRORL) << "Some features of Trident rely on little endianness. "
            "Change machine ...sorry";
    }

    std::string inputDir = "/Users/jacopo/Desktop/test3";

    std::chrono::system_clock::time_point start = std::chrono::system_clock::now();
    PermSorter::sortChunks2(inputDir, maxReadingThreads, parallelProcesses,
            estimatedSize, true);
    Loader::mergeDiskFragments(ParamsMergeDiskFragments(inputDir));
    std::chrono::duration<double> sec = std::chrono::system_clock::now() - start;
    LOG(INFOL) << "Time (sec) " << sec.count();
}
