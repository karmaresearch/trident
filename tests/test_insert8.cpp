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

    std::string inputDir = "/Users/jacopo/Desktop/test3/";
    std::vector<std::string> permDirs(6);
    permDirs[0] = inputDir + "permtmp-0";
    permDirs[1] = inputDir + "permtmp-1";
    permDirs[2] = inputDir + "permtmp-2";
    permDirs[3] = inputDir + "permtmp-3";
    permDirs[4] = inputDir + "permtmp-4";
    permDirs[5] = inputDir + "permtmp-5";
    std::vector<std::pair<string, char>> permutations;
    permutations.push_back(std::make_pair(permDirs[0], IDX_SPO));
    permutations.push_back(std::make_pair(permDirs[3], IDX_SOP));
    permutations.push_back(std::make_pair(permDirs[4], IDX_OSP));
    permutations.push_back(std::make_pair(permDirs[1], IDX_OPS));
    permutations.push_back(std::make_pair(permDirs[2], IDX_POS));
    permutations.push_back(std::make_pair(permDirs[5], IDX_PSO));

    std::chrono::system_clock::time_point start = std::chrono::system_clock::now();
    PermSorter::sortChunks2(permutations, maxReadingThreads,
            parallelProcesses,
            estimatedSize);

    Loader::mergeDiskFragments(ParamsMergeDiskFragments(inputDir));
    std::chrono::duration<double> sec = std::chrono::system_clock::now() - start;
    LOG(INFOL) << "Time (sec) " << sec.count();
}
