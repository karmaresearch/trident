#include <sstream>
#include <fstream>
#include <iostream>
#include <vector>
#include <cstdlib>
#include <random>
#include <chrono>

#include <trident/utils/parallel.h>

using namespace std;

struct __sorter {
    bool operator()(long a, long b) const {
        return a < b;
    }
};

int main(int argc, const char** argv) {
    long size = 500000000l;

    //Create a large vector of random elements
    std::cout << "Loading the vector ..." << std::endl;
    std::vector<long> vector;
    vector.resize(size);
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<long> dis(0, 1000000000l);
    for(uint64_t i = 0; i < size; ++i) {
        if (i % 100000000 == 0) {
            std::cout << "Generated " << i << std::endl;
        }
        vector[i] = dis(gen);
    }

    //Sort them
    std::cout << "Start sorting ..." << std::endl;
    std::chrono::system_clock::time_point start = std::chrono::system_clock::now();
    ParallelTasks::sort_int(vector.begin(), vector.end(), __sorter(), 8);
    std::chrono::duration<double> duration = std::chrono::system_clock::now() - start;
    std::cout << "Runtime: " << duration.count() * 1000 << "ms." << std::endl;

    long prev = -1;
    for(uint64_t i = 0; i < size; ++i) {
        if (vector[i] < prev) {
            std::cerr << "Error";
        }
        prev = vector[i];
    }

    return 0;
}
