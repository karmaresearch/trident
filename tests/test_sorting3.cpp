#include <sstream>
#include <fstream>
#include <iostream>
#include <vector>
#include <cstdlib>
#include <random>
#include <chrono>
#include <array>

#include <trident/utils/parallel.h>

using namespace std;

/*struct __sorter {
  bool operator()(long a, long b) const {
  return a < b;
  }
  };*/

typedef std::array<unsigned char, 15> __PermSorter_triple;

bool __PermSorter_triple_sorter(const __PermSorter_triple &a,
        const __PermSorter_triple &b) {
    return a < b;
}

void sortPermutation(char *start, char *end, int nthreads) {
    __PermSorter_triple *sstart = (__PermSorter_triple*) start;
    __PermSorter_triple *send = (__PermSorter_triple*) end;
    std::chrono::system_clock::time_point starttime = std::chrono::system_clock::now();
    ParallelTasks::sort_int(sstart, send, &__PermSorter_triple_sorter, nthreads);
    std::chrono::duration<double> duration = std::chrono::system_clock::now() - starttime;
}

int main(int argc, const char** argv) {
    int nthreads = 2;
    int parallelProcesses = 16;
    long chunk = 1*1000*1000l;
    long size = chunk * parallelProcesses;

    //Create a large vector of random elements
    std::cout << "Loading the vector ..." << std::endl;
    std::vector<std::vector<char>> vectors;
    vectors.resize(6);
    for(int i = 0; i < 6; ++i) {
        vectors[i].resize(size * 15);
    }
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<long> dis(0, 1000000000l);
    for(uint64_t i = 0; i < size; ++i) {
        if (i % 100000000 == 0) {
            std::cout << "Generated " << i << std::endl;
        }
        long number = dis(gen);
        for(int i = 0; i < vectors.size(); ++i) {
            *(long*)(vectors[i].data() + i * 15) = number;
        }
    }

    //Sort them
    std::vector<std::thread> threads;
    threads.resize(5);
    for(int i = 0; i < 5; ++i) {
            threads[i] = std::thread(
                    std::bind(&sortPermutation,
                        vectors[i+1].data(), vectors[i+1].data() + 15 * size,
                        nthreads));
    }
    sortPermutation(vectors[0].data(),
            vectors[0].data() + 15 * size,
            nthreads);
    for(int i = 0; i < 5; ++i) {
        threads[i].join();
    }

    /*long prev = -1;
      for(uint64_t i = 0; i < size; ++i) {
      if (vector[i] < prev) {
      std::cerr << "Error";
      }
      prev = vector[i];
      }*/

    return 0;
}
