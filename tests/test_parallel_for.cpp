#include <sstream>
#include <fstream>
#include <iostream>
#include <vector>
#include <cstdlib>
#include <random>
#include <chrono>

#include <trident/utils/parallel.h>
#include <kognac/logs.h>

using namespace std;

class Container {
    public:
        void operator()(const ParallelRange& r) const {
            LOG(ERRORL) << "begin " << r.begin() << " end " << r.end();
        }

};

int ParallelTasks::nthreads = -1;
int main(int argc, const char** argv) {
    int nthreads = 8;

    Container container;
    ParallelTasks::parallel_for(0, 100, 60, container);
    return 0;
}
