#include <iostream>
#include <cstdlib>
#include <sstream>
#include <fstream>
#include <boost/chrono.hpp>
#include <boost/filesystem.hpp>
#include <boost/log/trivial.hpp>

#include <kognac/triplewriters.h>
#include <trident/loader.h>

using namespace std;
namespace timens = boost::chrono;
namespace fs = boost::filesystem;

void generateTriples(string input, long n) {
   LZ4Writer writer(input);
   long seed = 0;
   for(long j = 0; j < n; ++j) {
            long t1 = abs(seed++);
            long t2 = abs(seed++);
            long t3 = abs(seed++);
            writer.writeLong(t1);
            writer.writeLong(t2);
            writer.writeLong(t3);
            if (j % 10000 == 0) {
                seed = random() * 10000000000l;
            }
            if (j % 100000000 == 0)
                BOOST_LOG_TRIVIAL(debug) << "Dumped " << j << " triples";
   }
}

int main(int argc, const char** argv) {

    long n = 100000000000;

    if (false) {
    //Generate random input
    cout << "Storing the input at " << argv[1] << endl;
    if (fs::exists(fs::path(argv[1]))) {
        fs::remove_all(fs::path(argv[1]));
    }
    fs::create_directories(fs::path(argv[1]));

    boost::thread threads[72];
    for(int i = 0; i < 72; ++i) {
        string out = string(argv[1]) + "/input" + to_string(i);
        threads[i] = boost::thread(generateTriples, out, n / 72);
    }
    for(int i = 0; i < 72; ++i) {
        threads[i].join();
    }

    }

    //Sort
    BOOST_LOG_TRIVIAL(debug) << "Start sorting";
    Loader::sortPermutation(string(argv[1]), 8, 72, true, 100000000000l, 30000000000l , 16);
    BOOST_LOG_TRIVIAL(debug) << "Stop sorting";

    return 0;
}
