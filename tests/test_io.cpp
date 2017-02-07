#include <cstdlib>
#include <string>
#include <iostream>

#include <kognac/compressor.h>

#include <boost/thread.hpp>
#include <boost/log/trivial.hpp>

using namespace std;

namespace timens = boost::chrono;

void readChunk(std::vector<FileInfo> *info) {
    timens::system_clock::time_point start = timens::system_clock::now();
    ifstream ifs;

    size_t sizebuffer = 20000000;
    char *buffer = new char[sizebuffer];

    for (int i = 0; i < info->size(); ++i) {
        const size_t size = info->at(i).size;
	//BOOST_LOG_TRIVIAL(debug) << "Processing chunk " << i << " of size " << size;
        if (size > sizebuffer) {
            delete[] buffer;
            buffer = new char[size + size / 2];
            sizebuffer = size + size / 2;
        }
        ifs.open(info->at(i).path);
        ifs.read(buffer, size);
        ifs.close();
    }
    delete[] buffer;
    boost::chrono::duration<double> sec = boost::chrono::system_clock::now()
                                          - start;
    BOOST_LOG_TRIVIAL(debug) << "Reading time chunk = " << sec.count() * 1000 << "ms.";
}

int main(int argc, const char** argv) {

    string input(argv[1]);
    int nthreads = atoi(argv[2]);

    //Read all the files
    vector<FileInfo> *chunks = Compressor::splitInputInChunks(input, nthreads);

    timens::system_clock::time_point start = timens::system_clock::now();
    boost::thread *threads = new boost::thread[nthreads];
    for (int i = 0; i < nthreads - 1; ++i) {
        threads[i] = boost::thread(boost::bind(readChunk, chunks + i + 1));
    }
    readChunk(chunks);
    for (int i = 0; i < nthreads - 1; ++i) {
        threads[i].join();
    }
    boost::chrono::duration<double> sec = boost::chrono::system_clock::now()
                                          - start;
    cout << "Total reading time = " << sec.count() * 1000 << "ms." << endl;

}
