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

struct L_CompressedTriple {
    char buffer[15];

    static L_CompressedTriple maxEl;

    void setS(const uint64_t s) {
        const char *ss = (const char *)(&s);
        buffer[0] = ss[0];
        buffer[1] = ss[1];
        buffer[2] = ss[2];
        buffer[3] = ss[3];
        buffer[4] = ss[4];
    }

    void setP(const uint64_t p) {
        const char *ss = (const char *)(&p);
        buffer[5] = ss[0];
        buffer[6] = ss[1];
        buffer[7] = ss[2];
        buffer[8] = ss[3];
        buffer[9] = ss[4];
    }

    void setO(const uint64_t o) {
        const char *ss = (const char *)(&o);
        buffer[10] = ss[0];
        buffer[11] = ss[1];
        buffer[12] = ss[2];
        buffer[13] = ss[3];
        buffer[14] = ss[4];
    }

    static L_CompressedTriple max() {
        L_CompressedTriple t;
        memset(t.buffer, 0xFF, 15);
        return t;
    };

    static L_CompressedTriple getMaxEl() {
        return maxEl;
    }

    uint64_t getS() const {
        return *((uint64_t*) buffer) & 0xFFFFFFFFFFl;
    }

    uint64_t getP() const {
        return *((uint64_t*) (buffer + 5)) & 0xFFFFFFFFFFl;
    }

    uint64_t getO() const {
        return *((uint64_t*) (buffer + 10)) & 0xFFFFFFFFFFl;
    }

    uint64_t getCount() const {
        return 1;
    }

    void writeTo(LZ4Writer &writer) {
        writer.writeRawArray(buffer, 15);
    }

    void writeTo(ofstream &writer) {
        writer.write(buffer, 15);
    }

    void readFrom(LZ4Reader *reader) {
        reader->parseRawArray(buffer, 15);
    }

    void readFrom(ifstream *reader) {
        reader->read(buffer, 15);
    }

    bool greater(const L_CompressedTriple &t) const {
        if (getS() > t.getS()) {
            return true;
        } else if (getS() == t.getS()) {
            if (getP() > t.getP()) {
                return true;
            } else if (getP() == t.getP()) {
                return getO() > t.getO();
            }
        }

        return false;
    }
};

L_CompressedTriple L_CompressedTriple::maxEl = L_CompressedTriple::max();

void mergesegments(std::string inputDir) {
    LOG(INFOL) << "Start merging " << inputDir;
    auto inputmerge = Utils::getFiles(inputDir, true);
    //NoLZ4FileMerger<L_CompressedTriple> merger(inputmerge, true, false);
    FastFileMerger<8, L_CompressedTriple> merger(inputmerge, true, false);
    long c = 0;
    long counter = 0;
    while (!merger.isEmpty()) {
        L_CompressedTriple t = merger.get();
        c += t.getS() + t.getP() + t.getO();
        counter += 1;
        if (counter % 1000000 == 0)
            LOG(INFOL) << "Processed " << counter;
    }
    LOG(INFOL) << "Stop merging " << inputDir << " counter=" << counter;
}

void createTriples(std::string inputDir, int nfiles, long triplesPerFile) {
    for(int i = 0; i < 6; ++i) {
        std::string permDir = inputDir + "/p" + std::to_string(i);
        Utils::create_directories(permDir);
        long startidx = 10000000000l;
        for(int j = 0; j < nfiles; ++j) {
            long startcounter = startidx + j;
            std::string filepath = permDir + "/input-" + std::to_string(j) + ".0";
            LZ4Writer writer(filepath);
            //ofstream writer(filepath, std::ifstream::binary);
            for(long m = 0; m < triplesPerFile; ++m) {
                L_CompressedTriple t;
                t.setS(startcounter);
                t.setP(startcounter);
                t.setO(startcounter);
                t.writeTo(writer);
                startcounter += nfiles;
            }
            //writer.close();
        }
    }
}

int main(int argc, const char** argv) {
    int maxReadingThreads = 2;
    int parallelProcesses = 8;
    int nindices = 6;
    int partsPerFiles = parallelProcesses / maxReadingThreads;
    uint64_t ntriples = 3500000000l;
    uint64_t estimatedSize = ntriples * 32;
    uint64_t max = ntriples / parallelProcesses;

    int n = 1;
    if(*(char *)&n != 1) {
        LOG(ERRORL) << "Some features of Trident rely on little endianness. "
            "Change machine ...sorry";
    }

    std::string inputDir = "/Users/jacopo/Desktop/test3";
    //Create random triples
    //createTriples(inputDir, 8, 43000000);

    std::chrono::system_clock::time_point start = std::chrono::system_clock::now();
    std::thread threads[6];
    for(int i = 0; i < nindices; ++i) {
        std::string permdir = inputDir + "/p" + std::to_string(i);
        threads[i] = std::thread(std::bind(&mergesegments, permdir));
    }
    for (int i = 0; i < nindices; ++i) {
        threads[i].join();
    }
    std::chrono::duration<double> sec = std::chrono::system_clock::now() - start;
    LOG(INFOL) << "Time (sec) " << sec.count();
}
