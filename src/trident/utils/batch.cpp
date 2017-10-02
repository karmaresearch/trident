#include <trident/utils/batch.h>
#include <trident/kb/kb.h>
#include <trident/kb/querier.h>

#include <boost/filesystem.hpp>

#include <fstream>
#include <algorithm>
#include <random>

namespace fs = boost::filesystem;

BatchCreator::BatchCreator(string kbdir, uint64_t batchsize,
        uint16_t nthreads) : kbdir(kbdir), batchsize(batchsize), nthreads(nthreads) {
    rawtriples = NULL;
    ntriples = 0;
    currentidx = 0;
}

void BatchCreator::createInputForBatch() {
    //Create a file called '_batch' in the maindir with a fixed-length record size
    LOG(INFO) << "Store the input for the batch process in " << kbdir + "/_batch ...";
    KBConfig config;
    KB kb(kbdir.c_str(), true, false, false, config);
    Querier *q = kb.query();
    auto itr = q->get(IDX_SPO, -1, -1, -1);
    long s,p,o;
    ofstream ofs;
    ofs.open(this->kbdir + "/_batch", ios::out | ios::app | ios::binary);
    while (itr->hasNext()) {
        itr->next();
        s = itr->getKey();
        p = itr->getValue1();
        o = itr->getValue2();
        const char *cs = (const char*)&s;
        const char *cp = (const char*)&p;
        const char *co = (const char*)&o;
        ofs.write(cs, 5); //Max numbers have 5 bytes
        ofs.write(cp, 5);
        ofs.write(co, 5);
    }
    q->releaseItr(itr);
    ofs.close();
    delete q;
    LOG(INFO) << "Done";
}

void BatchCreator::start() {
    //First check if the file exists
    string fin = this->kbdir + "/_batch";
    if (!fs::exists(fin)) {
        LOG(INFO) << "Could not find the input file for the batch. I will create it and store it in a file called '_batch'";
        createInputForBatch();
    }

    //Load the file into a memory-mapped file
    this->mapping = bip::file_mapping(fin.c_str(), bip::read_only);
    this->mapped_rgn = bip::mapped_region(this->mapping, bip::read_only);
    this->rawtriples = static_cast<char*>(this->mapped_rgn.get_address());
    this->ntriples = (uint64_t)this->mapped_rgn.get_size() / 15; //triples

    LOG(DEBUG) << "Creating index array ...";
    this->indices.resize(this->ntriples);
    for(long i = 0; i < this->ntriples; ++i) {
        this->indices[i] = i;
    }

    LOG(DEBUG) << "Shuffling array ...";
    auto engine = std::default_random_engine{};
    std::shuffle(this->indices.begin(), this->indices.end(), engine);
    this->currentidx = 0;
    LOG(DEBUG) << "Done";
}

bool BatchCreator::getBatch(std::vector<uint64_t> &output) {
    long i = 0;
    output.resize(this->batchsize * 3);
    //The output vector is already supposed to contain batchsize elements. Otherwise, resize it
    while (i < batchsize && currentidx < ntriples) {
        long idx = indices[currentidx];
        long s = *(long*)(rawtriples + idx * 15);
        s = s & 0xFFFFFFFFFFl;
        long p = *(long*)(rawtriples + idx * 15 + 5);
        p = p & 0xFFFFFFFFFFl;
        long o = *(long*)(rawtriples + idx * 15 + 10);
        o = o & 0xFFFFFFFFFFl;
        output[i*3] = s;
        output[i*3+1] = p;
        output[i*3+2] = o;
        i+=1;
        currentidx++;
    }
    if (i < batchsize) {
        output.resize(i*3);
    }
    return i > 0;
}

bool BatchCreator::getBatch(std::vector<uint64_t> &output1,
        std::vector<uint64_t> &output2,
        std::vector<uint64_t> &output3) {
    long i = 0;
    output1.resize(this->batchsize);
    output2.resize(this->batchsize);
    output3.resize(this->batchsize);
    //The output vector is already supposed to contain batchsize elements. Otherwise, resize it
    while (i < batchsize && currentidx < ntriples) {
        long idx = indices[currentidx];
        long s = *(long*)(rawtriples + idx * 15);
        s = s & 0xFFFFFFFFFFl;
        long p = *(long*)(rawtriples + idx * 15 + 5);
        p = p & 0xFFFFFFFFFFl;
        long o = *(long*)(rawtriples + idx * 15 + 10);
        o = o & 0xFFFFFFFFFFl;
        output1[i] = s;
        output2[i] = p;
        output3[i] = o;
        i+=1;
        currentidx++;
    }
    if (i < batchsize) {
        output1.resize(i);
        output2.resize(i);
        output3.resize(i);
    }
    return i > 0;
}
