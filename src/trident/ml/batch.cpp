#include <trident/ml/batch.h>
#include <trident/kb/kb.h>
#include <trident/kb/querier.h>

#include <fstream>
#include <algorithm>
#include <random>

BatchCreator::BatchCreator(string kbdir, uint64_t batchsize,
        uint16_t nthreads,
        const float valid,
        const float test,
        const bool filter,
        const bool createBatchFile,
        std::shared_ptr<Feedback> feedback) : kbdir(kbdir), batchsize(batchsize),
    nthreads(nthreads), valid(valid), test(test),
    createBatchFile(createBatchFile), filter(filter),
    feedback(feedback) {
        rawtriples = NULL;
        ntriples = 0;
        currentidx = 0;
    }

std::shared_ptr<Feedback> BatchCreator::getFeedback() {
    return feedback;
}

string BatchCreator::getValidPath() {
    return kbdir + "/_batch_valid";
}

string BatchCreator::getTestPath() {
    return kbdir + "/_batch_test";
}

string BatchCreator::getValidPath(string kbdir) {
    return kbdir + "/_batch_valid";
}

string BatchCreator::getTestPath(string kbdir) {
    return kbdir + "/_batch_test";
}

void BatchCreator::createInputForBatch(bool createTraining,
        const float valid, const float test) {
    KBConfig config;
    KB kb(kbdir.c_str(), true, false, false, config);
    Querier *q = kb.query();
    auto itr = q->get(IDX_SPO, -1, -1, -1);
    long s,p,o;

    ofstream ofs_valid;
    if (valid > 0) {
        ofs_valid.open(this->kbdir + "/_batch_valid", ios::out | ios::app | ios::binary);
    }
    ofstream ofs_test;
    if (test > 0) {
        ofs_test.open(this->kbdir + "/_batch_test", ios::out | ios::app | ios::binary);
    }
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_real_distribution<float> dis(0.0, 1.0);

    //Create a file called '_batch' in the maindir with a fixed-length record size
    ofstream ofs;
    if (createTraining) {
        LOG(INFOL) << "Store the input for the batch process in " << kbdir + "/_batch ...";
        ofs.open(this->kbdir + "/_batch", ios::out | ios::app | ios::binary);
    }
    while (itr->hasNext()) {
        itr->next();
        s = itr->getKey();
        p = itr->getValue1();
        o = itr->getValue2();
        const char *cs = (const char*)&s;
        const char *cp = (const char*)&p;
        const char *co = (const char*)&o;
        if (valid > 0 && dis(gen) < valid) {
            ofs_valid.write(cs, 5); //Max numbers have 5 bytes
            ofs_valid.write(cp, 5);
            ofs_valid.write(co, 5);
        } else if (test > 0 && dis(gen) < test) {
            ofs_test.write(cs, 5); //Max numbers have 5 bytes
            ofs_test.write(cp, 5);
            ofs_test.write(co, 5);
        } else if (createTraining) {
            ofs.write(cs, 5); //Max numbers have 5 bytes
            ofs.write(cp, 5);
            ofs.write(co, 5);
        }
    }
    q->releaseItr(itr);

    if (createTraining) {
        ofs.close();
    }
    if (valid > 0) {
        ofs_valid.close();
    }
    if (test > 0) {
        ofs_test.close();
    }

    delete q;
    LOG(INFOL) << "Done";
}

struct _pso {
    const char *rawtriples;
    _pso(const char *rawtriples) : rawtriples(rawtriples) {}
    bool operator() (const uint64_t idx1, const uint64_t idx2) const {
        long s1 = *(long*)(rawtriples + idx1 * 15);
        s1 = s1 & 0xFFFFFFFFFFl;
        long p1 = *(long*)(rawtriples + idx1 * 15 + 5);
        p1 = p1 & 0xFFFFFFFFFFl;
        long o1 = *(long*)(rawtriples + idx1 * 15 + 10);
        o1 = o1 & 0xFFFFFFFFFFl;
        long s2 = *(long*)(rawtriples + idx2 * 15);
        s2 = s2 & 0xFFFFFFFFFFl;
        long p2 = *(long*)(rawtriples + idx2 * 15 + 5);
        p2 = p2 & 0xFFFFFFFFFFl;
        long o2 = *(long*)(rawtriples + idx2 * 15 + 10);
        o2 = o2 & 0xFFFFFFFFFFl;
        if (p1 != p2) {
            return p1 < p2;
        } else if (s1 != s2) {
            return s1 < s2;
        } else {
            return o1 < o2;
        }
    }
};


void BatchCreator::start() {
    //First check if the file exists
    string fin = this->kbdir + "/_batch";
    if (Utils::exists(fin)) {
    } else {
        LOG(INFOL) << "Could not find the input file for the batch. I will create it and store it in a file called '_batch'";
        createInputForBatch(createBatchFile, valid, test);
    }

    //Load the file into a memory-mapped file
    this->mappedFile = std::unique_ptr<MemoryMappedFile>(new MemoryMappedFile(
                fin));
    this->rawtriples = this->mappedFile->getData();
    this->ntriples = this->mappedFile->getLength() / 15;

    LOG(DEBUGL) << "Creating index array ...";
    this->indices.resize(this->ntriples);
    for(long i = 0; i < this->ntriples; ++i) {
        this->indices[i] = i;
    }

    LOG(DEBUGL) << "Shuffling array ...";
    std::shuffle(this->indices.begin(), this->indices.end(), engine);
    this->currentidx = 0;
    LOG(DEBUGL) << "Done";
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
        if (filter && shouldBeUsed(s,p,o)) {
            output[i*3] = s;
            output[i*3+1] = p;
            output[i*3+2] = o;
            i+=1;
        }
        currentidx++;
    }
    if (i < batchsize) {
        output.resize(i*3);
    }
    return i > 0;
}

bool BatchCreator::shouldBeUsed(long s, long p, long o) {
    return feedback->shouldBeIncluded(s, p, o);
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
        if (filter && shouldBeUsed(s,p,o)) {
            output1[i] = s;
            output2[i] = p;
            output3[i] = o;
            i+=1;
        }
        currentidx++;
    }
    if (i < batchsize) {
        output1.resize(i);
        output2.resize(i);
        output3.resize(i);
    }
    return i > 0;
}

void BatchCreator::loadTriples(string path, std::vector<uint64_t> &output) {
    MemoryMappedFile mf(path);
    char *start = mf.getData();
    char *end = start + mf.getLength();
    while (start < end) {
        long s = *(long*)(start);
        s = s & 0xFFFFFFFFFFl;
        long p = *(long*)(start + 5);
        p = p & 0xFFFFFFFFFFl;
        long o = *(long*)(start + 10);
        o = o & 0xFFFFFFFFFFl;
        output.push_back(s);
        output.push_back(p);
        output.push_back(o);
        start += 15;
    }
}
