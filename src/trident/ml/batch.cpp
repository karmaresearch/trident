#include <trident/ml/batch.h>
#include <trident/kb/kb.h>
#include <trident/kb/querier.h>

#include <fstream>
#include <algorithm>
#include <random>
#include <unordered_map>

BatchCreator::BatchCreator(string kbdir, uint64_t batchsize,
        uint16_t nthreads,
        const float valid,
        const float test,
        const bool filter,
        const bool createBatchFile,
        std::shared_ptr<Feedback> feedback) : kbdir(kbdir), batchsize(batchsize),
    /*nthreads(nthreads),*/ valid(valid), test(test),
    createBatchFile(createBatchFile), filter(filter),
    feedback(feedback) {
        KBConfig config;
        this->kb = new KB(kbdir.c_str(), true, false, false, config);
        rawtriples = NULL;
        ntriples = 0;
        currentidx = 0;
        usedIndex = IDX_POS;
    }

std::shared_ptr<Feedback> BatchCreator::getFeedback() {
    return feedback;
}

string BatchCreator::getValidPath() {
    return kbdir + "/_batch_valid" + std::to_string(this->usedIndex);
}

string BatchCreator::getTestPath() {
    return kbdir + "/_batch_test" + std::to_string(this->usedIndex);
}

string BatchCreator::getValidPath(string kbdir, int usedIndex) {
    return kbdir + "/_batch_valid" + std::to_string(usedIndex);
}

string BatchCreator::getTestPath(string kbdir, int usedIndex) {
    return kbdir + "/_batch_test" + std::to_string(usedIndex);
}

void BatchCreator::createInputForBatch(bool createTraining,
        const float valid, const float test) {
    Querier *q = kb->query();
    auto itr = q->getIterator(this->usedIndex, -1, -1, -1);
    int64_t s, p, o;

    ofstream ofs_valid;
    if (valid > 0) {
        ofs_valid.open(this->kbdir + "/_batch_valid" + std::to_string(this->usedIndex), ios::out | ios::app | ios::binary);
    }
    ofstream ofs_test;
    if (test > 0) {
        ofs_test.open(this->kbdir + "/_batch_test" + std::to_string(this->usedIndex), ios::out | ios::app | ios::binary);
    }
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_real_distribution<float> dis(0.0, 1.0);

    //Create a file called '_batch' in the maindir with a fixed-length record size
    ofstream ofs;
    if (createTraining) {
        LOG(INFOL) << "Store the input for the batch process in " <<
            kbdir + "/_batch" + std::to_string(this->usedIndex) << " ...";
        ofs.open(this->kbdir + "/_batch" + std::to_string(this->usedIndex),
                ios::out | ios::app | ios::binary);
    }

    std::unordered_map<uint64_t, uint64_t> mapPredicates;
    if (!kb->areRelIDsSeparated()) {
        size_t i = 0;
        auto querier = kb->query();
        auto itr = querier->getTermList(IDX_POS);
        while (itr->hasNext()) {
            itr->next();
            auto pname = itr->getKey();
            mapPredicates.insert(std::make_pair(pname, i));
            i++;
        }
        querier->releaseItr(itr);
        delete querier;
    }

    while (itr->hasNext()) {
        itr->next();
        if (this->usedIndex == IDX_SPO) {
            s = itr->getKey();
            p = itr->getValue1();
            o = itr->getValue2();
        } else if (this->usedIndex == IDX_SOP) {
            s = itr->getKey();
            o = itr->getValue1();
            p = itr->getValue2();
        } else if (this->usedIndex == IDX_POS) {
            p = itr->getKey();
            o = itr->getValue1();
            s = itr->getValue2();
        } else if (this->usedIndex == IDX_PSO) {
            p = itr->getKey();
            s = itr->getValue1();
            o = itr->getValue2();
        } else if (this->usedIndex == IDX_OSP) {
            o = itr->getKey();
            s = itr->getValue1();
            p = itr->getValue2();
        } else if (this->usedIndex == IDX_OPS) {
            o = itr->getKey();
            p = itr->getValue1();
            s = itr->getValue2();
        }

        if (!kb->areRelIDsSeparated()) {
            p = mapPredicates[p];
        }

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
        int64_t s1 = *(int64_t*)(rawtriples + idx1 * 15);
        s1 = s1 & 0xFFFFFFFFFFl;
        int64_t p1 = *(int64_t*)(rawtriples + idx1 * 15 + 5);
        p1 = p1 & 0xFFFFFFFFFFl;
        int64_t o1 = *(int64_t*)(rawtriples + idx1 * 15 + 10);
        o1 = o1 & 0xFFFFFFFFFFl;
        int64_t s2 = *(int64_t*)(rawtriples + idx2 * 15);
        s2 = s2 & 0xFFFFFFFFFFl;
        int64_t p2 = *(int64_t*)(rawtriples + idx2 * 15 + 5);
        p2 = p2 & 0xFFFFFFFFFFl;
        int64_t o2 = *(int64_t*)(rawtriples + idx2 * 15 + 10);
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

int64_t BatchCreator::findFirstOccurrence(int64_t start, int64_t end, uint64_t x, int offset) {
    int64_t idxTerm = -1;
    while (start <= end) {
        int64_t middle = start + (end - start) / 2;
        int64_t term = *(int64_t*)(rawtriples + middle * 15 + offset);
        term = term & 0xFFFFFFFFFFl;
        if (term < x) {
            start = middle + 1;
        } else if (term > x) {
            end = middle - 1;
        } else {
            idxTerm = middle;
            end = middle - 1;
        }
    }
    return idxTerm;
}

int64_t BatchCreator::findLastOccurrence(int64_t start, int64_t end, uint64_t x, int offset) {
    int64_t idxTerm = -1;
    while (start <= end) {
        int64_t middle = start + (end - start) / 2;
        int64_t term = *(int64_t*)(rawtriples + middle * 15 + offset);
        term = term & 0xFFFFFFFFFFl;
        if (term < x) {
            start = middle + 1;
        } else if (term > x) {
            end = middle - 1;
        } else {
            idxTerm = middle;
            start = middle + 1;
        }
    }
    return idxTerm;
}


void BatchCreator::populateIndicesFromQuery(int64_t s, int64_t p, int64_t o) {
    int64_t start = 0;
    int64_t end = ntriples;

    int64_t firstTerm = ~0lu;
    int offset = 0;
    if (this->usedIndex == IDX_POS || this->usedIndex == IDX_PSO) {
        firstTerm = p;
        offset = 5;
    } else if (this->usedIndex == IDX_SOP || this->usedIndex == IDX_SPO) {
        firstTerm = s;
        offset = 0;
    } else {
        firstTerm = o;
        offset = 10;
    }

    int64_t idxStartFirstTerm = -1;
    if (firstTerm != -1) {
        idxStartFirstTerm = findFirstOccurrence(start, end, firstTerm, offset);
        if (idxStartFirstTerm != -1) {
            int64_t idxEndFirstTerm = findLastOccurrence(idxStartFirstTerm,
                    end, firstTerm, offset);
            start = idxStartFirstTerm;
            if (idxEndFirstTerm < end)
                end = idxEndFirstTerm + 1;
        } else {
            start = end = 0;
        }
    }

    int64_t idxStartSecondTerm = -1;
    if (idxStartFirstTerm != -1) {
        int64_t secondTerm = ~0lu;
        if (this->usedIndex == IDX_OPS || this->usedIndex == IDX_SPO) {
            secondTerm = p;
            offset = 5;
        } else if (this->usedIndex == IDX_OSP || this->usedIndex == IDX_PSO) {
            secondTerm = s;
            offset = 0;
        } else {
            secondTerm = o;
            offset = 10;
        }
        if (secondTerm != -1) {
            idxStartSecondTerm = findFirstOccurrence(start, end, secondTerm,
                    offset);
            if (idxStartSecondTerm != -1) {
                int64_t idxEndSecondTerm = findLastOccurrence(idxStartSecondTerm,
                        end, secondTerm, offset);
                start = idxStartSecondTerm;
                if (idxEndSecondTerm < end)
                    end = idxEndSecondTerm + 1;
            } else {
                start = end = 0;
            }
        }
    }

    if (idxStartSecondTerm != -1) {
        int64_t thirdTerm;
        if (this->usedIndex == IDX_OPS || this->usedIndex == IDX_POS) {
            thirdTerm = s;
            offset = 0;
        } else if (this->usedIndex == IDX_OSP || this->usedIndex == IDX_SOP) {
            thirdTerm = p;
            offset = 5;
        } else {
            thirdTerm = o;
            offset = 10;
        }
        if (thirdTerm != -1) {
            int64_t idxStartThirdTerm = findFirstOccurrence(start, end, thirdTerm,
                    offset);
            if (idxStartThirdTerm != -1) {
                start = idxStartThirdTerm;
                end = start + 1;
            } else {
                start = end = 0;
            }
        }
    }

    //std::cout << "Creating an index with " << (end - start) << std::endl;

    //Populate indices
    this->indices.resize(end - start);
    uint64_t i = 0;
    while (start < end) {
        indices[i++] = start++;
    }
}

uint64_t BatchCreator::getNBatches() {
    int64_t n = indices.size() / this->batchsize;
    if ((indices.size() % this->batchsize) != 0) {
        return n + 1;
    } else {
        return n;
    }
}

void BatchCreator::start(int64_t s, int64_t p, int64_t o) {
    //Get index for s,p,o
    this->usedIndex = IDX_POS;
    if (s != -1 || p != -1 || o != -1) {
        if (valid != 0 || test != 0) {
            LOG(ERRORL) << "Query-based batching works only if the entire KB is used for training";
            throw 10;
        }
        Querier *q = kb->query();
        this->usedIndex = q->getIndex(s, p, o);
        delete q;
    }

    //First check if the file exists
    string fin = this->kbdir + "/_batch" + std::to_string(this->usedIndex);
    if (Utils::exists(fin)) {
    } else {
        if (createBatchFile) {
            LOG(INFOL) << "Could not find the input file for the batch."
                " I will create it and store it in a file called '_batch'";
            createInputForBatch(createBatchFile, valid, test);
        }
    }

    if (createBatchFile) {
        //Load the file into a memory-mapped file
        this->mappedFile = std::unique_ptr<MemoryMappedFile>(new MemoryMappedFile(
                    fin));
        this->rawtriples = this->mappedFile->getData();
        this->ntriples = this->mappedFile->getLength() / 15;
    } else {
        this->kbbatch = std::unique_ptr<KBBatch>(new KBBatch(kb));
        this->kbbatch->populateCoordinates();
        this->ntriples = this->kbbatch->ntriples();
    }

    LOG(DEBUGL) << "Creating index array ...";
    if (s != -1 || p != -1 || o != -1) {
        assert(createBatchFile);
        //Put in indices only the triples that satisfy the query
        populateIndicesFromQuery(s, p, o);
    } else {
        this->indices.resize(this->ntriples);
        for(int64_t i = 0; i < this->ntriples; ++i) {
            this->indices[i] = i;
        }
    }

    LOG(DEBUGL) << "Shuffling array ...";
    shuffle();
    LOG(DEBUGL) << "Done";
}

void BatchCreator::shuffle() {
    std::shuffle(this->indices.begin(), this->indices.end(), engine);
    this->currentidx = 0;
}

bool BatchCreator::getBatch(std::vector<uint64_t> &output) {
    int64_t i = 0;
    output.resize(this->batchsize * 3);
    //The output vector is already supposed to contain batchsize elements. Otherwise, resize it
    while (i < batchsize && currentidx < indices.size()) {
        int64_t idx = indices[currentidx];
        uint64_t s,p,o;
        if (createBatchFile) {
            s = *(uint64_t*)(rawtriples + idx * 15);
            s = s & 0xFFFFFFFFFFl;
            p = *(uint64_t*)(rawtriples + idx * 15 + 5);
            p = p & 0xFFFFFFFFFFl;
            o = *(uint64_t*)(rawtriples + idx * 15 + 10);
            o = o & 0xFFFFFFFFFFl;
        } else {
            kbbatch->getAt(idx, s, p, o);
        }
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

bool BatchCreator::shouldBeUsed(int64_t s, int64_t p, int64_t o) {
    return feedback->shouldBeIncluded(s, p, o);
}

bool BatchCreator::getBatchNr(uint64_t nr, std::vector<uint64_t> &output1,
        std::vector<uint64_t> &output2,
        std::vector<uint64_t> &output3) {
    output1.resize(this->batchsize);
    output2.resize(this->batchsize);
    output3.resize(this->batchsize);

    //Try to get up to batchsize triples
    int64_t i = 0;
    int64_t cidx = nr * this->batchsize;

    while (i < batchsize && cidx < indices.size()) {
        int64_t idx = indices[cidx];
        uint64_t s, p, o;
        if (createBatchFile) {
            s = *(uint64_t *)(rawtriples + idx * 15);
            s = s & 0xFFFFFFFFFFl;
            p = *(uint64_t *)(rawtriples + idx * 15 + 5);
            p = p & 0xFFFFFFFFFFl;
            o = *(uint64_t *)(rawtriples + idx * 15 + 10);
            o = o & 0xFFFFFFFFFFl;
        } else {
            kbbatch->getAt(idx, s, p, o);
        }
        if (!filter || shouldBeUsed(s,p,o)) {
            output1[i] = s;
            output2[i] = p;
            output3[i] = o;
            i+=1;
        }
        cidx++;
    }
    if (i < this->batchsize) {
        output1.resize(i);
        output2.resize(i);
        output3.resize(i);
    }
    return i > 0;
}

bool BatchCreator::getBatch(std::vector<uint64_t> &output1,
        std::vector<uint64_t> &output2,
        std::vector<uint64_t> &output3) {
    int64_t i = 0;
    output1.resize(this->batchsize);
    output2.resize(this->batchsize);
    output3.resize(this->batchsize);
    //The output vector is already supposed to contain batchsize elements.
    //Otherwise, resize it
    while (i < batchsize && currentidx < indices.size()) {
        int64_t idx = indices[currentidx];
        uint64_t s,p,o;
        if (createBatchFile) {
            s = *(uint64_t *)(rawtriples + idx * 15);
            s = s & 0xFFFFFFFFFFl;
            p = *(uint64_t *)(rawtriples + idx * 15 + 5);
            p = p & 0xFFFFFFFFFFl;
            o = *(uint64_t *)(rawtriples + idx * 15 + 10);
            o = o & 0xFFFFFFFFFFl;
        } else {
            kbbatch->getAt(idx, s, p, o);
        }
        if (!filter || shouldBeUsed(s,p,o)) {
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
        int64_t s = *(int64_t*)(start);
        s = s & 0xFFFFFFFFFFl;
        int64_t p = *(int64_t*)(start + 5);
        p = p & 0xFFFFFFFFFFl;
        int64_t o = *(int64_t*)(start + 10);
        o = o & 0xFFFFFFFFFFl;
        output.push_back(s);
        output.push_back(p);
        output.push_back(o);
        start += 15;
    }
}

BatchCreator::KBBatch::KBBatch(KB *kb) : kb(kb) {
    this->querier = std::unique_ptr<Querier>(kb->query());
}

void BatchCreator::KBBatch::populateCoordinates() {
    //Load all files
    allposfiles = kb->openAllFiles(IDX_POS);
    auto itr = (TermItr*)querier->getTermList(IDX_POS);
    string kbdir = kb->getPath();
    string posdir = kbdir + "/p" + to_string(IDX_POS);
    //Get all the predicates
    uint64_t current = 0;
    while (itr->hasNext()) {
        itr->next();
        uint64_t pred = itr->getKey();
        uint64_t card = itr->getCount();
        PredCoordinates info;
        info.pred = pred;
        info.boundary = current + card;

        //Get beginning of the table
        short currentFile = itr->getCurrentFile();
        int64_t currentMark = itr->getCurrentMark();
        string fdidx = posdir + "/" + to_string(currentFile) + ".idx";
        if (Utils::exists(fdidx)) {
            ifstream idxfile(fdidx);
            idxfile.seekg(8 + 11 * currentMark);
            char buffer[5];
            idxfile.read(buffer, 5);
            int64_t pos = Utils::decode_longFixedBytes(buffer, 5);
            info.buffer = allposfiles[currentFile] + pos;
        } else {
            LOG(ERRORL) << "Cannot happen!";
            throw 10;
        }
        char currentStrat = itr->getCurrentStrat();
        int storageType = StorageStrat::getStorageType(currentStrat);
        if (storageType == NEWCOLUMN_ITR) {
            //Get reader and offset
            char header1 = info.buffer[0];
            char header2 = info.buffer[1];
            const uint8_t bytesPerStartingPoint =  header2 & 7;
            const uint8_t bytesPerCount = (header2 >> 3) & 7;
            const uint8_t remBytes = bytesPerCount + bytesPerStartingPoint;
            const uint8_t bytesPerFirstEntry = (header1 >> 3) & 7;
            const uint8_t bytesPerSecondEntry = (header1) & 7;
            info.offset = remBytes;

            //update the buffer
            int offset = 2;
            info.nfirstterms = Utils::decode_vlong2(info.buffer, &offset);
            Utils::decode_vlong2(info.buffer, &offset);
            info.buffer = info.buffer + offset;

            FactoryNewColumnTable::get12Reader(bytesPerFirstEntry,
                    bytesPerSecondEntry, &info.reader);
        } else if (storageType == NEWROW_ITR) {
            const char nbytes1 = (currentStrat >> 3) & 3;
            const char nbytes2 = (currentStrat >> 1) & 3;
            FactoryNewRowTable::get12Reader(nbytes1, nbytes2, &info.reader);
        } else {
            LOG(ERRORL) << "Not supported";
            throw 10;
        }
        predicates.push_back(info);
        current += card;
    }
    querier->releaseItr(itr);
}

void BatchCreator::KBBatch::getAt(uint64_t pos,
        uint64_t &s,
        uint64_t &p,
        uint64_t &o) {
    auto itr = predicates.begin();
    uint64_t offset = 0;
    while(itr != predicates.end() && pos >= itr->boundary) {
        offset = itr->boundary;
        itr++;
    }
    pos -= offset;
    if (itr != predicates.end()) {
        //Take the reader and read the values
        itr->reader(itr->nfirstterms, itr->offset,
                itr->buffer, pos, o, s);
        p = itr->pred;
    }
}

BatchCreator::KBBatch::~KBBatch() {
    querier = NULL;
}

uint64_t BatchCreator::KBBatch::ntriples() {
    return kb->getSize();
}
