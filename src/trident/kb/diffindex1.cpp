/*
 * Copyright 2017 Jacopo Urbani
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 **/


#include <trident/kb/diffindex.h>
#include <trident/kb/querier.h>
#include <trident/iterators/diff1itr.h>
#include <trident/iterators/emptyitr.h>

#include <kognac/lz4io.h>

#include <chrono>

DiffIndex1::DiffIndex1(string dir, DiffIndex::TypeUpdate type) : DiffIndex(type, DiffIndex::DIFF1) {
    ifstream f;
    f.open(dir + "/stats");
    char buffer[8];
    f.read(buffer, 2);
    nbytes = buffer[0];
    posValues = buffer[1];
    f.read(buffer, 8);
    size = Utils::decode_long(buffer);
    for (int i = 0; i < 3; ++i) {
        f.read(buffer, 8);
        triple[i] = Utils::decode_long(buffer);
    }
    for (int i = 0; i < 3; ++i) {
        f.read(buffer, 8);
        nkeys[i] = Utils::decode_long(buffer);
        f.read(buffer, 8);
        nuniquekeys[i] = Utils::decode_long(buffer);
    }
    for (int i = 0; i < 6; ++i) {
        f.read(buffer, 8);
        nfirstterms[i] = Utils::decode_long(buffer);
        f.read(buffer, 8);
        nuniquefirstterms[i] = Utils::decode_long(buffer);
    }
    f.close();

    //Load the values
    values = std::unique_ptr<ROMappedFile>(new ROMappedFile(dir + "/values"));
    if (posValues == 0) {
        newpairs1 = std::unique_ptr<ROMappedFile>(new ROMappedFile(dir + "/newps"));
        newpairs2 = std::unique_ptr<ROMappedFile>(new ROMappedFile(dir + "/newos"));
    } else if (posValues == 1) {
        newpairs1 = std::unique_ptr<ROMappedFile>(new ROMappedFile(dir + "/newsp"));
        newpairs2 = std::unique_ptr<ROMappedFile>(new ROMappedFile(dir + "/newop"));
    } else {
        newpairs1 = std::unique_ptr<ROMappedFile>(new ROMappedFile(dir + "/newso"));
        newpairs2 = std::unique_ptr<ROMappedFile>(new ROMappedFile(dir + "/newpo"));
    }
}

extern EmptyItr emptyItr;
PairItr *DiffIndex1::getIterator(int idx, int64_t first, int64_t second, int64_t third,
        int64_t &nfirstterms) {
    assert(factory != NULL);
    //nfirstterms = 0;
    int64_t permutedTriple[3];
    permutedTriple[0] = permutedTriple[1] = permutedTriple[2] = 0;
    switch (idx) {
        case IDX_SPO:
            permutedTriple[0] = triple[0];
            permutedTriple[1] = triple[1];
            permutedTriple[2] = triple[2];
            break;
        case IDX_SOP:
            permutedTriple[0] = triple[0];
            permutedTriple[1] = triple[2];
            permutedTriple[2] = triple[1];
            break;
        case IDX_POS:
            permutedTriple[0] = triple[1];
            permutedTriple[1] = triple[2];
            permutedTriple[2] = triple[0];
            break;
        case IDX_PSO:
            permutedTriple[0] = triple[1];
            permutedTriple[1] = triple[0];
            permutedTriple[2] = triple[2];
            break;
        case IDX_OPS:
            permutedTriple[0] = triple[2];
            permutedTriple[1] = triple[1];
            permutedTriple[2] = triple[0];
            break;
        case IDX_OSP:
            permutedTriple[0] = triple[2];
            permutedTriple[1] = triple[0];
            permutedTriple[2] = triple[1];
            break;
    }
    if (first != -1 && permutedTriple[0] != -1 &&
            first != permutedTriple[0]) {
        return &emptyItr;
    }
    if (second != -1 && permutedTriple[1] != -1 &&
            second != permutedTriple[1]) {
        return &emptyItr;
    }
    if (third != -1 && permutedTriple[2] != -1 &&
            third != permutedTriple[2]) {
        return &emptyItr;
    }

    //The array is in the position where the triple is -1
    if (permutedTriple[0] == -1) {
        if (first >= 0) {
            //We should check whether the key is in the array
            int pos = posValueInArray(first);
            if (pos == size) {
                return &emptyItr;
            } else {
                Diff1Itr *itr = factory->get();
                itr->init(permutedTriple[0], permutedTriple[1], permutedTriple[2],
                        values->getBuffer() + pos * nbytes,
                        nbytes, 1, 0);
                nfirstterms += getNFirstTerms(idx, first, pos, second);
                return itr;
            }
        } else {
            Diff1Itr *itr = factory->get();
            itr->init(permutedTriple[0], permutedTriple[1], permutedTriple[2],
                    values->getBuffer(),
                    nbytes, size, 0);
            nfirstterms += getNFirstTerms(idx, first, -1, second);
            return itr;
        }
    } else if (permutedTriple[1] == -1) {
        if (second >= 0) {
            int pos = posValueInArray(second);
            if (pos == size) {
                return &emptyItr;
            } else {
                Diff1Itr *itr = factory->get();
                itr->init(permutedTriple[0], permutedTriple[1], permutedTriple[2],
                        values->getBuffer() + pos * nbytes,
                        nbytes, 1, 1);
                nfirstterms += getNFirstTerms(idx, first, pos, second);
                return itr;
            }
        } else {
            Diff1Itr *itr = factory->get();
            itr->init(permutedTriple[0], permutedTriple[1], permutedTriple[2],
                    values->getBuffer(),
                    nbytes, size, 1);
            nfirstterms += getNFirstTerms(idx, first, -1, second);
            return itr;
        }
    } else {
        assert(permutedTriple[2] == -1);
        if (third >= 0) {
            int pos = posValueInArray(third);
            if (pos == size) {
                return &emptyItr;
            } else {
                Diff1Itr *itr = factory->get();
                itr->init(permutedTriple[0], permutedTriple[1], permutedTriple[2],
                        values->getBuffer() + pos * nbytes,
                        nbytes, 1, 2);
                nfirstterms += getNFirstTerms(idx, first, pos, second);
                return itr;
            }
        } else {
            Diff1Itr *itr = factory->get();
            itr->init(permutedTriple[0], permutedTriple[1], permutedTriple[2],
                    values->getBuffer(),
                    nbytes, size, 2);
            nfirstterms += getNFirstTerms(idx, first, -1, second);
            return itr;
        }
    }
}

int64_t DiffIndex1::getNFirstTerms(const int idx, const int64_t first,
        const int64_t posfirst, const int64_t second) {
    if (first == -1)
        return 0;
    if (second >= 0)
        return 1;

    if (posValues == 0) {
        switch (idx) {
            case IDX_SPO:
                assert(posfirst >= 0);
                //must check the PS list
                return newpairs1->getBuffer()[posfirst];
            case IDX_SOP:
                assert(posfirst >= 0);
                //must check the OS list
                return newpairs2->getBuffer()[posfirst];
            case IDX_OPS:
                return nuniquefirstterms[IDX_OPS];
            case IDX_OSP:
                return nuniquefirstterms[IDX_OSP];
            case IDX_POS:
                return nuniquefirstterms[IDX_POS];
            case IDX_PSO:
                return nuniquefirstterms[IDX_PSO];
        }
    } else if (posValues == 1) {
        switch (idx) {
            case IDX_SPO:
                return nuniquefirstterms[IDX_SPO];
            case IDX_SOP:
                return nuniquefirstterms[IDX_SOP];
            case IDX_OPS:
                return nuniquefirstterms[IDX_OPS];
            case IDX_OSP:
                return nuniquefirstterms[IDX_OSP];
            case IDX_POS:
                return newpairs2->getBuffer()[posfirst];
            case IDX_PSO:
                return newpairs1->getBuffer()[posfirst];
        }
    } else { //posvalues = 2
        switch (idx) {
            case IDX_SPO:
                return nuniquefirstterms[IDX_SPO];
            case IDX_SOP:
                return nuniquefirstterms[IDX_SOP];
            case IDX_OPS:
                return newpairs2->getBuffer()[posfirst];
            case IDX_OSP:
                return newpairs1->getBuffer()[posfirst];
            case IDX_POS:
                return nuniquefirstterms[IDX_OPS];
            case IDX_PSO:
                return nuniquefirstterms[IDX_OSP];
        }
    }
    return 0;
}

void DiffIndex1::getTermListItr(int idx, DiffTermItr *itr) {
    int posFirstElement;
    if (idx == IDX_POS || idx == IDX_PSO) {
        posFirstElement = 1;
    } else if (idx == IDX_SPO || idx == IDX_SOP) {
        posFirstElement = 0;
    } else {
        posFirstElement = 2;
    }
    if (posFirstElement == posValues) {
        itr->init(idx, nkeys[idx], nuniquekeys[idx], values->getBuffer(), nbytes);
    } else {
        itr->init(idx, nkeys[idx], nuniquekeys[idx], triple[posFirstElement]);
    }
}

void DiffIndex1::setFactoryIterators(Factory<Diff1Itr> *f) {
    this->factory = f;
}

bool DiffIndex1::valueInArray(const int64_t v) const {
    const char *s = values->getBuffer();
    const char *e = values->getBuffer() + (size * nbytes);
    while (s < e) {
        size_t nelements = (e - s) / nbytes;
        const char *middle = s + nelements / 2 * nbytes;
        int64_t middleValue;
        if (nbytes == 4)
            middleValue = Utils::decode_int(middle);
        else {
            assert(nbytes == 8);
            middleValue = Utils::decode_long(middle);
        }
        if (middleValue < v) {
            s = middle + nbytes;
        } else if (middleValue > v) {
            e = middle;
        } else {
            return true;
        }
    }
    return false;
}

int DiffIndex1::posValueInArray(const int64_t v) const {
    const char *s = values->getBuffer();
    const char *e = values->getBuffer() + (size * nbytes);
    while (s < e) {
        size_t pivot = ((e - s) / nbytes) / 2;
        const char *middle = s + pivot * nbytes;
        int64_t middleValue;
        if (nbytes == 4)
            middleValue = Utils::decode_int(middle);
        else {
            assert(nbytes == 8);
            middleValue = Utils::decode_long(middle);
        }
        if (middleValue < v) {
            s = middle + nbytes;
        } else if (middleValue > v) {
            e = middle;
        } else {
            return (middle - values->getBuffer()) / nbytes;
        }
    }
    return size;
}

int64_t DiffIndex1::getCard(int idx, int64_t first) const {
    if (idx == IDX_POS || idx == IDX_PSO) {
        if (posValues == 1) {
            //if first is in the array, then card is one, otherwise it is 0
            if (valueInArray(first)) {
                return 1;
            } else {
                return 0;
            }
        } else {
            if (first == triple[1])
                return size;
            else
                return 0;
        }
    } else if (idx == IDX_SPO || idx == IDX_SOP) {
        if (posValues == 0) {
            if (valueInArray(first)) {
                return 1;
            } else {
                return 0;
            }
        } else {
            if (first == triple[0])
                return size;
            else
                return 0;
        }
    } else { //OPS and OSP
        if (posValues == 2) {
            if (valueInArray(first)) {
                return 1;
            } else {
                return 0;
            }
        } else {
            if (first == triple[2])
                return size;
            else
                return 0;
        }
    }
    return 0;
}

int64_t DiffIndex1::getSize() const {
    return size;
}

int64_t DiffIndex1::getNUniqueKeys(int idx) {
    return nuniquekeys[idx];
}

int64_t DiffIndex1::getUniqueNFirstTerms(int idx) {
    return nuniquefirstterms[idx];
}

int64_t DiffIndex1::getNFirstTables(int idx) {
    return nfirstterms[idx];
}

void DiffIndex1::createDiffIndex(string outputdir,
        bool dumpRawFormat,
        Querier *q,
        int64_t triple[3],
        std::vector<uint64_t> &values,
        uint8_t posvalues) {
    std::chrono::system_clock::time_point start = std::chrono::system_clock::now();
    //Write an empty file to indicate this is a type 1 update
    ofstream f;
    f.open(outputdir + "/type1");
    f.put(0);
    f.close();

    if (dumpRawFormat) {
        Utils::create_directories(outputdir);
        LZ4Writer writer(outputdir + "/raw");
        for (size_t i = 0; i < values.size(); ++i) {
            for (size_t j = 0; j < 3; ++j) {
                if (j == posvalues) {
                    assert(triple[j] == -1);
                    writer.writeLong(values[i]);
                } else {
                    assert(triple[j] >= 0);
                    writer.writeLong(triple[j]);
                }
            }
        }
    }


    //Create an empty file to store the values
    uint8_t nbytes = Utils::numBytesFixedLength(values.back());
    if (nbytes <= 4)
        nbytes = 4;
    else
        nbytes = 8;
    f.open(outputdir + "/values");
    f.seekp(values.size() * nbytes);
    f.put(0);
    f.close();

    //Write down the values
    {
        MemoryMappedFile mf(outputdir + "/values", false, 0, values.size() * nbytes);
        char *currentbuffer = mf.getData();
        for (size_t i = 0; i < values.size(); ++i) {
            uint64_t v = values[i];
            if (nbytes == 4) {
                Utils::encode_int(currentbuffer, v);
                currentbuffer += 4;
            } else {
                Utils::encode_long(currentbuffer, v);
                currentbuffer += 8;
            }
        }
        mf.flushAll();
    }

    //Calculate the new keys on the values
    int64_t valuesnewkeys = 0;
    PairItr *itrkey;
    if (posvalues == 0) {
        itrkey = q->getTermList(IDX_SPO);
    } else if (posvalues == 1) {
        itrkey = q->getTermList(IDX_POS);
    } else {
        itrkey = q->getTermList(IDX_OPS);
    }
    if (itrkey->hasNext()) {
        itrkey->next();
    } else {
        q->releaseItr(itrkey);
        itrkey = NULL;
    }
    for (size_t i = 0; i < values.size(); ++i) {
        int64_t v = values[i];
        if (itrkey) {
            while (itrkey->getKey() < v && itrkey->hasNext()) {
                itrkey->next();
            }
            if (itrkey->getKey() < v) {
                //the iterator is finished
                q->releaseItr(itrkey);
                itrkey = NULL;
                valuesnewkeys++;
            } else if (itrkey->getKey() != v) {
                valuesnewkeys++;
            }
        } else {
            valuesnewkeys++;
        }
    }

    //Calculate new keys on the constant fields, and the number of (unique) first terms
    const int64_t size = values.size();
    int64_t nkeys[3];
    int64_t nuniquekeys[3];
    int64_t nfirstterms[6];
    int64_t nuniquefirstterms[6];
    for (int i = 0; i < 6; ++i) {
        nfirstterms[i] = 1;
    }

    if (posvalues == 0) {
        /*** KEYS ***/
        nkeys[0] = size;
        nuniquekeys[0] = valuesnewkeys;
        nkeys[1] = 1;
        nkeys[2] = 1;
        if (q->existKey(IDX_POS, triple[1])) {
            nuniquekeys[1] = 0;
        } else {
            nuniquekeys[1] = 1;
        }
        if (q->existKey(IDX_OPS, triple[2])) {
            nuniquekeys[2] = 0;
        } else {
            nuniquekeys[2] = 1;
        }
        /*** FIRST TERMS ***/
        nfirstterms[IDX_PSO] = nfirstterms[IDX_OSP] = nfirstterms[IDX_SPO]
            = nfirstterms[IDX_SOP] = size;
        //Try PS
        assert(triple[1] >= 0);
        PairItr *itr = q->getPermuted(IDX_PSO, triple[1], -1, -1);
        string fout = outputdir + "/newps";
        nuniquefirstterms[IDX_PSO] = nuniquefirstterms[IDX_SPO] = outerJoin(itr, values, fout);
        q->releaseItr(itr);
        //Try OS
        assert(triple[2] >= 0);
        itr = q->getPermuted(IDX_OSP, triple[2], -1, -1);
        fout = outputdir + "/newos";
        nuniquefirstterms[IDX_OSP] = nuniquefirstterms[IDX_SOP] = outerJoin(itr, values, fout);
        q->releaseItr(itr);
        //Try PO
        itr = q->getPermuted(IDX_POS, triple[1], triple[2], -1);
        if (itr->hasNext()) {
            nuniquefirstterms[IDX_POS] = nuniquefirstterms[IDX_OPS] = 0;
        } else {
            nuniquefirstterms[IDX_POS] = nuniquefirstterms[IDX_OPS] = 1;
        }
        q->releaseItr(itr);
    } else if (posvalues == 1) {
        /*** KEYS ***/
        nkeys[0] = 1;
        nkeys[1] = size;
        nuniquekeys[1] = valuesnewkeys;
        nkeys[2] = 1;
        if (q->existKey(IDX_SPO, triple[0])) {
            nuniquekeys[0] = 0;
        } else {
            nuniquekeys[0] = 1;
        }
        if (q->existKey(IDX_OPS, triple[2])) {
            nuniquekeys[2] = 0;
        } else {
            nuniquekeys[2] = 1;
        }
        /*** FIRST TERMS ***/
        nfirstterms[IDX_SPO] = nfirstterms[IDX_OPS] = nfirstterms[IDX_PSO] =
            nfirstterms[IDX_POS] = size;
        //Try SP
        PairItr *itr = q->getPermuted(IDX_SPO, triple[0], -1, -1);
        string fout = outputdir + "/newsp";
        nuniquefirstterms[IDX_SPO] = nuniquefirstterms[IDX_PSO] = outerJoin(itr, values, fout);
        q->releaseItr(itr);
        //Try OP
        itr = q->getPermuted(IDX_OPS, triple[2], -1, -1);
        fout = outputdir + "/newop";
        nuniquefirstterms[IDX_OPS] = nuniquefirstterms[IDX_POS] = outerJoin(itr, values, fout);
        q->releaseItr(itr);
        //Try SO
        itr = q->getPermuted(IDX_SOP, triple[0], triple[2], -1);
        if (itr->hasNext()) {
            nuniquefirstterms[IDX_SOP] = nuniquefirstterms[IDX_OSP] = 0;
        } else {
            nuniquefirstterms[IDX_SOP] = nuniquefirstterms[IDX_OSP] = 1;
        }
        q->releaseItr(itr);
    } else {
        /*** KEYS ***/
        nkeys[0] = 1;
        nkeys[1] = 1;
        nkeys[2] = size;
        nuniquekeys[2] = valuesnewkeys;
        if (q->existKey(IDX_SPO, triple[0])) {
            nuniquekeys[0] = 0;
        } else {
            nuniquekeys[0] = 1;
        }
        if (q->existKey(IDX_POS, triple[1])) {
            nuniquekeys[1] = 0;
        } else {
            nuniquekeys[1] = 1;
        }
        /*** FIRST TERMS ***/
        nfirstterms[IDX_POS] = nfirstterms[IDX_SOP] = nfirstterms[IDX_OSP] =
            nfirstterms[IDX_OPS] = size;
        //Try SO
        PairItr *itr = q->getPermuted(IDX_SOP, triple[0], -1, -1);
        string fout = outputdir + "/newso";
        nuniquefirstterms[IDX_SOP] = nuniquefirstterms[IDX_OSP] = outerJoin(itr, values, fout);
        q->releaseItr(itr);
        //Try PO
        itr = q->getPermuted(IDX_POS, triple[1], -1, -1);
        fout = outputdir + "/newpo";
        nuniquefirstterms[IDX_POS] = nuniquefirstterms[IDX_OPS] = outerJoin(itr, values, fout);
        q->releaseItr(itr);
        //Try SP
        itr = q->getPermuted(IDX_SPO, triple[0], triple[1], -1);
        if (itr->hasNext()) {
            nuniquefirstterms[IDX_SPO] = nuniquefirstterms[IDX_PSO] = 0;
        } else {
            nuniquefirstterms[IDX_SPO] = nuniquefirstterms[IDX_PSO] = 1;
        }
        q->releaseItr(itr);
    }

    //Write statistics on file
    f.open(outputdir + "/stats");
    char buffer[8];
    buffer[0] = nbytes;
    buffer[1] = posvalues;
    f.write(buffer, 2);
    Utils::encode_long(buffer, size);
    f.write(buffer, 8);
    for (int i = 0; i < 3; ++i) {
        Utils::encode_long(buffer, triple[i]);
        f.write(buffer, 8);
    }
    for (int i = 0; i < 3; ++i) {
        Utils::encode_long(buffer, nkeys[i]);
        f.write(buffer, 8);
        Utils::encode_long(buffer, nuniquekeys[i]);
        f.write(buffer, 8);
    }
    for (int i = 0; i < 6; ++i) {
        Utils::encode_long(buffer, nfirstterms[i]);
        f.write(buffer, 8);
        Utils::encode_long(buffer, nuniquefirstterms[i]);
        f.write(buffer, 8);
    }
    f.close();
    std::chrono::duration<double> sec = std::chrono::system_clock::now()
        - start;
    LOG(INFOL) << "Runtime creating indices one column = " << sec.count() * 1000;
}

int64_t DiffIndex1::outerJoin(PairItr *itr, std::vector<uint64_t> &values, string fout) {
    std::chrono::system_clock::time_point start = std::chrono::system_clock::now();
    ofstream f(fout);
    itr->ignoreSecondColumn();
    if (itr->hasNext()) {
        itr->next();
    } else {
        itr = NULL;
    }
    int64_t out = 0;
    for (size_t i = 0; i < values.size(); ++i) {
        if (itr) {
            while (itr->getValue1() < values[i] && itr->hasNext()) {
                itr->next();
            }
            if (itr->getValue1() < values[i]) {
                itr = NULL;
                out++;
                f << (uint8_t)1;
            } else if (itr->getValue1() != values[i]) {
                out++;
                f << (uint8_t)1;
            } else {
                f << (uint8_t)0;
            }
        } else {
            out++;
            f << (uint8_t)1;
        }
    }
    std::chrono::duration<double> sec = std::chrono::system_clock::now()
        - start;
    LOG(INFOL) << "Runtime checking first pairs = " << sec.count() * 1000;
    return out;
}
