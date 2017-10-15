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


#ifndef DICTMGMT_H_
#define DICTMGMT_H_

#include <trident/kb/consts.h>
#include <trident/kb/statistics.h>

#include <kognac/hashfunctions.h>
#include <kognac/utils.h>
#include <kognac/logs.h>

#include <sparsehash/sparse_hash_map>
#include <sparsehash/dense_hash_map>

class Root;
class StringBuffer;
class TreeItr;

using namespace std;

class Row {
private:
    char *bData[MAX_N_PATTERNS];
    int size;
public:
    Row() {
        size = 0;
        for (int i = 0; i < MAX_N_PATTERNS; ++i) {
            bData[i] = NULL;
        }
    }
    void setSize(int size) {
        if (this->size < size) {
            for (int i = this->size; i < size; ++i) {
                if (bData[i] == NULL) {
                    bData[i] = new char[MAX_TERM_SIZE];
                }
            }
        }
        this->size = size;
    }

    char *getRawData(const int j) {
        return bData[j];
    }

    void printRow() {
        for (int i = 0; i < size; ++i) {
            std::cout << bData[i] << " ";
        }
        std::cout << "\n";
    }
    ~Row() {
        for (int i = 0; i < MAX_N_PATTERNS; ++i) {
            if (bData[i] != NULL)
                delete[] bData[i];
        }
    }
};

class DictMgmt {
public:
    struct Dict {
        std::shared_ptr<Stats> stats;
        std::shared_ptr<Root> dict;
        std::shared_ptr<Root> invdict;
        std::shared_ptr<StringBuffer> sb;
        long size;
        long nextid;

        Dict() {
            stats = std::shared_ptr<Stats>(new Stats());
            size = nextid = 0;
        }

        ~Dict() {
            LOG(DEBUGL) << "Deallocating dict ...";
            dict = std::shared_ptr<Root>();
            LOG(DEBUGL) << "Deallocating invdict ...";
            invdict = std::shared_ptr<Root>();
            LOG(DEBUGL) << "Deallocating sb ...";
            sb = std::shared_ptr<StringBuffer>();
            LOG(DEBUGL) << "Deallocating stats ...";
            stats = std::shared_ptr<Stats>();
        }
    };

private:

    //Define the task to execute
    //long* dataToProcess;
    int nTuples;
    int sTuples;
    bool printTuples;

    long *insertedNewTerms;
    long largestID;

    bool hash;

    Row row;

    //Used if additional terms are defined in the updates
    std::vector<uint64_t> beginrange;
    std::vector<Dict> dictionaries;
    //
    //global datastructures for small updates (GUD=global update dictionary)
    google::dense_hash_map<uint64_t, string> gud_idtext;
    google::sparse_hash_map<string, uint64_t> gud_textid;
    google::sparse_hash_map<uint64_t, uint64_t> r2e;
    google::sparse_hash_map<uint64_t, string> r2s;
    bool gud_modified;
    uint64_t gud_largestID;
    string gudLocation;

public:

    DictMgmt(Dict mainDict, string dirToStoreGUD, bool hash, string e2r,
            string e2s);

    void addUpdates(std::vector<Dict> &updates);

    void putInUpdateDict(const uint64_t id, const char *term, const size_t len);

    uint64_t getGUDSize() {
        return gud_idtext.size();
    }

    uint64_t getNRels() {
        return r2e.size() + r2s.size(); //Either one or the others are populated
    }

    uint64_t getLargestGUDTerm() {
        return gud_largestID;
    }

    long getNTermsInserted() {
        return insertedNewTerms[0];
    }

    long getLargestIDInserted() {
        return largestID;
    }

    TreeItr *getInvDictIterator();

    TreeItr *getDictIterator();

    StringBuffer *getStringBuffer();

    bool getText(nTerm key, char *value);

    bool getTextRel(nTerm key, char *value, int &size);

    bool getText(nTerm key, char *value, int &size);

    void getTextFromCoordinates(long coordinates, char *output,
                                int &sizeOutput);

    bool getNumber(const char *key, const int sizeKey, nTerm *value);

    bool putDict(const char *key, int sizeKey, nTerm &value);

    bool putDict(const char *key, int sizeKey, nTerm &value,
                 long &coordinates);

    bool putInvDict(const char *key, int sizeKey, nTerm &value);

    bool putInvDict(const nTerm key, const long coordinates);

    bool putPair(const char *key, int sizeKey, nTerm &value);

    void appendPair(const char *key, int sizeKey, nTerm &value);

    bool useHashForCompression() {
        return hash;
    }

    void clean() {
        dictionaries.clear();
    }

    ~DictMgmt();
};

#endif
