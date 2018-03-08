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

#define DICTMGMT_INTEGER  UINT64_C(0x4000000000000000)
#define DICTMGMT_FLOAT UINT64_C(0x8000000000000000)

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
            int64_t size;
            int64_t nextid;

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
        //int64_t* dataToProcess;
        int nTuples;
        int sTuples;
        bool printTuples;

        int64_t *insertedNewTerms;
        int64_t largestID;

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

        int64_t getNTermsInserted() {
            return insertedNewTerms[0];
        }

        int64_t getLargestIDInserted() {
            return largestID;
        }

        TreeItr *getInvDictIterator();

        TreeItr *getDictIterator();

        StringBuffer *getStringBuffer();

        LIBEXP bool getText(nTerm key, char *value);

        bool getTextRel(nTerm key, char *value, int &size);

        bool getText(nTerm key, char *value, int &size);

        void getTextFromCoordinates(int64_t coordinates, char *output,
                int &sizeOutput);

        LIBEXP bool getNumber(const char *key, const int sizeKey, nTerm *value);

        bool putDict(const char *key, int sizeKey, nTerm &value);

        bool putDict(const char *key, int sizeKey, nTerm &value,
                int64_t &coordinates);

        bool putInvDict(const char *key, int sizeKey, nTerm &value);

        bool putInvDict(const nTerm key, const int64_t coordinates);

        bool putPair(const char *key, int sizeKey, nTerm &value);

        void appendPair(const char *key, int sizeKey, nTerm &value);

        bool useHashForCompression() {
            return hash;
        }

        void clean() {
            dictionaries.clear();
        }

        /*** METHODS USED WHEN THE IDS ARE ACTUAL NUMBERS ***/
        static bool isnumeric(uint64_t term) {
            bool res = term & UINT64_C(0xC000000000000000); //check the two most significant bits are non-zero
            return res;
        }

        static uint64_t getIntValue(uint64_t term) {
            uint64_t raw = term & UINT64_C(0x3FFFFFFFFFFFFFFF);
            return raw;
        }

        static float getFloatValue(uint64_t term) {
            float fv = *((float*)(((char*)&term+4)));
            return fv;
        }

        static void setFloatValue(uint64_t &term) {
            term |= DICTMGMT_FLOAT;
        }

        static void setIntValue(uint64_t &term) {
            term |= DICTMGMT_INTEGER;
        }

        static string tostr(uint64_t term) {
            uint64_t raw = term & UINT64_C(0x3FFFFFFFFFFFFFFF);
            return to_string(raw);
        }

        static uint64_t getType(uint64_t term) {
            uint64_t type = term & UINT64_C(0xC000000000000000);
            return type;
        }

        static int compare(uint64_t type1, uint64_t number1, uint64_t type2, uint64_t number2) {
            if (type1 == DICTMGMT_INTEGER && type2 == DICTMGMT_INTEGER) {
                number1 = number1 & UINT64_C(0x3FFFFFFFFFFFFFFF);
                number2 = number2 & UINT64_C(0x3FFFFFFFFFFFFFFF);
                if (number1 < number2) {
                    return -1;
                } else if (number1 > number2) {
                    return 1;
                } else {
                    return 0;
                }
            } else if (type1 == DICTMGMT_INTEGER) {
                number1 = number1 & UINT64_C(0x3FFFFFFFFFFFFFFF);
                float n2 = *((float*)(((char*)&number2+4)));
                if (number1 < n2) {
                    return -1;
                } else if (number1 > n2) {
                    return 1;
                } else {
                    return 0;
                }
            } else if (type2 == DICTMGMT_INTEGER) {
                float n1 = *((float*)(((char*)&number1+4)));
                number2 = number2 & UINT64_C(0x3FFFFFFFFFFFFFFF);
                if (n1 < number2) {
                    return -1;
                } else if (n1 > number2) {
                    return 1;
                } else {
                    return 0;
                }
            } else { //both floats
                float n1 = *((float*)(((char*)&number1+4)));
                float n2 = *((float*)(((char*)&number2+4)));
                if (n1 < n2) {
                    return -1;
                } else if (n1 > n2) {
                    return 1;
                } else {
                    return 0;
                }

            }
        }

        ~DictMgmt();
};

#endif
