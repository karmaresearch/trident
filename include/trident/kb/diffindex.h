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


#ifndef _DIFFINDEX_H
#define _DIFFINDEX_H

#include <trident/iterators/pairitr.h>
#include <trident/iterators/termitr.h>
#include <trident/iterators/difftermitr.h>
#include <trident/iterators/diff1itr.h>

#include <trident/kb/updatestats.h>
#include <trident/tree/coordinates.h>
#include <trident/utils/propertymap.h>
#include <trident/kb/kbconfig.h>
#include <kognac/factory.h>

#include <string>

#define THRESHOLD_USEGLOBALFILES 1000000

class Querier;
class Root;
class StorageStrat;
class DiffScanItr;

class RWMappedFile {
private:
    const std::string file;
    std::unique_ptr<MemoryMappedFile> mf;

    uint64_t currentposition;
    char *currentbuffer;
    size_t spaceleft;

public:
    struct Block {
        uint64_t begin;
        char *buffer;
    };

    RWMappedFile(std::string file, size_t initialsize);

    Block getNewBlock(const size_t maxTableSize);

    void setLastBlockSize(const size_t spaceUsed);

    ~RWMappedFile();
};

class ROMappedFile {
private:
    MemoryMappedFile mf;
    const char *buffer;

public:
    ROMappedFile(std::string file) : mf(file) {
        buffer = mf.getData();
    }
    const char *getBuffer() {
        return buffer;
    }
};


class DiffIndex {
public:
    enum TypeUpdate {ADDITION, DELETE };
    enum ClassUpdate { DIFF1, DIFF3 };

private:
    const TypeUpdate type;
    const ClassUpdate clazz;

public:
    DiffIndex(TypeUpdate type, ClassUpdate clazz) : type(type), clazz(clazz) {
    }

    TypeUpdate getType() const {
        return type;
    }

    ClassUpdate getClass() const {
        return clazz;
    }

    virtual PairItr *getIterator(int idx, int64_t first, int64_t second, int64_t third,
                                 int64_t &nfirstterms) = 0;

    virtual int64_t getSize() const = 0;

    virtual int64_t getCard(int idx, int64_t first) const = 0;

    virtual int64_t getNUniqueKeys(int idx) = 0;

    virtual void getTermListItr(int idx, DiffTermItr *itr) = 0;

    virtual int64_t getUniqueNFirstTerms(int idx) = 0;

    virtual int64_t getNFirstTables(int idx) = 0;

    virtual ~DiffIndex() {}

};

class DiffIndex1 : public DiffIndex {
private:
    int64_t size;
    uint8_t nbytes;
    int64_t nkeys[3];
    int64_t nuniquekeys[3];
    int64_t nfirstterms[6];
    int64_t nuniquefirstterms[6];

    int64_t triple[3];
    uint8_t posValues;
    std::unique_ptr<ROMappedFile> values;
    std::unique_ptr<ROMappedFile> newpairs1;
    std::unique_ptr<ROMappedFile> newpairs2;
    Factory<Diff1Itr> *factory;

    static int64_t outerJoin(PairItr *itr, std::vector<uint64_t> &values, string filenewkeys);

    bool valueInArray(const int64_t v) const;

    int posValueInArray(const int64_t v) const;

    int64_t getNFirstTerms(const int idx, const int64_t first, const int64_t posfirst,
                        const int64_t second);

public:
    DiffIndex1(string dir, DiffIndex::TypeUpdate type);

    PairItr *getIterator(int idx, int64_t first, int64_t second, int64_t third,
                         int64_t &nfirstterms);

    int64_t getSize() const;

    int64_t getCard(int idx, int64_t first) const;

    int64_t getNUniqueKeys(int idx);

    void getTermListItr(int idx, DiffTermItr *itr);

    int64_t getUniqueNFirstTerms(int idx);

    int64_t getNFirstTables(int idx);

    void setFactoryIterators(Factory<Diff1Itr> *f);

    static void createDiffIndex(string outputdir,
                                bool dumpRawFormat,
                                Querier *q,
                                int64_t triple[3],
                                std::vector<uint64_t> &values,
                                uint8_t posvalues);

};

class DiffIndex3 : public DiffIndex {
private:
    struct StatStrategy {
        int64_t bothRowsSmallCounter;
        int64_t bothRowsCounter;
        int64_t bothColumnsCounter;
        int64_t columnRowCounter;
        int64_t rowColumnCounter;

        StatStrategy() : bothRowsSmallCounter(0), bothRowsCounter(0),
            bothColumnsCounter(0), columnRowCounter(0),
            rowColumnCounter(0) {}
    };

    std::string dir;
    std::unique_ptr<Root> s;
    int64_t nkeys_s;
    std::unique_ptr<Root> p;
    int64_t nkeys_p;
    std::unique_ptr<Root> o;
    int64_t nkeys_o;
    std::unique_ptr<ROMappedFile> spo_f;
    std::unique_ptr<ROMappedFile> sop_f;
    std::unique_ptr<ROMappedFile> pos_f;
    std::unique_ptr<ROMappedFile> pso_f;
    std::unique_ptr<ROMappedFile> ops_f;
    std::unique_ptr<ROMappedFile> osp_f;
    Root *roots[6];
    const char *buffers[6];
    StorageStrat *strat;
    int64_t size;

    //Data structures to contain the unique keys. At the moment they are not used
    int64_t nuniquekeys[6];
    int64_t nuniquefirstterms[6];
    int64_t nfirstterms[6];

    static bool shouldUseColumns(const std::vector<uint64_t> &table);

    static void getStrategyAndInserter(const std::vector<uint64_t> &tmp1,
                                       const std::vector<uint64_t> &tmp2,
                                       char &strat1,
                                       char &strat2,
                                       char &flagbytes1,
                                       char &flagbytes2);

    static uint64_t storeRowFormat(const std::vector<uint64_t> &tmp1,
                                   const std::vector<uint64_t> &tmp2,
                                   char *&buffer1,
                                   char *&buffer2,
                                   int64_t *counters1,
                                   int64_t *counters2,
                                   UpdateStats *stats,
                                   char &strat1,
                                   char &strat2);

    static uint64_t storeRowColumnFormat(const std::vector<uint64_t> &tmp1,
                                         const std::vector<uint64_t> &tmp2,
                                         char *&buffer1,
                                         char *&buffer2,
                                         int64_t *counters1,
                                         int64_t *counters2,
                                         UpdateStats *stats,
                                         char &strat1,
                                         char &strat2,
                                         const bool invCounters);

    static uint64_t storeColumnFormat(const std::vector<uint64_t> &tmp1,
                                      const std::vector<uint64_t> &tmp2,
                                      char *&buffer1,
                                      char *&buffer2,
                                      UpdateStats *stats,
                                      char &strat1,
                                      char &strat2);

    static uint64_t storeTablesOnBuffer(const int64_t key,
                                        const std::vector<uint64_t> &tmp1,
                                        const std::vector<uint64_t> &tmp2,
                                        const int perm1,
                                        const int perm2,
                                        std::unique_ptr<RWMappedFile> &mappedFile1,
                                        std::unique_ptr<RWMappedFile> &mappedFile2,
                                        StatStrategy &stat,
                                        int64_t *counters1,
                                        int64_t *counters2,
                                        UpdateStats *stats);

    static size_t sortIndex(string outputdir,
                            string globaldir,
                            int perm1,
                            int perm2,
                            std::vector<uint32_t> &idx1,
                            std::vector<uint64_t> &firstcolumn,
                            std::vector<uint64_t> &secondcolumn,
                            std::vector<uint64_t> &thirdcolumn,
                            PropertyMap & map,
                            Querier *q,
                            UpdateStats *stats,
                            const bool sort);


public:

    DiffIndex3(std::string dir, const char **globalbuffers, KBConfig &config,
               TypeUpdate type);

    PairItr *getIterator(int idx, int64_t first, int64_t second, int64_t third,
                         int64_t &nfirstterms);

    PairItr *getIterator(int idx, int64_t key, TermCoordinates &coord);

    PairItr *getScan(int idx, DiffScanItr *itr);

    int64_t getSize() const;

    int64_t getCard(int idx, int64_t first) const;

    int64_t getNUniqueKeys(int idx);

    void getTermListItr(int idx, DiffTermItr *itr);

    int64_t getUniqueNFirstTerms(int idx);

    int64_t getNFirstTables(int idx);

    void setStorageStrat(StorageStrat *strat) {
        this->strat = strat;
    }

    static void createDiffIndex(DiffIndex::TypeUpdate type,
                                string outputdir,
                                string diffdir,
                                std::vector<uint64_t> &all_s,
                                std::vector<uint64_t> &all_p,
                                std::vector<uint64_t> &all_o,
                                bool dumpRawFormat,
                                Querier *q,
                                bool sort);

    ~DiffIndex3();
};

#endif
