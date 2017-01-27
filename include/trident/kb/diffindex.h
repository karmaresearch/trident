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

#include <boost/interprocess/file_mapping.hpp>
#include <boost/interprocess/mapped_region.hpp>
#include<boost/iostreams/device/mapped_file.hpp>
#include <string>

#define THRESHOLD_USEGLOBALFILES 1000000

namespace bip = boost::interprocess;
namespace memmap = boost::iostreams;

class Querier;
class Root;
class StorageStrat;
class DiffScanItr;

class RWMappedFile {
private:
    const std::string file;
    memmap::mapped_file_sink sink;

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
    bip::file_mapping mapping;
    bip::mapped_region mapped_rgn;
    const char *buffer;

public:
    ROMappedFile(std::string file) :
        mapping(file.c_str(), bip::read_only),
        mapped_rgn(mapping, bip::read_only) {
        buffer = static_cast<const char*>(mapped_rgn.get_address());
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

    virtual PairItr *getIterator(int idx, long first, long second, long third,
                                 long &nfirstterms) = 0;

    virtual long getSize() const = 0;

    virtual long getCard(int idx, long first) const = 0;

    virtual long getNUniqueKeys(int idx) = 0;

    virtual void getTermListItr(int idx, DiffTermItr *itr) = 0;

    virtual long getUniqueNFirstTerms(int idx) = 0;

    virtual long getNFirstTables(int idx) = 0;

    virtual ~DiffIndex() {}

};

class DiffIndex1 : public DiffIndex {
private:
    long size;
    uint8_t nbytes;
    long nkeys[3];
    long nuniquekeys[3];
    long nfirstterms[6];
    long nuniquefirstterms[6];

    long triple[3];
    uint8_t posValues;
    std::unique_ptr<ROMappedFile> values;
    std::unique_ptr<ROMappedFile> newpairs1;
    std::unique_ptr<ROMappedFile> newpairs2;
    Factory<Diff1Itr> *factory;

    static long outerJoin(PairItr *itr, std::vector<uint64_t> &values, string filenewkeys);

    bool valueInArray(const long v) const;

    int posValueInArray(const long v) const;

    long getNFirstTerms(const int idx, const long first, const long posfirst,
                        const long second);

public:
    DiffIndex1(string dir, DiffIndex::TypeUpdate type);

    PairItr *getIterator(int idx, long first, long second, long third,
                         long &nfirstterms);

    long getSize() const;

    long getCard(int idx, long first) const;

    long getNUniqueKeys(int idx);

    void getTermListItr(int idx, DiffTermItr *itr);

    long getUniqueNFirstTerms(int idx);

    long getNFirstTables(int idx);

    void setFactoryIterators(Factory<Diff1Itr> *f);

    static void createDiffIndex(string outputdir,
                                bool dumpRawFormat,
                                Querier *q,
                                long triple[3],
                                std::vector<uint64_t> &values,
                                uint8_t posvalues);

};

class DiffIndex3 : public DiffIndex {
private:
    struct StatStrategy {
        long bothRowsSmallCounter;
        long bothRowsCounter;
        long bothColumnsCounter;
        long columnRowCounter;
        long rowColumnCounter;

        StatStrategy() : bothRowsSmallCounter(0), bothRowsCounter(0),
            bothColumnsCounter(0), columnRowCounter(0),
            rowColumnCounter(0) {}
    };

    std::string dir;
    std::unique_ptr<Root> s;
    long nkeys_s;
    std::unique_ptr<Root> p;
    long nkeys_p;
    std::unique_ptr<Root> o;
    long nkeys_o;
    std::unique_ptr<ROMappedFile> spo_f;
    std::unique_ptr<ROMappedFile> sop_f;
    std::unique_ptr<ROMappedFile> pos_f;
    std::unique_ptr<ROMappedFile> pso_f;
    std::unique_ptr<ROMappedFile> ops_f;
    std::unique_ptr<ROMappedFile> osp_f;
    Root *roots[6];
    const char *buffers[6];
    StorageStrat *strat;
    long size;

    //Data structures to contain the unique keys. At the moment they are not used
    long nuniquekeys[6];
    long nuniquefirstterms[6];
    long nfirstterms[6];

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
                                   long *counters1,
                                   long *counters2,
                                   UpdateStats *stats,
                                   char &strat1,
                                   char &strat2);

    static uint64_t storeRowColumnFormat(const std::vector<uint64_t> &tmp1,
                                         const std::vector<uint64_t> &tmp2,
                                         char *&buffer1,
                                         char *&buffer2,
                                         long *counters1,
                                         long *counters2,
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

    static uint64_t storeTablesOnBuffer(const long key,
                                        const std::vector<uint64_t> &tmp1,
                                        const std::vector<uint64_t> &tmp2,
                                        const int perm1,
                                        const int perm2,
                                        std::unique_ptr<RWMappedFile> &mappedFile1,
                                        std::unique_ptr<RWMappedFile> &mappedFile2,
                                        StatStrategy &stat,
                                        long *counters1,
                                        long *counters2,
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

    PairItr *getIterator(int idx, long first, long second, long third,
                         long &nfirstterms);

    PairItr *getIterator(int idx, long key, TermCoordinates &coord);

    PairItr *getScan(int idx, DiffScanItr *itr);

    long getSize() const;

    long getCard(int idx, long first) const;

    long getNUniqueKeys(int idx);

    void getTermListItr(int idx, DiffTermItr *itr);

    long getUniqueNFirstTerms(int idx);

    long getNFirstTables(int idx);

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
