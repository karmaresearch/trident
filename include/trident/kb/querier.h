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


#ifndef QUERIER_H_
#define QUERIER_H_

//#include <trident/iterators/storageitr.h>
#include <trident/iterators/arrayitr.h>
#include <trident/iterators/scanitr.h>
#include <trident/iterators/aggritr.h>
#include <trident/iterators/cacheitr.h>
#include <trident/iterators/termitr.h>
#include <trident/iterators/compositeitr.h>
#include <trident/iterators/compositetermitr.h>
#include <trident/iterators/difftermitr.h>
#include <trident/iterators/diffscanitr.h>
#include <trident/iterators/compositescanitr.h>
#include <trident/iterators/rmitr.h>
#include <trident/iterators/rmcompositetermitr.h>

#include <trident/tree/coordinates.h>
#include <trident/binarytables/storagestrat.h>
#include <trident/binarytables/factorytables.h>
#include <trident/kb/diffindex.h>

#include <kognac/factory.h>

#include <boost/chrono.hpp>

#include <iostream>

namespace timens = boost::chrono;

class TableStorage;
class ListPairHandler;
class GroupPairHandler;
class Root;
class DictMgmt;
class CacheIdx;
class KB;

class Querier {
    private:
        Root* tree;
        DictMgmt *dict;
        TableStorage **files;
        long lastKeyQueried;
        bool lastKeyFound;
        const long inputSize;
        const long nTerms;
        const long *nTablesPerPartition;
        const long *nFirstTablesPerPartition;

        std::string pathRawData;
        bool copyRawData;

        const int nindices;

        std::vector<std::unique_ptr<DiffIndex>> &diffIndices;
        std::unique_ptr<Querier> sampler;

        TermCoordinates currentValue;

        //Factories
        Factory<ArrayItr> factory2;
        Factory<ScanItr> factory3;
        Factory<AggrItr> factory4;
        Factory<CacheItr> factory5;
        Factory<TermItr> factory7;
        Factory<CompositeItr> factory8;
        Factory<CompositeTermItr> factory9;
        Factory<DiffTermItr> factory10;
        Factory<DiffScanItr> factory11;
        Factory<CompositeScanItr> factory12;
        Factory<Diff1Itr> factory13;
        Factory<RmItr> factory14;
        Factory<RmCompositeTermItr> factory15;

        Factory<NewColumnTable> ncFactory;
        FactoryNewRowTable nrFactory;
        FactoryNewClusterTable ncluFactory;

        StorageStrat strat;

        //Statistics
        long aggrIndices, notAggrIndices, cacheIndices;
        long spo, ops, pos, sop, osp, pso;

        void initNewIterator(TableStorage *storage,
                int fileIdx,
                long mark,
                PairItr *t,
                long v1,
                long v2,
                const bool setConstraints);

    public:

        struct Counters {
            long statsRow;
            long statsColumn;
            long statsCluster;
            long aggrIndices;
            long notAggrIndices;
            long cacheIndices;
            long spo, ops, pos, sop, osp, pso;
        };

        Querier(Root* tree, DictMgmt *dict, TableStorage** files,
                const long inputSize,
                const long nTerms, const int nindices,
                const long* nTablesPerPartition,
                const long* nFirstTablesPerPartition,
                KB *sampleKB,
                std::vector<std::unique_ptr<DiffIndex>> &diffIndices);

        void initDiffIndex(DiffIndex *diff);

        TermItr *getKBTermList(const int perm, const bool enforcePerm);

        PairItr *getTermList(const int perm);

        bool existKey(int perm, long key);

        TableStorage *getTableStorage(const int perm) {
            if (nindices <= perm)
                return NULL;
            else
                return files[perm];
        }

        StorageStrat *getStorageStrat() {
            return &strat;
        }

        PairItr *get(const int idx, const long s, const long p, const long o) {
            return get(idx, s, p, o, true);
        }

        PairItr *get(const int idx, const long s, const long p,
                const long o, const bool cons);

        PairItr *get(const int idx, TermCoordinates &value,
                const long key, const long v1,
                const long v2, const bool cons);

        PairItr *get(const int perm,
                const long key,
                const short fileIdx,
                const long mark,
                const char strategy,
                const long v1,
                const long v2,
                const bool constrain,
                const bool noAggr);

        PairItr *getPermuted(const int idx, const long el1, const long el2,
                const long el3, const bool constrain);

        uint64_t isAggregated(const int idx, const long first, const long second,
                const long third);

        uint64_t isReverse(const int idx, const long first, const long second,
                const long third);

        uint64_t getCardOnIndex(const int idx, const long first, const long second, const long third) {
            return getCardOnIndex(idx, first, second, third, false);
        }

        uint64_t getCardOnIndex(const int idx, const long first, const long second,
                const long third, bool skipLast);

        long getCard(const long s, const long p, const long o);

        long getCard(const long s, const long p, const long o, uint8_t pos);

        //uint64_t getCard(const int idx, const long v);

        uint64_t estCardOnIndex(const int idx, const long first, const long second,
                const long third);

        long estCard(const long s, const long p, const long o);

        bool isEmpty(const long s, const long p, const long o);

        int getIndex(const long s, const long p, const long o);

        char getStrategy(const int idx, const long v);

        int *getOrder(int idx);

        int *getInvOrder(int idx);

        uint64_t getInputSize() const {
            uint64_t tot = inputSize;
            for (size_t i = 0; i < diffIndices.size(); ++i) {
                long sizeUpdate = diffIndices[i]->getSize();
                if (diffIndices[i]->getType() == DiffIndex::TypeUpdate::ADDITION) {
                    tot += sizeUpdate;
                } else {
                    tot -= sizeUpdate;
                }
            }
            return tot;
        }

        uint64_t getNFirstTablesPerPartition(const int idx) {
            uint64_t output = nFirstTablesPerPartition[idx];
            if (!diffIndices.empty()) {
                for (size_t i = 0; i < diffIndices.size(); ++i) {
                    if (diffIndices[i]->getType() == DiffIndex::TypeUpdate::ADDITION) {
                        output += diffIndices[i]->getUniqueNFirstTerms(idx);
                    } else {
                        output -= diffIndices[i]->getUniqueNFirstTerms(idx);
                    }
                }
            }
            return output;
        }

        uint64_t getNTerms() const {
            return nTerms;
        }

        void releaseItr(PairItr *itr);

        void resetCounters() {
            strat.resetCounters();
            aggrIndices = notAggrIndices = cacheIndices = 0;
            spo = ops = pos = sop = osp = pso = 0;
        }

        Counters getCounters() {
            Counters c;
            c.statsRow = strat.statsRow;
            c.statsColumn = strat.statsColumn;
            c.statsCluster = strat.statsCluster;
            c.aggrIndices = aggrIndices;
            c.notAggrIndices = notAggrIndices;
            c.cacheIndices = cacheIndices;
            c.spo = spo;
            c.ops = ops;
            c.pos = pos;
            c.sop = sop;
            c.osp = osp;
            c.pso = pso;
            return c;
        }

        ArrayItr *getArrayIterator() {
            return factory2.get();
        }

        PairItr *getPairIterator(TermCoordinates *value,
                int perm,
                const long key,
                long c1,
                long c2,
                const bool constrain,
                const bool noAggr) {
            short fileIdx = value->getFileIdx(perm);
            long mark = value->getMark(perm);
            char strategy = value->getStrategy(perm);
            return get(perm, key, fileIdx, mark, strategy, c1, c2, constrain, noAggr);
        }

        PairItr *newItrOnReverse(PairItr *itr, const long v1, const long v2);

        PairItr *getFilePairIterator(const int perm, const long constraint1,
                const long constraint2,
                const char strategy, const short file,
                const long pos) {
            PairItr *t = strat.getBinaryTable(strategy);
            initNewIterator(files[perm], file, pos, (PairItr*) t, constraint1, constraint2, true);
            return t;
        }

        DictMgmt *getDictMgmt() {
            return dict;
        }

        Querier &getSampler() {
            if (sampler == NULL) {
                BOOST_LOG_TRIVIAL(error) << "No sampler available";
                throw 10;
            } else {
                return *sampler;
            }
        }

        std::string getPathRawData() {
            return pathRawData;
        }

        /*//The second row is ignored! This is useful for unlabelled graphs
          long getValue1AtRow(const int idx,
          const short fileIdx,
          const int mark,
          const char strategy,
          const long rowId);*/

        const char *getTable(const int perm,
                const short fileId,
                const long markId);

};

#endif
