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

#include <iostream>

class TableStorage;
class ListPairHandler;
class GroupPairHandler;
class Root;
class DictMgmt;
class CacheIdx;
class KB;
class Partial;

class Querier {
    private:
        Root* tree;
        DictMgmt *dict;
        TableStorage **files;
        int64_t lastKeyQueried;
        bool lastKeyFound;
        const int64_t inputSize;
        const int64_t nTerms;
        const int64_t *nTablesPerPartition;
        const int64_t *nFirstTablesPerPartition;

        std::string pathRawData;
        bool copyRawData;

        // const int nindices;

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

        bool *present;

        Partial **partial;

        //Statistics
        int64_t aggrIndices, notAggrIndices, cacheIndices;
        int64_t spo, ops, pos, sop, osp, pso;

        void initNewIterator(TableStorage *storage,
                int fileIdx,
                int64_t mark,
                PairItr *t,
                int64_t v1,
                int64_t v2,
                const bool setConstraints);

        PairItr *summaryDiff(const int perm, DiffIndex::TypeUpdate tp);

        PairItr *getIterator(const int idx, const int64_t s, const int64_t p,
                const int64_t o, const bool cons);

        PairItr *get(const int idx, TermCoordinates &value,
                const int64_t key, const int64_t v1,
                const int64_t v2, const bool cons);

        static int64_t getCard_internal(Querier *q, const int64_t s, const int64_t p, const int64_t o);

    public:

        struct Counters {
            int64_t statsRow;
            int64_t statsColumn;
            int64_t statsCluster;
            int64_t aggrIndices;
            int64_t notAggrIndices;
            int64_t cacheIndices;
            int64_t spo, ops, pos, sop, osp, pso;
        };

        Querier(Root* tree, DictMgmt *dict, TableStorage** files,
                const int64_t inputSize,
                const int64_t nTerms, const int nindices,
                const int64_t* nTablesPerPartition,
                const int64_t* nFirstTablesPerPartition,
                KB *sampleKB,
                std::vector<std::unique_ptr<DiffIndex>> &diffIndices, bool *present, Partial **partial);

        void initDiffIndex(DiffIndex *diff);

        TermItr *getKBTermList(const int perm, const bool enforcePerm);

        DDLEXPORT PairItr *getTermList(const int perm);

        DDLEXPORT PairItr *summaryAddDiff();

        DDLEXPORT PairItr *summaryRmDiff();

        bool existKey(int perm, int64_t key);

        StorageStrat *getStorageStrat() {
            return &strat;
        }

        bool isPresent(int idx) {
            return present[idx];
        }

        DDLEXPORT PairItr *getIterator(const int idx, const int64_t s, const int64_t p, const int64_t o);

        // Method below should be phased out, but is part of the trident interface.
        DDLEXPORT PairItr *get(const int idx, const int64_t s, const int64_t p, const int64_t o) {
            return getIterator(idx, s, p, o);
        }

        PairItr *getIterator(const int perm,
                const int64_t key,
                const short fileIdx,
                const int64_t mark,
                const char strategy,
                const int64_t v1,
                const int64_t v2,
                const bool constrain,
                const bool noAggr);

        // This one may return NULL if the requested idx is not present
        DDLEXPORT PairItr *getPermuted(const int idx, const int64_t el1, const int64_t el2,
                const int64_t el3, const bool constrain);

        // but this one may not return NULL.
        DDLEXPORT PairItr *getPermuted(const int idx, const int64_t el1, const int64_t el2,
                const int64_t el3);

        DDLEXPORT uint64_t isAggregated(const int idx, const int64_t first, const int64_t second,
                const int64_t third);

        DDLEXPORT uint64_t isReverse(const int idx, const int64_t first, const int64_t second,
                const int64_t third);

        DDLEXPORT uint64_t getCardOnIndex(const int idx, const int64_t first, const int64_t second, const int64_t third) {
            return getCardOnIndex(idx, first, second, third, false);
        }

        DDLEXPORT uint64_t getCardOnIndex(const int idx, const int64_t first, const int64_t second,
                const int64_t third, bool skipLast);

        DDLEXPORT int64_t getCard(const int64_t s, const int64_t p, const int64_t o);

        DDLEXPORT int64_t getCard(const int64_t s, const int64_t p, const int64_t o, uint8_t pos);

        //uint64_t getCard(const int idx, const int64_t v);

        DDLEXPORT uint64_t estCardOnIndex(const int idx, const int64_t first, const int64_t second,
                const int64_t third);

        DDLEXPORT int64_t estCard(const int64_t s, const int64_t p, const int64_t o);

        DDLEXPORT bool isEmpty(const int64_t s, const int64_t p, const int64_t o);

        DDLEXPORT bool exists(const int64_t s, const int64_t p, const int64_t o);

        DDLEXPORT int getIndex(const int64_t s, const int64_t p, const int64_t o);

        char getStrategy(const int idx, const int64_t v);

        DDLEXPORT int *getOrder(int idx);

        DDLEXPORT int *getInvOrder(int idx);

        Partial *getPartial(int idx) {
            return partial[idx];
        }

        uint64_t getInputSize() const {
            uint64_t tot = inputSize;
            for (size_t i = 0; i < diffIndices.size(); ++i) {
                int64_t sizeUpdate = diffIndices[i]->getSize();
                if (diffIndices[i]->getType() == DiffIndex::TypeUpdate::ADDITION_df) {
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
                    if (diffIndices[i]->getType() == DiffIndex::TypeUpdate::ADDITION_df) {
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

        LIBEXP void releaseItr(PairItr *itr);

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
                const int64_t key,
                int64_t c1,
                int64_t c2,
                const bool constrain,
                const bool noAggr) {
            short fileIdx = value->getFileIdx(perm);
            int64_t mark = value->getMark(perm);
            char strategy = value->getStrategy(perm);
            return getIterator(perm, key, fileIdx, mark, strategy, c1, c2, constrain, noAggr);
        }

        PairItr *newItrOnReverse(PairItr *itr, const int64_t v1, const int64_t v2);

        PairItr *getFilePairIterator(const int perm, const int64_t constraint1,
                const int64_t constraint2,
                const char strategy, const short file,
                const int64_t pos) {
            PairItr *t = strat.getBinaryTable(strategy);
            initNewIterator(files[perm], file, pos, (PairItr*) t, constraint1, constraint2, true);
            return t;
        }

        DictMgmt *getDictMgmt() {
            return dict;
        }

        Querier &getSampler() {
            if (sampler == NULL) {
                LOG(ERRORL) << "No sampler available";
                throw 10;
            } else {
                return *sampler;
            }
        }

        std::string getPathRawData() {
            return pathRawData;
        }

        /*//The second row is ignored! This is useful for unlabelled graphs
          int64_t getValue1AtRow(const int idx,
          const short fileIdx,
          const int mark,
          const char strategy,
          const int64_t rowId);*/

        const char *getTable(const int perm,
                const short fileId,
                const int64_t markId);

};

#endif
