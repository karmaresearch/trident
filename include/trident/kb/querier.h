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
#include <trident/iterators/filtersameitr.h>

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

class KeyCardItr {
    protected:
        uint64_t currentKey1;
        uint64_t currentKey2;
        uint64_t currentCard;

    public:
        KeyCardItr() {
        }

        uint64_t getKey1() {
            return currentKey1;
        }

        uint64_t getKey2() {
            return currentKey2;
        }

        uint64_t getCard() {
            return currentCard;
        }

        virtual bool hasNext() = 0;

        virtual void next() = 0;

        virtual ~KeyCardItr() {};
};

class ConstKeyCardItr : public KeyCardItr {
    private:
        bool hn;

    public:
        ConstKeyCardItr(Querier *q, uint64_t s, uint64_t r, uint64_t d, uint64_t key);

        ConstKeyCardItr(Querier *q, uint64_t s, uint64_t r, uint64_t d, uint64_t key1, uint64_t key2);

        bool hasNext() {
            return hn;
        }

        void next() {
            hn = false;
        }

        ~ConstKeyCardItr() {
        }
};

class EdgeItr {
    private:
        int *order;
        PairItr *itr;

    public:
        EdgeItr(int *order, PairItr *itr) : order(order), itr(itr) {
        }

        bool hasNext() {
            return itr->hasNext();
        }

        void next() {
            itr->next();
        }

        uint64_t getValue(int i) {
            switch(i) {
                case 0:
                    return itr->getKey();
                case 1:
                    return itr->getValue1();
                case 2:
                    return itr->getValue2();
                default:
                    throw 10;
            }
        }

        void getTriple(uint64_t *v) {
            v[0] = getSubject();
            v[1] = getPredicate();
            v[2] = getObject();
        }

        uint64_t getSubject() {
            return getValue(order[0]);
        }

        uint64_t getPredicate() {
            return getValue(order[1]);
        }

        uint64_t getObject() {
            return getValue(order[2]);
        }
};

class OneKeyCardItr: public KeyCardItr {
    private:
        EdgeItr *itr;
        bool hn;
        bool hnDone;
        bool nextDone;
        uint64_t nextCard;
        uint64_t nextKey1;

    public:
        OneKeyCardItr(EdgeItr *itr) : itr(itr), hnDone(false), nextDone(false) {
        }

        bool hasNext() {
            if (! hnDone) {
                hnDone = true;
                if (nextDone) {
                    hn = true;
                } else if (itr->hasNext()) {
                    itr->next();
                    hn = true;
                } else {
                    hn = false;
                }
                if (hn) {
                    nextDone = false;
                    nextCard = 1;
                    nextKey1 = itr->getValue(0);
                    while (itr->hasNext()) {
                        itr->next();
                        if (itr->getValue(0) != nextKey1) {
                            nextDone = true;
                            break;
                        }
                        nextCard++;
                    }
                }
            }
            return hn;
        }

        void next() {
            if (! hn || ! hnDone) {
                throw 10;
            }
            hnDone = false;
            currentCard = nextCard;
            currentKey1 = nextKey1;
        }

        ~OneKeyCardItr() {
            delete itr;
        }
};

class TwoKeyCardItr: public KeyCardItr {
    private:
        EdgeItr *itr;
        bool hn;
        bool hnDone;
        bool nextDone;
        uint64_t nextCard;
        uint64_t nextKey1;
        uint64_t nextKey2;

    public:
        TwoKeyCardItr(EdgeItr *itr) : itr(itr), hnDone(false), nextDone(false) {
        }

        bool hasNext() {
            if (! hnDone) {
                hnDone = true;
                if (nextDone) {
                    hn = true;
                } else if (itr->hasNext()) {
                    itr->next();
                    hn = true;
                } else {
                    hn = false;
                }
                if (hn) {
                    nextDone = false;
                    nextCard = 1;
                    nextKey1 = itr->getValue(0);
                    nextKey2 = itr->getValue(1);
                    while (itr->hasNext()) {
                        itr->next();
                        if (itr->getValue(0) != nextKey1 || itr->getValue(1) != nextKey2) {
                            nextDone = true;
                            break;
                        }
                        nextCard++;
                    }
                }
            }
            return hn;
        }

        void next() {
            if (! hn || ! hnDone) {
                throw 10;
            }
            hnDone = false;
            currentCard = nextCard;
            currentKey1 = nextKey1;
            currentKey2 = nextKey2;
        }

        ~TwoKeyCardItr() {
            delete itr;
        }
};

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
        Factory<FilterSameItr> factory16;

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

        bool sameVar(const int64_t s, const int64_t r, const int64_t d, int &p1, int &p2) {
            if (s < 0) {
                if (r == s) {
                    p1 = 0;
                    p2 = 1;
                    return true;
                }
                if (d == s) {
                    p1 = 0;
                    p2 = 2;
                    return true;
                }
            }
            if (r < 0) {
                if (d == r) {
                    p1 = 1;
                    p2 = 2;
                    return true;
                }
            }
            return false;
        }

        uint64_t countKeyCardItr(KeyCardItr *it) {
            uint64_t card = 0;
            while (it->hasNext()) {
                it->next();
                card++;
            }
            return card;
        }
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

        DDLEXPORT PairItr *get(const int idx, const int64_t s, const int64_t p, const int64_t o) {
            return getIterator(idx, s, p, o);
        }

        DDLEXPORT EdgeItr *getEdgeItr(const int idx, const int64_t s, const int64_t r, const int64_t d) {
            int p1, p2;
            PairItr *itr;
            if (sameVar(s, r, d, p1, p2)) {
                PairItr *it = getIterator(idx, s < 0 ? -1 : s, r < 0 ? -1 : r, d < 0 ? -1 : d);
                FilterSameItr *fi = factory16.get();
                fi->init(it, p1, p2);
                itr = fi;
            } else {
                itr = getIterator(idx, s < 0 ? -1 : s, r < 0 ? -1 : r, d < 0 ? -1 : d);
            }
            return new EdgeItr(getInvOrder(idx), itr);
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

        // Primitives

        // Gets node label, returns true when found
        DDLEXPORT  bool lbl_n(nTerm key, char *value, int &len) {
            return getDictMgmt()->getText(key, value, len);
        }

        // Gets edge label, returns true when found
        DDLEXPORT  bool lbl_e(nTerm key, char *value, int &len) {
            return getDictMgmt()->getText(key, value, len);
        }

        // Gets node id, returns true when found
        DDLEXPORT  bool nodid(char *key, int len, nTerm *value) {
            return getDictMgmt()->getNumber(key, len, value);
        }

        // Gets edge id, returns true when found
        DDLEXPORT  bool edgid(char *key, int len, nTerm *value) {
            return getDictMgmt()->getNumber(key, len, value);
        }

        DDLEXPORT  EdgeItr *edg_srd(const int64_t s, const int64_t r, const int64_t d) {
            if (d >= 0) {
                if (r >= 0) {
                    return getEdgeItr(IDX_OPS, s, r, d);
                }
                return getEdgeItr(IDX_OSP, s, r, d);
            }
            if (r >= 0) {
                return getEdgeItr(IDX_PSO, s, r, d);
            }
            return getEdgeItr(IDX_SPO, s, r, d);
        }

        DDLEXPORT  EdgeItr *edg_sdr(const int64_t s, const int64_t r, const int64_t d) {
            if (r >= 0) {
                if (d >= 0) {
                    return getEdgeItr(IDX_POS, s, r, d);
                }
                return getEdgeItr(IDX_PSO, s, r, d);
            }
            if (d >= 0) {
                return getEdgeItr(IDX_OSP, s, r, d);
            }
            return getEdgeItr(IDX_SOP, s, r, d);
        }

        DDLEXPORT  EdgeItr *edg_drs(const int64_t s, const int64_t r, const int64_t d) {
            if (s >= 0) {
                if (r >= 0) {
                    return getEdgeItr(IDX_SPO, s, r, d);
                }
                return getEdgeItr(IDX_SOP, s, r, d);
            }
            if (r >= 0) {
                return getEdgeItr(IDX_POS, s, r, d);
            }
            return getEdgeItr(IDX_OPS, s, r, d);
        }

        DDLEXPORT  EdgeItr *edg_dsr(const int64_t s, const int64_t r, const int64_t d) {
            if (r >= 0) {
                if (s >= 0) {
                    return getEdgeItr(IDX_PSO, s, r, d);
                }
                return getEdgeItr(IDX_POS, s, r, d);
            }
            if (s >= 0) {
                return getEdgeItr(IDX_SOP, s, r, d);
            }
            return getEdgeItr(IDX_OSP, s, r, d);
        }

        DDLEXPORT  EdgeItr *edg_rsd(const int64_t s, const int64_t r, const int64_t d) {
            if (d >= 0) {
                if (s >= 0) {
                    return getEdgeItr(IDX_OSP, s, r, d);
                }
                return getEdgeItr(IDX_OPS, s, r, d);
            }
            if (s >= 0) {
                return getEdgeItr(IDX_SPO, s, r, d);
            }
            return getEdgeItr(IDX_PSO, s, r, d);
        }

        DDLEXPORT  EdgeItr *edg_rds(const int64_t s, const int64_t r, const int64_t d) {
            if (s >= 0) {
                if (d >= 0) {
                    return getEdgeItr(IDX_SOP, s, r, d);
                }
                return getEdgeItr(IDX_SPO, s, r, d);
            }
            if (d >= 0) {
                return getEdgeItr(IDX_OPS, s, r, d);
            }
            return getEdgeItr(IDX_POS, s, r, d);
        }

        DDLEXPORT uint64_t countEdges(const int64_t s, const int64_t r, const int64_t d) {
            int p1, p2;
            if (sameVar(s, r, d, p1, p2)) {
                uint64_t cnt = 0;
                PairItr *it = getIterator(IDX_SPO, s, r, d);
                FilterSameItr *fi = factory16.get();
                fi->init(it, p1, p2);
                while (fi->hasNext()) {
                    fi->next();
                    cnt++;
                }
                releaseItr(fi);
                return cnt;
            } else {
                return getCard(s, r, d);
            }
        }

        DDLEXPORT KeyCardItr *grp_s(const int64_t s, const int64_t r, const int64_t d) {
            if (s >= 0) {
                return new ConstKeyCardItr(this, s, r, d, s);
            }
            EdgeItr *it = d >= 0 ? edg_sdr(s, r, d) : edg_srd(s, r, d);
            return new OneKeyCardItr(it);
        }

        DDLEXPORT KeyCardItr *grp_r(const int64_t s, const int64_t r, const int64_t d) {
            if (r >= 0) {
                return new ConstKeyCardItr(this, s, r, d, r);
            }
            EdgeItr *it = d >= 0 ? edg_rds(s, r, d) : edg_rsd(s, r, d);
            return new OneKeyCardItr(it);
        }

        DDLEXPORT KeyCardItr *grp_d(const int64_t s, const int64_t r, const int64_t d) {
            if (d >= 0) {
                return new ConstKeyCardItr(this, s, r, d, d);
            }
            EdgeItr *it = r >= 0 ? edg_drs(s, r, d) : edg_dsr(s, r, d);
            return new OneKeyCardItr(it);
        }

        DDLEXPORT KeyCardItr *grp_sr(const int64_t s, const int64_t r, const int64_t d) {
            if (s >= 0 && r >= 0) {
                return new ConstKeyCardItr(this, s, r, d, s, r);
            }
            EdgeItr *it = edg_srd(s, r, d);
            return new TwoKeyCardItr(it);
        }

        DDLEXPORT KeyCardItr *grp_sd(const int64_t s, const int64_t r, const int64_t d) {
            if (s >= 0 && d >= 0) {
                return new ConstKeyCardItr(this, s, r, d, s, d);
            }
            EdgeItr *it = edg_sdr(s, r, d);
            return new TwoKeyCardItr(it);
        }

        DDLEXPORT KeyCardItr *grp_rs(const int64_t s, const int64_t r, const int64_t d) {
            if (r >= 0 && s >= 0) {
                return new ConstKeyCardItr(this, s, r, d, r, s);
            }
            EdgeItr *it = edg_rsd(s, r, d);
            return new TwoKeyCardItr(it);
        }

        DDLEXPORT KeyCardItr *grp_rd(const int64_t s, const int64_t r, const int64_t d) {
            if (r >= 0 && d >= 0) {
                return new ConstKeyCardItr(this, s, r, d, r, d);
            }
            EdgeItr *it = edg_rds(s, r, d);
            return new TwoKeyCardItr(it);
        }

        DDLEXPORT KeyCardItr *grp_ds(const int64_t s, const int64_t r, const int64_t d) {
            if (d >= 0 && s >= 0) {
                return new ConstKeyCardItr(this, s, r, d, d, s);
            }
            EdgeItr *it = edg_dsr(s, r, d);
            return new TwoKeyCardItr(it);
        }

        DDLEXPORT KeyCardItr *grp_dr(const int64_t s, const int64_t r, const int64_t d) {
            if (d >= 0 && r >= 0) {
                return new ConstKeyCardItr(this, s, r, d, d, r);
            }
            EdgeItr *it = edg_drs(s, r, d);
            return new TwoKeyCardItr(it);
        }

        DDLEXPORT uint64_t cnt_grp_s(const int64_t s, const int64_t r, const int64_t d) {
            if (s >= 0) {
                ConstKeyCardItr v(this, s, r, d, s);
                return v.hasNext() ? 1 : 0;
            }
            return getCard(s, r, d, 0);
        }

        DDLEXPORT uint64_t cnt_grp_r(const int64_t s, const int64_t r, const int64_t d) {
            if (r >= 0) {
                ConstKeyCardItr v(this, s, r, d, r);
                return v.hasNext() ? 1 : 0;
            }
            return getCard(s, r, d, 1);
        }

        DDLEXPORT uint64_t cnt_grp_d(const int64_t s, const int64_t r, const int64_t d) {
            if (d >= 0) {
                ConstKeyCardItr v(this, s, r, d, d);
                return v.hasNext() ? 1 : 0;
            }
            return getCard(s, r, d, 2);
        }

        DDLEXPORT uint64_t cnt_grp_sr(const int64_t s, const int64_t r, const int64_t d) {
            if (s >= 0 && r >= 0) {
                ConstKeyCardItr v(this, s, r, d, s, r);
                return v.hasNext() ? 1 : 0;
            }

            KeyCardItr *it = grp_sr(s, r, d);
            uint64_t card = countKeyCardItr(it);
            delete it;
            return card;
        }

        DDLEXPORT uint64_t cnt_grp_sd(const int64_t s, const int64_t r, const int64_t d) {
            if (s >= 0 && d >= 0) {
                ConstKeyCardItr v(this, s, r, d, s, d);
                return v.hasNext() ? 1 : 0;
            }
            KeyCardItr *it = grp_sd(s, r, d);
            uint64_t card = countKeyCardItr(it);
            delete it;
            return card;
        }

        DDLEXPORT uint64_t cnt_grp_rs(const int64_t s, const int64_t r, const int64_t d) {
            if (r >= 0 && s >= 0) {
                ConstKeyCardItr v(this, s, r, d, r, s);
                return v.hasNext() ? 1 : 0;
            }
            KeyCardItr *it = grp_rs(s, r, d);
            uint64_t card = countKeyCardItr(it);
            delete it;
            return card;
        }

        DDLEXPORT uint64_t cnt_grp_rd(const int64_t s, const int64_t r, const int64_t d) {
            if (r >= 0 && d >= 0) {
                ConstKeyCardItr v(this, s, r, d, r, d);
                return v.hasNext() ? 1 : 0;
            }
            KeyCardItr *it = grp_rd(s, r, d);
            uint64_t card = countKeyCardItr(it);
            delete it;
            return card;
        }

        DDLEXPORT uint64_t cnt_grp_ds(const int64_t s, const int64_t r, const int64_t d) {
            if (d >= 0 && s >= 0) {
                ConstKeyCardItr v(this, s, r, d, d, s);
                return v.hasNext() ? 1 : 0;
            }
            KeyCardItr *it = grp_ds(s, r, d);
            uint64_t card = countKeyCardItr(it);
            delete it;
            return card;
        }

        DDLEXPORT uint64_t cnt_grp_dr(const int64_t s, const int64_t r, const int64_t d) {
            if (d >= 0 && r >= 0) {
                ConstKeyCardItr v(this, s, r, d, d, r);
                return v.hasNext() ? 1 : 0;
            }
            KeyCardItr *it = grp_dr(s, r, d);
            uint64_t card = countKeyCardItr(it);
            delete it;
            return card;
        }

        DDLEXPORT void pos_srd(const int64_t s, const int64_t r, const int64_t d, int pos,  uint64_t *result) {
            // TODO: implement special cases
            EdgeItr *it = edg_srd(s, r, d);
            bool hasNext = it->hasNext();
            for (int i = 0; i < pos; i++) {
                if (! hasNext) {
                    throw 10;
                }
                it->next();
                hasNext = it->hasNext();
            }
            if (! hasNext) {
                throw 10;
            }
            it->getTriple(result);
            delete it;
        }

        DDLEXPORT void pos_sdr(const int64_t s, const int64_t r, const int64_t d, int pos,  uint64_t *result) {
            // TODO: implement special cases
            EdgeItr *it = edg_sdr(s, r, d);
            bool hasNext = it->hasNext();
            for (int i = 0; i < pos; i++) {
                if (! hasNext) {
                    throw 10;
                }
                it->next();
                hasNext = it->hasNext();
            }
            if (! hasNext) {
                throw 10;
            }
            it->getTriple(result);
            delete it;
        }

        DDLEXPORT void pos_rsd(const int64_t s, const int64_t r, const int64_t d, int pos,  uint64_t *result) {
            // TODO: implement special cases
            EdgeItr *it = edg_rsd(s, r, d);
            bool hasNext = it->hasNext();
            for (int i = 0; i < pos; i++) {
                if (! hasNext) {
                    throw 10;
                }
                it->next();
                hasNext = it->hasNext();
            }
            if (! hasNext) {
                throw 10;
            }
            it->getTriple(result);
            delete it;
        }

        DDLEXPORT void pos_rds(const int64_t s, const int64_t r, const int64_t d, int pos,  uint64_t *result) {
            // TODO: implement special cases
            EdgeItr *it = edg_rds(s, r, d);
            bool hasNext = it->hasNext();
            for (int i = 0; i < pos; i++) {
                if (! hasNext) {
                    throw 10;
                }
                it->next();
                hasNext = it->hasNext();
            }
            if (! hasNext) {
                throw 10;
            }
            it->getTriple(result);
            delete it;
        }

        DDLEXPORT void pos_dsr(const int64_t s, const int64_t r, const int64_t d, int pos,  uint64_t *result) {
            // TODO: implement special cases
            EdgeItr *it = edg_dsr(s, r, d);
            bool hasNext = it->hasNext();
            for (int i = 0; i < pos; i++) {
                if (! hasNext) {
                    throw 10;
                }
                it->next();
                hasNext = it->hasNext();
            }
            if (! hasNext) {
                throw 10;
            }
            it->getTriple(result);
            delete it;
        }

        DDLEXPORT void pos_drs(const int64_t s, const int64_t r, const int64_t d, int pos,  uint64_t *result) {
            // TODO: implement special cases
            EdgeItr *it = edg_drs(s, r, d);
            bool hasNext = it->hasNext();
            for (int i = 0; i < pos; i++) {
                if (! hasNext) {
                    throw 10;
                }
                it->next();
                hasNext = it->hasNext();
            }
            if (! hasNext) {
                throw 10;
            }
            it->getTriple(result);
            delete it;
        }
};

#endif
