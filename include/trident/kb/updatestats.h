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


#ifndef _UPDATE_STATS_H
#define _UPDATE_STATS_H

class Querier;
class UpdateStats {
public:
    struct KeyInfo {
        uint64_t key;
        char strat1, strat2;
        uint32_t pos1, pos2;
        uint64_t nelements;
        uint64_t nfirsts1, nfirsts2;
        KeyInfo() : nfirsts1(0), nfirsts2(0) {}
    };

protected:
    const int perm1;
    const int perm2;
    const bool storep1;
    const bool storep2;

    bool shouldCheck1, shouldCheck2;
    int64_t c1;
    int64_t c2;
    int64_t totalc1;
    int64_t totalc2;
    std::vector<uint64_t> invertedpairs1;
    std::vector<uint64_t> invertedpairs2;
    std::vector<KeyInfo> keys;

    int64_t nkeys;
    int64_t totalkeys;
    uint64_t currentkey;
    PairItr *existingkeys;

    PairItr *itr1;
    PairItr *itr2;
    Querier *q;
public:
    UpdateStats(Querier *q, int perm1,
                int perm2, bool storep1, bool storep2);

    void addCoordForKey(int64_t key, uint32_t pos1, uint32_t pos2,
                        uint64_t nelements, char strat1, char strat2,
                        uint64_t nfirsts1, uint64_t nfirsts2) {
        KeyInfo k;
        k.key = key;
        k.pos1 = pos1;
        k.pos2 = pos2;
        k.nelements = nelements;
        k.strat1 = strat1;
        k.strat2 = strat2;
        k.nfirsts1 = nfirsts1;
        k.nfirsts2 = nfirsts2;
        keys.push_back(k);
    }

    ~UpdateStats();

    int64_t getCount1() {
        return c1;
    }

    int64_t getCount2() {
        return c2;
    }

    int64_t getTotalCount1() {
        return totalc1;
    }

    int64_t getTotalCount2() {
        return totalc2;
    }

    int64_t getNewKeys() {
        return nkeys;
    }

    int64_t getTotalKeys() {
        return totalkeys;
    }

    std::vector<KeyInfo> &getAllKeys() {
        return keys;
    }

    std::vector<uint64_t> &getInvertedPairs1() {
        return invertedpairs1;
    }

    std::vector<uint64_t> &getInvertedPairs2() {
        return invertedpairs2;
    }

    virtual void setKey(const int64_t key, const size_t sizetable) = 0;

    virtual void addFirst1(const int64_t v, const int64_t count) = 0;

    virtual void addFirst2(const int64_t v, const int64_t count) = 0;
};

class UpdateStats_add : public UpdateStats {
public:
    UpdateStats_add(Querier *q, int perm1,
                    int perm2, bool storep1, bool storep2) :
        UpdateStats(q, perm1, perm2, storep1, storep2) {
    }

    void setKey(const int64_t key, const size_t sizetable);

    void addFirst1(const int64_t v, const int64_t count);

    void addFirst2(const int64_t v, const int64_t count);
};

class UpdateStats_rm : public UpdateStats {
public:
    UpdateStats_rm(Querier *q, int perm1,
                    int perm2, bool storep1, bool storep2) :
        UpdateStats(q, perm1, perm2, storep1, storep2) {
    }

    void setKey(const int64_t key, const size_t sizetable);

    void addFirst1(const int64_t v, const int64_t count);

    void addFirst2(const int64_t v, const int64_t count);
};



#endif
