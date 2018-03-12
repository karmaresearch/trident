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


#ifndef _CACHEITR
#define _CACHEITR

#include <trident/iterators/cacheitr.h>
#include <trident/iterators/pairitr.h>
#include <trident/kb/consts.h>

#include <trident/kb/cacheidx.h>

class CacheIdx;
class Querier;

class CacheItr : public PairItr {
private:
    Querier *q;
    uint64_t estimatedSize;
    CacheIdx *cache;

    std::vector<std::pair<uint64_t, uint64_t>> newPairs;
    std::vector<CacheBlock> newBlocks;

    std::vector<std::pair<uint64_t, uint64_t>> *p;
    uint64_t idxEndGroup;

    std::vector<std::pair<uint64_t, uint64_t>> *existingPairs;
    std::vector<CacheBlock> *existingBlocks;

    uint64_t currentIdx, v1, v2;
    int64_t lastDeltaValue, startDelta;
    bool groupSet;

    bool hasNextChecked, n;

    CacheBlock *searchBlock(std::vector<CacheBlock> *blocks,
                            const uint64_t v);
public:
    int getTypeItr()  {
        return CACHE_ITR;
    }

    void init(Querier *q, const uint64_t estimatedSize, CacheIdx *cache, int64_t c1,
              int64_t c2);

    int64_t getValue1();

    int64_t getValue2();

    bool hasNext();

    void next();

    void clear();

    bool gotoFirstTerm(int64_t c1);

    void gotoSecondTerm(int64_t c2);

    void mark();

    uint64_t getCardinality();

    //uint64_t estimateCardinality();

    void reset(const char i);

    bool allowMerge() {
        return false;
    }

    void ignoreSecondColumn() {
        throw 10; //not supported
    }
};

#endif
