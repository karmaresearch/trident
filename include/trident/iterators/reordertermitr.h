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


#ifndef REORDERTERMITR_H_
#define REORDERTERMITR_H_

#include <trident/iterators/pairitr.h>
#include <trident/kb/consts.h>
#include <trident/kb/querier.h>

#include <inttypes.h>
#include <vector>

#include <google/dense_hash_set>

class ReOrderTermItr: public PairItr {
private:

    Querier *q;
    PairItr *helper;
    std::vector<uint64_t> keys;
    size_t nextKeyIndex;
    int idx;
    bool initialized;
    size_t m_nextKeyIndex;
    size_t m_key;

    void fillValuesO(google::dense_hash_set<uint64_t> &keys);

    void fillValuesP(google::dense_hash_set<uint64_t> &keys);

    void fillValues();

public:
    ReOrderTermItr(PairItr *helper, int idx, Querier *q)
        : q(q), helper(helper), nextKeyIndex(0), idx(idx), initialized(false) {
    }

    LIBEXP int getTypeItr() {
        return REORDERTERM_ITR;
    }

    LIBEXP int64_t getValue1() {
        return 0;
    }

    LIBEXP int64_t getValue2() {
        return 0;
    }

	LIBEXP int64_t getCount() {
        return 1;
    }

	LIBEXP uint64_t getCardinality();

	LIBEXP uint64_t estCardinality() {
        return getCardinality();
    }

	LIBEXP void ignoreSecondColumn() {
        // nothing
    }

	LIBEXP bool hasNext();

	LIBEXP void next();

	LIBEXP void mark() {
        m_key = key;
        m_nextKeyIndex = nextKeyIndex;
    }

	LIBEXP void reset(const char i) {
        key = m_key;
        nextKeyIndex = m_nextKeyIndex;
    }

	LIBEXP void clear() {
        if (helper != NULL) {
            q->releaseItr(helper);
            helper = NULL;
        }
    }

	LIBEXP void moveto(const int64_t c1, const int64_t c2) {
        // not supported
        throw 10;
    }

    ~ReOrderTermItr() {
        clear();
    }

};

#endif /* REORDERTERMITR_H_ */
