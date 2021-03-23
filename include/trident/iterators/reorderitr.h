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


#ifndef REORDERITR_H_
#define REORDERITR_H_

#include <trident/iterators/pairitr.h>
#include <trident/iterators/arrayitr.h>
#include <trident/kb/consts.h>
#include <trident/kb/querier.h>

#include <inttypes.h>
#include <vector>

#include <google/dense_hash_map>

typedef std::pair<uint64_t, std::shared_ptr<Pairs>> KeyValue;

class ReOrderItr: public PairItr {
private:

    Querier *q;
    PairItr *helper;
    std::vector<KeyValue> array;
    int idx;
    size_t nextKeyIndex;
    ArrayItr *itr;
    bool initialized;
    bool ignSecondColumn;
    std::vector<bool> sorted;
    int64_t p, o;
    ArrayItr *m_itr;
    uint64_t m_key;
    size_t m_nextKeyIndex;

    void fillValue(google::dense_hash_map<uint64_t, std::shared_ptr<Pairs>> &keyToPair, uint64_t key, uint64_t v1, uint64_t v2);

    void fillValuesOSP(google::dense_hash_map<uint64_t, std::shared_ptr<Pairs>> &keyToPair);

    void fillValuesOPS(google::dense_hash_map<uint64_t, std::shared_ptr<Pairs>> &keyToPair);

    void fillValuesPSO(google::dense_hash_map<uint64_t, std::shared_ptr<Pairs>> &keyToPair);

    void fillValuesPOS(google::dense_hash_map<uint64_t, std::shared_ptr<Pairs>> &keyToPair);

    void fillValues();

public:
    ReOrderItr(PairItr *helper, int idx, Querier *q, int64_t p, int64_t o)
        : q(q), helper(helper), idx(idx), nextKeyIndex(0), itr(NULL), initialized(false),
          ignSecondColumn(false), p(p), o(o), m_itr(NULL) {
        initializeConstraints();
    }

    LIBEXP int getTypeItr() {
        return REORDER_ITR;
    }

    LIBEXP int64_t getValue1() {
        return itr->getValue1();
    }

    LIBEXP int64_t getValue2() {
        return itr->getValue2();
    }

	LIBEXP int64_t getCount() {
        return itr->getCount();
    }

	LIBEXP uint64_t getCardinality();

	LIBEXP uint64_t estCardinality() {
        return getCardinality();
    }

	LIBEXP void ignoreSecondColumn() {
        ignSecondColumn = true;
        if (itr != NULL) {
            itr->ignoreSecondColumn();
        }
    }

	LIBEXP bool hasNext();

	LIBEXP void next();

	LIBEXP void mark() {
        m_itr = itr;
        m_key = key;
        m_nextKeyIndex = nextKeyIndex;
    }

	LIBEXP void reset(const char i) {
        if (itr != NULL && itr != m_itr) {
            itr->clear();
            delete itr;
        }
        itr = m_itr;
        key = m_key;
        nextKeyIndex = m_nextKeyIndex;
    }

	LIBEXP void clear() {
        if (helper != NULL) {
            q->releaseItr(helper);
            helper = NULL;
        }
        if (m_itr != NULL && m_itr != itr) {
            m_itr->clear();
            delete m_itr;
            m_itr = NULL;
        }
        if (itr != NULL) {
            itr->clear();
            delete itr;
            itr = NULL;
        }
    }

	LIBEXP void moveto(const int64_t c1, const int64_t c2) {
        if (itr == NULL || ! itr->hasNext()) {
            next();
        }
        itr->moveto(c1, c2);
    }

    ~ReOrderItr() {
        clear();
    }

};

#endif /* REORDERITR_H_ */
