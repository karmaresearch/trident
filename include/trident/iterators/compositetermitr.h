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


#ifndef _COMPOSITE_TERM_ITR_H
#define _COMPOSITE_TERM_ITR_H

#include <trident/iterators/pairitr.h>
#include <trident/iterators/difftermitr.h>
#include <trident/kb/consts.h>

class CompositeTermItr : public PairItr {
private:
    std::vector<PairItr*> children;
    std::vector<PairItr*> activechildren;
    long currentCount;

    static bool _sorter(PairItr *i1, PairItr *i2) {
        return i1->getKey() > i2->getKey();
    }
public:
    int getTypeItr() {
        return COMPOSITETERM_ITR;
    }

    long getValue1() {
        return 0;
    }

    long getValue2() {
        return 0;
    }

    void ignoreSecondColumn() {
        throw 10; //not supported
    }

    void clear() {
        children.clear();
        activechildren.clear();
    }

    void mark() {
        throw 10;
    }

    void reset(const char i) {
        throw 10;
    }

    void moveto(const long c1, const long c2) {
        throw 10;
    }

    long getCount() {
        return currentCount;
    }

    void add(PairItr *itr) {
        currentCount = 0;
        children.push_back(itr);
        if (itr->hasNext()) {
            itr->next();
            activechildren.push_back(itr);
        }
        if (activechildren.size() > 1) {
            sort(activechildren.begin(), activechildren.end(), _sorter);
        }
    }

    std::vector<PairItr*> getChildren() {
        return activechildren;
    }

    bool hasNext() {
        return !activechildren.empty();
    }

    uint64_t getCardinality() {
        uint64_t card = 0;
        for (size_t i = 0; i < activechildren.size(); ++i) {
            int typeItr = activechildren[i]->getTypeItr();
            if (typeItr == TERM_ITR) {
                card += activechildren[i]->getCardinality();
            } else if (typeItr == DIFFTERM_ITR) {
                card += ((DiffTermItr*)activechildren[i])->getNUniqueKeys();
            } else {
                throw 10;
            }
        }
        return card;
    }

    uint64_t estCardinality() {
        return getCardinality();
    }

    void next() {
        const long key = activechildren.back()->getKey();
        setKey(key);
        currentCount = 0;
        //Move also all other iterators with the same key
        for (int i = activechildren.size() - 1; i >= 0; i--) {
            if (activechildren[i]->getKey() == key) {
                currentCount += activechildren[i]->getCount();
                if (activechildren[i]->hasNext()) {
                    activechildren[i]->next();
                } else {
                    activechildren.erase(activechildren.begin() + i);
                }
            }
        }

        if (children.size() > 1) {
            sort(activechildren.begin(), activechildren.end(), _sorter);
        }
    }

    void init() {
	initializeConstraints();
        children.clear();
        activechildren.clear();
    }
};

#endif
