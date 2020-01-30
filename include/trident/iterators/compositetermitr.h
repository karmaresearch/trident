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
    bool hnc, nc;
    int64_t currentCount;

    static bool _sorter(PairItr *i1, PairItr *i2) {
        return i1->getKey() > i2->getKey();
    }
public:
    int getTypeItr() {
        return COMPOSITETERM_ITR;
    }

    int64_t getValue1() {
        return 0;
    }

    int64_t getValue2() {
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

    void moveto(const int64_t c1, const int64_t c2) {
        throw 10;
    }

    int64_t getCount() {
        if (hnc) {
            throw 10;
        }
        int64_t count = 0;
        for (int i = activechildren.size() - 1; i >= 0; i--) {
            if (activechildren[i]->getKey() == getKey()) {
                count += activechildren[i]->getCount();
            }
        }
        return count;
    }

    void add(PairItr *itr) {
        currentCount = 0;
        children.push_back(itr);
        if (itr->hasNext()) {
            itr->next();
            activechildren.push_back(itr);
            nc = true;
        }
        if (activechildren.size() > 1) {
            sort(activechildren.begin(), activechildren.end(), _sorter);
        }
    }

    std::vector<PairItr*> getChildren() {
        return activechildren;
    }

    bool hasNext() {
        if (! hnc) {
            hnc = true;
            nc = false;
            if (activechildren.empty()) {
                return nc;
            }
            const int64_t oldkey = activechildren.back()->getKey();
            for (int i = activechildren.size() - 1; i >= 0; i--) {
                if (activechildren[i]->getKey() == oldkey) {
                    if (activechildren[i]->hasNext()) {
                        activechildren[i]->next();
                    } else {
                        activechildren.erase(activechildren.begin() + i);
                    }
                }
            }
            if (! activechildren.empty()) {
                if (activechildren.size() > 1) {
                    sort(activechildren.begin(), activechildren.end(), _sorter);
                }
                nc = true;
            }
        }
        return nc;
    }

    uint64_t getCardinality() {
        if (hnc) {
            throw 10;
        }
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
        if (! hnc && ! hasNext()) {
            return;
        }
        setKey(activechildren.back()->getKey());
        hnc = false;
    }

    void init() {
        initializeConstraints();
        children.clear();
        activechildren.clear();
        nc = false;
        hnc = true; // because add() shifts added iterators.
    }
};

#endif
