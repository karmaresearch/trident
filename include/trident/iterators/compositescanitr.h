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


#ifndef _COMPOSITESCAN_ITR_H
#define _COMPOSITESCAN_ITR_H

#include <trident/iterators/pairitr.h>
#include <trident/kb/consts.h>

#include <vector>

class Querier;
class CompositeScanItr : public PairItr {
private:
    bool hn, hnc;
    Querier *q;
    long v1, v2;
    bool ignseccol;
    long count;
    int perm;

    std::vector<PairItr *> children;
    std::vector<PairItr *> allChildren;

    static bool _sorter_noign(PairItr *i1, PairItr *i2) {
        if (i1->getKey() > i2->getKey()) {
            return true;
        } else  if (i1->getKey() == i2->getKey()) {
            if (i1->getValue1() > i2->getValue1()) {
                return true;
            } else if (i1->getValue1() == i2->getValue1()) {
                return i1->getValue2() > i2->getValue2();
            }
        }
        return false;
    }

    static bool _sorter_ign(PairItr *i1, PairItr *i2) {
        if (i1->getKey() > i2->getKey()) {
            return true;
        } else if (i1->getKey() == i2->getKey()) {
            return (i1->getValue1() > i2->getValue1());
        }
        return false;
    }

public:
    int getTypeItr() {
        return COMPOSITESCAN_ITR;
    }

    long getValue1() {
        return v1;
    }

    long getValue2() {
        return v2;
    }

    void mark() {
        throw 10;
    }

    void reset(const char i) {
        throw 10;
    }

    long getCount();

    void ignoreSecondColumn();

    void clear();

    void moveto(const long c1, const long c2);

    void gotoKey(long keyToSearch);

    bool hasNext();

    void next();

    void setQuerier(Querier *q);

    uint64_t getCardinality();

    uint64_t estCardinality();

    void addChild(PairItr *itr);

    void init(int perm);
};

#endif
