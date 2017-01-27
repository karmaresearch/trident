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


#ifndef _COMPOSITE_ITR_H
#define _COMPOSITE_ITR_H

#include <trident/iterators/pairitr.h>
#include <trident/kb/consts.h>

#include <vector>

class DiffIndex;
class CompositeItr: public PairItr {
private:
    long v1, v2;
    bool ignseccol;

    std::vector<PairItr*> children;
    int lastIdx;
    //std::vector<PairItr*> allchildren;

    bool hn, hnc;
    bool isSorted,decreasedKey;
    DiffIndex *currentdiff;
    long currentCount;
    long nfirstterms;
    PairItr *kbitr;

    void rearrangeChildren();

public:
    int getTypeItr() {
        return COMPOSITE_ITR;
    }

    long getValue1() {
        return v1;
    }

    long getValue2() {
        return v2;
    }

    bool hasNext();

    void next();

    void ignoreSecondColumn();

    long getCount();

    DiffIndex *getCurrentDiff();

    uint64_t getCardinality();

    uint64_t estCardinality();

    void clear() {
        children.clear();
        //allchildren.clear();
    }

    void mark() {
        throw 10;
    }

    void reset(const char i) {
        throw 10;
    }

    void moveto(const long c1, const long c2);

    void init(std::vector<PairItr*> &iterators, long nfirstterms,
              PairItr *kbitr);

    std::vector<PairItr*> getChildren();
};

#endif
