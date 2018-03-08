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


#ifndef _RMITR_H
#define _RMITR_H

#include <trident/kb/consts.h>
#include <trident/iterators/pairitr.h>

class RmItr : public PairItr {
private:
    PairItr *itr;
    PairItr *rmitr;
    int64_t delnfirstterms;
    bool rmitrvalid;
    bool noseccol;
    int64_t v1, v2;
    int64_t curCount, nextCount;
    bool hnc, hn;

    bool savedrmitrvalid;
    int64_t savedv1, savedv2, savedcurCount, savednextCount;


    int cmp(PairItr *itr1, PairItr *itr2);

public:
    int getTypeItr() {
        return RM_ITR;
    }

    int64_t getValue1() {
        return v1;
    }

    int64_t getValue2() {
        return v2;
    }

    void clear() {
    }

    void mark() {
        savedrmitrvalid = rmitrvalid;
	savedv1 = v1;
	savedv2 = v2;
	savedcurCount = curCount;
	savednextCount = nextCount;
	itr->mark();
	rmitr->mark();
    }

    void reset(const char i) {
        rmitrvalid = savedrmitrvalid;
	hnc = false;
	if (i == 0) {
	    v1 = savedv1;
	} else {
	    v2 = savedv2;
	}
	curCount = savedcurCount;
	nextCount = savednextCount;
	itr->reset(i);
	rmitr->reset(i);
    }

    PairItr *getMainItr() {
        return itr;
    }

    PairItr *getRmItr() {
        return rmitr;
    }

    void moveto(const int64_t c1, const int64_t c2);

    void init(PairItr *itr, PairItr *rmitr, int64_t nfirstterms);

    bool hasNext();

    void next();

    int64_t getCount();

    uint64_t getCardinality();

    uint64_t estCardinality();

    void ignoreSecondColumn();
};

#endif
