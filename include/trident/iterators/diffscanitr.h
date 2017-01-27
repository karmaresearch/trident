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


#ifndef _DIFF_SCANITR_H
#define _DIFF_SCANITR_H

#include <trident/iterators/pairitr.h>
#include <trident/kb/consts.h>
#include <trident/tree/treeitr.h>

#include <memory>

class DiffIndex;
class Querier;
class DiffScanItr : public PairItr {
private:
    int perm;
    long currentkey, v1, v2;
    bool hn, hnc;
    bool ignseccol;

    std::unique_ptr<TreeItr> root;
    DiffIndex *diff;
    Querier *q;

    PairItr *currentItr;

public:
    int getTypeItr() {
        return DIFFSCAN_ITR;
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

    void ignoreSecondColumn() {
        ignseccol = true;
        if (currentItr) {
            currentItr->ignoreSecondColumn();
        }
    }

    long getCount() {
        if (ignseccol) {
            return currentItr->getCount();
        } else {
            return 1;
        }
    }

    void clear();

    void moveto(const long c1, const long c2);

    void gotoKey(long keyToSearch);

    bool hasNext();

    void next();

    void init(TreeItr *root, int perm, DiffIndex *diff);

    void setQuerier(Querier *q);

    uint64_t getCardinality();

    uint64_t estCardinality();
};

#endif
