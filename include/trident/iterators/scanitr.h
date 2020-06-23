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


#ifndef SCANITR_H_
#define SCANITR_H_

#include <trident/kb/consts.h>
#include <trident/iterators/pairitr.h>
#include <trident/tree/coordinates.h>
#include <trident/iterators/termitr.h>
#include <trident/binarytables/storagestrat.h>
#include <trident/binarytables/tableshandler.h>

class Querier;
class TreeItr;

class ScanItr: public PairItr {

private:
    Querier *q;
    int idx;
    bool ignseccolumn;

    PairItr *currentTable;
    PairItr *reversedItr;
    bool hnc, hn;
    TermItr *itr1;
    TermItr *itr2;

    bool m_hnc, m_hn;
    PairItr *m_currentTable;
    PairItr *m_reversedItr;
    TermItr *m_itr1;
    TermItr *m_itr2;

    //TableStorage *storage;
    //StorageStrat *strat;

public:
    void init(int idx, Querier *q);

    int getTypeItr() {
        return SCAN_ITR;
    }

    int64_t getValue1() {
        if (!reversedItr)
            return currentTable->getValue1();
        else
            return reversedItr->getValue1();
    }

    int64_t getValue2() {
        if (!reversedItr)
            return currentTable->getValue2();
        else
            return reversedItr->getValue2();
    }

    bool allowMerge() {
        return true;
    }

    void ignoreSecondColumn() {
        ignseccolumn = true;
        if (currentTable) {
            currentTable->ignoreSecondColumn();
        }
        if (reversedItr)
            reversedItr->ignoreSecondColumn();
    }

    int64_t getCount() {
        if (!reversedItr)
            return currentTable->getCount();
        else
            return reversedItr->getCount();
    }

    void mark();

    void reset(const char i);

    bool hasNext();

    void next();

    bool next(int64_t &v1, int64_t &v2, int64_t &v3);

    void clear();

    uint64_t getCardinality();

    uint64_t estCardinality();

    void gotoKey(int64_t k);

    void moveto(const int64_t c1, const int64_t c2);
};

#endif
