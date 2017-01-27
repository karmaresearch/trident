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


#ifndef POSITR_H_
#define POSITR_H_

#include <trident/kb/consts.h>
#include <trident/iterators/pairitr.h>

#include <iostream>

class Querier;

class AggrItr: public PairItr {
    private:
        Querier *q;
        PairItr *mainItr;
        PairItr *secondItr;
        bool noSecColumn;
        long value1, value2;

        bool hasNextChecked, n;
        int idx;

        //long p;

        char strategy(long coordinates) {
            return (char) ((coordinates >> 48) & 0xFF);
        }

        short file(long coordinates) {
            return (short)((coordinates >> 32) & 0xFFFF);
        }

        int pos(long coordinates) {
            return (int) coordinates;
        }

        void setup_second_itr(const int idx);

    public:
        int getTypeItr() {
            return AGGR_ITR;
        }

        long getValue1() {
            return value1;
        }

        long getValue2() {
            return value2;
        }

        bool hasNext();

        void next();

        void mark();

        void reset(const char i);

        void clear();

        void moveto(const long c1, const long c2);

        void init(int idx, PairItr* itr, Querier *q);

        uint64_t getCardinality();

        uint64_t estCardinality();

        long getCount();

        void setConstraint1(const long c1) {
            PairItr::setConstraint1(c1);
            mainItr->setConstraint1(c1);
        }

        void setConstraint2(const long c2) {
            PairItr::setConstraint2(c2);
            if (secondItr)
                secondItr->setConstraint2(c2);
        }

        PairItr *getMainItr() {
            return mainItr;
        }

        PairItr *getSecondItr() {
            return secondItr;
        }

        void ignoreSecondColumn() {
            noSecColumn = true;
        }
};

#endif
