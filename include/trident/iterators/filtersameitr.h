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


#ifndef _FILTERSAME_ITR_H
#define _FILTERSAME_ITR_H

#include <trident/iterators/pairitr.h>
#include <trident/kb/consts.h>

#include <kognac/logs.h>

#include <vector>

// This is a PairItr with an underlying PairItr, only selecting values when the value at pos1 is equal
// to the value at pos2.
class FilterSameItr: public PairItr {
    private:
        PairItr *underlying;
        int pos1, pos2;

        bool hn, hnc;
        bool savedhn, savedhnc;

    public:

        void init(PairItr *underlying, int pos1, int pos2) {
            this->underlying = underlying;
            this->pos1 = pos1;
            this->pos2 = pos2;
        }

        int getTypeItr() {
            return FILTERSAME_ITR;
        }

        int64_t getValue1() {
            return underlying->getValue1();
        }

        int64_t getValue2() {
            return underlying->getValue2();
        }

        int64_t getKey() {
            return underlying->getKey();
        }

        void setKey(int64_t key) {
            underlying->setKey(key);
        }

        bool hasNext() {
            if (! hnc) {
                hnc = true;
                hn = false;
                while (underlying->hasNext()) {
                    underlying->next();
                    if (pos1 == 0) {
                        if (pos2 == 1) {
                            if (underlying->getKey() == underlying->getValue1()) {
                                hn = true;
                                break;
                            }
                        } else {
                            if (underlying->getKey() == underlying->getValue2()) {
                                hn = true;
                                break;
                            }
                        }
                    }
                    else {
                        assert(pos1 == 1 && pos2 == 2);
                        if (underlying->getValue1() == underlying->getValue2()) {
                            hn = true;
                            break;
                        }
                    }
                }
            }
            return hn;
        }

        void next() {
            hnc = false;
        }

        void ignoreSecondColumn() {
            underlying->ignoreSecondColumn();
        }

        int64_t getCount() {
            // TODO
            throw 10;
        }

        uint64_t getCardinality() {
            // TODO
            throw 10;
        }

        uint64_t estCardinality() {
            return getCardinality();
        }

        void clear() {
            underlying->clear();
        }

        void mark() {
            savedhn = hn;
            savedhnc = hnc;
            underlying->mark();
        }

        void reset(const char r) {
            hn = savedhn;
            hnc = savedhnc;
            underlying->reset(r);
        }

        void moveto(const int64_t c1, const int64_t c2) {
            underlying->moveto(c1, c2);
            hnc = false;
        }

        void gotoKey(int64_t k) {
            hnc = false;
            underlying->gotoKey(k);
        }

        PairItr *getUnderlyingItr() {
            return underlying;
        }

};

#endif
