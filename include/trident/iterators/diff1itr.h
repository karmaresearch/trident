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


#ifndef _DIFF1ITR_H
#define _DIFF1ITR_H

#include <trident/iterators/pairitr.h>
#include <trident/kb/consts.h>

class Diff1Itr : public PairItr {
private:
    int64_t v1, v2;
    bool noseccol;
    bool first;

    const char *s;
    const char *e;
    uint8_t posarray;
    uint8_t nbytes;
    int64_t nelements;

public:
    int getTypeItr() {
        return DIFF1_ITR;
    }

    int64_t getValue1() {
        return v1;
    }

    int64_t getValue2() {
        return v2;
    }

    void mark() {
        throw 10;
    }

    void reset(const char i) {
        throw 10;
    }

    void ignoreSecondColumn() {
        noseccol = true;
        if (!first && posarray == 2)
            s = e;
    }

    void clear() {
    }

    void next() {
        first = false;
        if (posarray == 0) {
            if (nbytes == 4) {
                setKey(Utils::decode_int(s));
                s += 4;
            } else {
                setKey(Utils::decode_long(s));
                s += 8;
            }
        } else if (posarray == 1) {
            if (nbytes == 4) {
                v1 = Utils::decode_int(s);
                s += 4;
            } else {
                v1 = Utils::decode_long(s);
                s += 8;
            }
        } else {
            if (noseccol) {
                s = e;
            } else {
                if (nbytes == 4) {
                    v2 = Utils::decode_int(s);
                    s += 4;
                } else {
                    v2 = Utils::decode_long(s);
                    s += 8;
                }
            }
        }
    }

    int64_t getCount() {
        switch (posarray) {
        case 0:
            return 1;
        case 1:
            return 1;
        case 2:
            return nelements;
        }
        throw 10;
    }

    uint64_t getCardinality() {
        if (noseccol) {
            if (posarray == 2) {
                return 1;
            } else {
                return nelements;
            }
        } else {
            return nelements;
        }
    }

    uint64_t estCardinality() {
        return getCardinality();
    }

    void moveto(const int64_t c1, const int64_t c2) {
        //TODO
        throw 10;
    }

    bool hasNext() {
        return s != e;
    }

    void init(int64_t key, int64_t first, int64_t second, const char *values,
              uint8_t nbytes, int64_t nelements, uint8_t posarray) {
	initializeConstraints();
        noseccol = false;
        setKey(key);
        v1 = first;
        v2 = second;
        s = values;
        e = values + nbytes * nelements;
        this->posarray = posarray;
        this->nelements = nelements;
        this->nbytes = nbytes;
        first = true;
    }
};

#endif
