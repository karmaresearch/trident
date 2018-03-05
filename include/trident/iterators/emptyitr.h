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


#ifndef _EMPTY_ITR_H
#define _EMPTY_ITR_H

#include <trident/iterators/pairitr.h>

class EmptyItr: public PairItr {
public:
    int getTypeItr() {
        return EMPTY_ITR;
    }

    int64_t getValue1() {
        return 0;
    }

    int64_t getValue2() {
        return 0;
    }

    int64_t getCount() {
        return 0;
    }

    bool hasNext() {
        return false;
    }

    void next() {
    }

    void ignoreSecondColumn() {
    }

    void clear() {
    }

    void mark() {
    }

    void reset(const char i) {
    }

    void moveto(const int64_t c1, const int64_t c2) {
    }

    bool allowMerge() {
        return false;
    }

    uint64_t estCardinality() {
        return 0;
    }

    uint64_t getCardinality() {
        return 0;
    }
};

#endif
