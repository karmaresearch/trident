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


#ifndef ARRAYITR_H_
#define ARRAYITR_H_

#include <trident/iterators/pairitr.h>
#include <trident/kb/consts.h>

#include <inttypes.h>
#include <vector>
#include <memory>

typedef std::vector<std::pair<uint64_t, uint64_t> > Pairs;

class ArrayItr: public PairItr {
private:
    std::shared_ptr<Pairs> array;
    int nElements;
    int pos;
    int64_t v1, v2;

    int markPos;
    bool hasNextChecked;
    bool n;
    bool ignSecondColumn;
    int64_t countElems;

    static int binarySearch(Pairs *array,
                            int start, int end, uint64_t key);

public:
    int getTypeItr() {
        return ARRAY_ITR;
    }

    int64_t getValue1() {
        return v1;
    }

    int64_t getValue2() {
        return v2;
    }

    int64_t getCount();

    uint64_t getCardinality();

    uint64_t estCardinality();

    void ignoreSecondColumn();

    bool hasNext();

    void next();

    void mark();

    void reset(const char i);

    void clear();

    void moveto(const int64_t c1, const int64_t c2);

    void init(std::shared_ptr<Pairs> values, int64_t v1, int64_t v2);
};

#endif /* ARRAYITR_H_ */
