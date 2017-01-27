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


#ifndef _TUPLEKBITERATOR_H
#define _TUPLEKBITERATOR_H

#include <trident/iterators/tupleiterators.h>
#include <trident/iterators/pairitr.h>
#include <vector>

class Tuple;
class Querier;
class TupleKBItr : public TupleIterator {
private:
    Querier *querier;
    PairItr *physIterator;
    int idx;
    int *invPerm;

    bool onlyVars;
    uint8_t varsPos[3];
    uint8_t sizeTuple;

    std::vector<std::pair<uint8_t, uint8_t>> equalFields;

    bool nextProcessed;
    bool nextOutcome;
    size_t processedValues;

    bool checkFields();

public:
    TupleKBItr();

    PairItr *getPhysicalIterator() {
        return physIterator;
    }

    void ignoreSecondColumn() {
        physIterator->ignoreSecondColumn();
    }

    void init(Querier *querier, const Tuple *literal,
              const std::vector<uint8_t> *fieldsToSort) {
        init(querier, literal, fieldsToSort, false);
    }

    void init(Querier *querier, const Tuple *literal,
              const std::vector<uint8_t> *fieldsToSort, bool onlyVars);

    bool hasNext();

    void next();

    size_t getTupleSize();

    uint64_t getElementAt(const int pos);

    ~TupleKBItr();

    void clear();
};

#endif
