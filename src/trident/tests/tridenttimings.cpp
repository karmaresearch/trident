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


#include <trident/tests/common.h>
#include <trident/kb/kb.h>
#include <trident/kb/querier.h>

TridentTimings::TridentTimings(string inputfile, string filequeries) :
    Timings(filequeries), q(NULL), inputfile(inputfile) {}

void TridentTimings::init() {
    kb = std::unique_ptr<KB>(new KB(inputfile.c_str(), true, false, true, config));
    q = kb->query();
}

std::chrono::duration<double> TridentTimings::launchQuery(
    const int perm,
    const int64_t s,
    const int64_t p,
    const int64_t o,
    const int countIgnores,
    int64_t & c,
    int64_t & junk) {

    int64_t v1, v2, v3;

    PairItr *itr;
    std::chrono::system_clock::time_point start = std::chrono::system_clock::now();

    if (countIgnores == 2) {
        itr = q->getTermList(perm);
    } else {
        itr = q->getIterator(perm, s, p, o);
        if (itr != NULL && countIgnores == 1) {
            itr->ignoreSecondColumn();
        }
    }
    c = 0;

    bool hasNext = false;
    if (itr != NULL && itr->hasNext()) {
        do {
            hasNext = itr->next(v1, v2, v3);
            c++;
            junk += v1 + v2 + v3;
        } while (hasNext);
    }
    /*while (itr->hasNext()) {
        itr->next(v1, v2, v3);
        c++;
        junk += v1 + v2 + v3;
        //junk += itr->getValue1();
        //if (countIgnores == 0)
        //    junk += itr->getValue2();
    }*/
    std::chrono::duration<double> dur = std::chrono::system_clock::now()
                                          - start;
    q->releaseItr(itr);
    return dur;
}
