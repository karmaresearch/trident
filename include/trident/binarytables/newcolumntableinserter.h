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


#ifndef _NEWCOLUMNTABLEINSERTER_H
#define _NEWCOLUMNTABLEINSERTER_H

#include <trident/binarytables/binarytableinserter.h>

class NewColumnTableInserter: public BinaryTableInserter {
private:
    uint64_t largestElement1, largestElement2, largestGroup;

    long prevel1, prevtotalsize2;
    std::vector<std::pair<uint64_t, uint64_t>> tmpfirstpairs;
    std::vector<uint64_t> tmpsecondpairs;

    const size_t thresholdToOffload;
    long offloadedElements1, offloadedElements2;
    bool fileopen1, fileopen2;
    ofstream offloadfile1;
    ifstream offloadfile1_r;
    ofstream offloadfile2;
    ifstream offloadfile2_r;

public:

    NewColumnTableInserter() : thresholdToOffload(500000000l) {
    }

    int getType() {
        return NEWCOLUMN_ITR;
    }

    void startAppend();

    void append(long t1, long t2);

    void stopAppend();
};

#endif
