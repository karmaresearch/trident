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


#include <trident/binarytables/newclustertableinserter.h>

void NewClusterTableInserter::startAppend() {
    prevt1 = -1;
    v2.clear();
}

void NewClusterTableInserter::append(int64_t t1, int64_t t2) {
    if (t1 != prevt1) {
        //Write down the group
        writeGroup();
        prevt1 = t1;
    }
    v2.push_back(t2);
}

void NewClusterTableInserter::stopAppend() {
    writeGroup();
}

void NewClusterTableInserter::writeGroup() {
    if (!v2.empty()) {
        writeFirstTerm(prevt1);
        //Write the counter
        if (countsize == 1) {
            writeByte(v2.size());
        } else if (countsize == 4) {
            writeInt(v2.size());
        }
        for (const auto &el : v2) {
            writeSecondTerm(el);
        }
        v2.clear();
    }
}

void NewClusterTableInserter::writeFirstTerm(int64_t t) {
    switch (reader1) {
    case 0:
        writeByte(t);
        return;
    case 1:
        writeShort(t);
        return;
    case 2:
        writeInt(t);
        return;
    case 3:
        writeLongInt(t);
        return;
    }
}

void NewClusterTableInserter::writeSecondTerm(int64_t t) {
    switch (reader2) {
    case 0:
        writeByte(t);
        return;
    case 1:
        writeShort(t);
        return;
    case 2:
        writeInt(t);
        return;
    case 3:
        writeLongInt(t);
        return;
    }
}
