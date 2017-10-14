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


#include <trident/binarytables/rowtableinserter.h>

void RowTableInserter::startAppend() {
    previousValue1 = 0;
    nElements = 0;
}

void RowTableInserter::append(long t1, long t2) {
    bool writeIndexEntry = false;
    long keyToStore = 0;
    if (t1 != previousValue1 && nElements >= FIRST_INDEX_SIZE) {
        keyToStore = previousValue1;
        writeIndexEntry = index != NULL;
        nElements = 0;
    }

    // Write the index entry
    if (writeIndexEntry) {
        index->add(keyToStore, getCurrentFile(), getRelativePosition());
    }

    if (diffValue1 == DIFFERENCE) {
        t1 -= previousValue1;
        previousValue1 += t1;
    } else {
        previousValue1 = t1;
    }

    writeFirstTerm(t1);
    writeSecondTerm(t2);
    nElements++;
}

void RowTableInserter::stopAppend() {
}

void RowTableInserter::writeFirstTerm(long t) {
    switch (comprValue1) {
    case COMPR_1:
        writeVLong(t);
        return;
    case COMPR_2:
        writeVLong2(t);
        return;
    case NO_COMPR:
        writeLong(t);
        return;
    }
}

void RowTableInserter::writeSecondTerm(long t) {
    switch (comprValue2) {
    case COMPR_1:
        writeVLong(t);
        return;
    case COMPR_2:
        writeVLong2(t);
        return;
    case NO_COMPR:
        writeLong(t);
    }
}

void RowTableInserter::setCompressionMode(int v1, int v2) {
    comprValue1 = v1;
    comprValue2 = v2;
}

void RowTableInserter::setDifferenceMode(int d1) {
    diffValue1 = d1;
}
