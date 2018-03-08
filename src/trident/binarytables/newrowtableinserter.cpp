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


#include <trident/binarytables/newrowtableinserter.h>

void NewRowTableInserter::startAppend() {
}

void NewRowTableInserter::append(int64_t t1, int64_t t2) {
    writeFirstTerm(t1);
    writeSecondTerm(t2);
}

void NewRowTableInserter::stopAppend() {
}

void NewRowTableInserter::writeFirstTerm(int64_t t) {
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

void NewRowTableInserter::writeSecondTerm(int64_t t) {
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
    case 4:
        writeLong(t);
        return;
    }
}
