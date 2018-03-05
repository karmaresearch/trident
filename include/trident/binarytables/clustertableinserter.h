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


#ifndef _CLUSTERTABLE_INSERTER_H
#define _CLUSTERTABLE_INSERTER_H

#include <trident/binarytables/fileindex.h>
#include <trident/binarytables/binarytableinserter.h>
#include <trident/kb/consts.h>

class ClusterTableInserter : public BinaryTableInserter {
private:
    //Common vars
    int64_t previousFirstTerm, previousSecondTerm;
    short baseSecondTermFile;
    int baseSecondTermPos;
    short fileLastFirstTerm;
    int posLastFirstTerm;

    FileIndex *secondTermIndex;
    bool removeSecondTermIndex;

    int64_t lastSecondTerm;
    int64_t nElementsForIndexing;

    //Additional vars used to write the second term
    int bytesUsed;
    bool smallGroupMode;
    int nElementsGroup;

    int diffMode1;
    int compr1;
    int compr2;

    int getRelativeSecondTermPos();
    int getRelativeSecondTermPos(short file, int absPos);
    void writeNElementsAt(char b, short file, int pos);
    void insertSecondTerm(bool last);
    void updateSecondTermIndex(int64_t lastTermWritten, int bytesTaken,
                               short currentFile, int currentPos);
    int64_t calculateSecondTermToWrite(int64_t term);
    int writeSecondTerm(int64_t termToWrite);
    void updateFirstTermIndex(const int64_t t1);
    void writeFirstTerm(int64_t termToWrite);
    int64_t calculateFirstTermToWrite(int64_t termToWrite);

public:
    int getType() {
        return CLUSTER_ITR;
    }

    void startAppend();

    void append(int64_t t1, int64_t t2);

    void stopAppend();

    void mode_difference(int modeValue1);

    void mode_compression(int compr1, int compr2);
};

#endif
