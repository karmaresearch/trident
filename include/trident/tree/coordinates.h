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


#ifndef VALUE_H_
#define VALUE_H_

#include <trident/kb/consts.h>

#include <kognac/logs.h>

#include <inttypes.h>

typedef struct Coordinates {
    uint8_t permutation;
    uint64_t nElements;
    uint16_t file;
    uint64_t posInFile;
    uint8_t strategy;
    Coordinates *next;
} Coordinates;

class TermCoordinates {
private:
    short fileIdxs[N_PARTITIONS];
    int64_t marks[N_PARTITIONS];
    char strategies[N_PARTITIONS];
    int64_t nElements[N_PARTITIONS];
    bool activePermutations[N_PARTITIONS];
public:

    void clear() {
        activePermutations[0] = false;
        activePermutations[1] = false;
        activePermutations[2] = false;
        activePermutations[3] = false;
        activePermutations[4] = false;
        activePermutations[5] = false;
    }

    void set(Coordinates *el) {
        clear();

        while (el != NULL) {
            int part = el->permutation;
            fileIdxs[part] = el->file;
            marks[part] = el->posInFile;
            strategies[part] = el->strategy;
            nElements[part] = el->nElements;
            activePermutations[part] = true;
            el = el->next;
        }
    }

    void set(int permutation, short file, int64_t pos, int64_t nElements,
             char strategy) {
        activePermutations[permutation] = true;
        fileIdxs[permutation] = file;
        marks[permutation] = pos;
        this->nElements[permutation] = nElements;
        strategies[permutation] = strategy;
    }

    short getFileIdx(int perm) const {
        return fileIdxs[perm];
    }

    int64_t getMark(int perm) const {
        return marks[perm];
    }

    char getStrategy(int perm) const {
        return strategies[perm];
    }

    int64_t getNElements(int perm) const {
        return nElements[perm];
    }

    bool exists(int perm) const {
        return activePermutations[perm];
    }

    uint64_t* getValues(int perm, int64_t *size) {
        LOG(ERRORL) << "Not implemented";
        return NULL;
    }

    void copyFrom(TermCoordinates *value) {
        activePermutations[0] = value->activePermutations[0];
        activePermutations[1] = value->activePermutations[1];
        activePermutations[2] = value->activePermutations[2];
        activePermutations[3] = value->activePermutations[3];
        activePermutations[4] = value->activePermutations[4];
        activePermutations[5] = value->activePermutations[5];

        if (activePermutations[0]) {
            fileIdxs[0] = value->fileIdxs[0];
            nElements[0] = value->nElements[0];
            marks[0] = value->marks[0];
            strategies[0] = value->strategies[0];
        }

        if (activePermutations[1]) {
            fileIdxs[1] = value->fileIdxs[1];
            nElements[1] = value->nElements[1];
            marks[1] = value->marks[1];
            strategies[1] = value->strategies[1];
        }

        if (activePermutations[2]) {
            fileIdxs[2] = value->fileIdxs[2];
            nElements[2] = value->nElements[2];
            marks[2] = value->marks[2];
            strategies[2] = value->strategies[2];
        }

        if (activePermutations[3]) {
            fileIdxs[3] = value->fileIdxs[3];
            nElements[3] = value->nElements[3];
            marks[3] = value->marks[3];
            strategies[3] = value->strategies[3];
        }

        if (activePermutations[4]) {
            fileIdxs[4] = value->fileIdxs[4];
            nElements[4] = value->nElements[4];
            marks[4] = value->marks[4];
            strategies[4] = value->strategies[4];
        }

        if (activePermutations[5]) {
            fileIdxs[5] = value->fileIdxs[5];
            nElements[5] = value->nElements[5];
            marks[5] = value->marks[5];
            strategies[5] = value->strategies[5];
        }
    }
};

#endif /* VALUE_H_ */
