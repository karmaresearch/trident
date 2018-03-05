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


#ifndef INDEX_H_
#define INDEX_H_

#include <vector>
#include <algorithm>
#include <string.h>
#include <iostream>

#define INITIAL_SIZE 128

class FileIndex {
private:
    int lengthArrays;
    int lengthAdditionalArrays;

    int64_t *keys;
    short *files;
    int *positions;
    int size;
    int additionalSize;
    int64_t *additionalKeys;
    FileIndex **additionalIndices;

    void checkLengthArrays(int size) {
        if (size >= lengthArrays) {
            int newsize = std::max(std::max(size * 2, INITIAL_SIZE), (int) (lengthArrays * 1.5));
            int64_t *newkeys = new int64_t[newsize];
            int *newpositions = new int[newsize];
            short *newfiles = new short[newsize];
            if (size > 0) {
                memcpy(newkeys, keys, sizeof(int64_t) * lengthArrays);
                memcpy(newpositions, positions, sizeof(int) * lengthArrays);
                memcpy(newfiles, files, sizeof(short) * lengthArrays);
                delete[] keys;
                delete[] positions;
                delete[] files;
            }
            keys = newkeys;
            positions = newpositions;
            files = newfiles;
            lengthArrays = newsize;
        }
    }

    void checkLengthAdditionalArrays(int size) {
        if (size >= lengthAdditionalArrays) {
            int newsize = std::max(std::max(size * 2, INITIAL_SIZE), (int) (lengthAdditionalArrays * 1.5));
            int64_t *newkeys = new int64_t[newsize];
            FileIndex **newindices = new FileIndex*[newsize];
            if (size > 0) {
                memcpy(newkeys, additionalKeys,
                       sizeof(int64_t) * lengthAdditionalArrays);
                memcpy(newindices, additionalIndices,
                       sizeof(FileIndex*) * lengthAdditionalArrays);
                delete[] additionalKeys;
                delete[] additionalIndices;
            }
            additionalKeys = newkeys;
            additionalIndices = newindices;
            lengthAdditionalArrays = newsize;
        }
    }

public:
    FileIndex();
    void unserialize(char* buffer, int *offset);
    short file(int idx);
    int pos(int idx);
    int64_t key(int idx);
    int idx(const int64_t key);
    int idx(const int startIdx, const int64_t key);
    FileIndex *additional_idx(int64_t key);
    int sizeIndex();
    ~FileIndex();

    //Used for insert
    bool isEmpty();
    char* serialize(char *buffer, int &offset, int &maxSize);
    void add(int64_t key, short file, int pos);
    void addAdditionalIndex(int64_t key, FileIndex *idx);
};

#endif /* INDEX_H_ */
