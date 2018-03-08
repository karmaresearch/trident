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


#ifndef _COMPRFILEDESCRIPTOR_H
#define _COMPRFILEDESCRIPTOR_H

#include <trident/kb/consts.h>
#include <trident/utils/memorymgr.h>

#include <kognac/utils.h>

#include <fstream>
#include <sstream>
#include <string>

#define COMPRESSION_ENABLED 0
#define MAX_N_MAPPINGS  5000


struct FileSegment {
    char block[BLOCK_SIZE];
    int memId;
};

class Stats;
class ComprFileDescriptor {
private:
    const std::string file;
    const int id;
    const bool readOnly;
    int sizeMappings;
    int mappings[MAX_N_MAPPINGS];

    int uncompressedSize;
    FileSegment *uncompressedBuffers[MAX_N_MAPPINGS];

    //char specialTmpBuffer[BLOCK_MIN_SIZE];
    std::unique_ptr<char[]> specialTmpBuffers[MAX_SESSIONS];

    int lastAccessedSegment;

    //Used for writing
    int rawSize;
    char* rawBuffer;

    std::ifstream readOnlyInput;
    MemoryManager<FileSegment> *tracker;

    Stats* const stats;

    void uncompressBlock(const int b);
    void writeFile();
    //bool isUsed();
public:
    ComprFileDescriptor(bool readOnly, int id, std::string file, int maxSize,
                        MemoryManager<FileSegment> *tracker,
                        ComprFileDescriptor **parentArray, Stats* const stats);

    char* getBuffer(int offset, int *length, const int sesID);

    char* getBuffer(int offset, int *length, int &memoryBlock, const int sesID);

    int getFileLength() {
        return uncompressedSize;
    }

    int getId() {
        return id;
    }

    void appendLong(int64_t v) {
        Utils::encode_long(rawBuffer, uncompressedSize, v);
        uncompressedSize += 8;
    }

    int appendVLong(int64_t v) {
        uncompressedSize = Utils::encode_vlong(rawBuffer, uncompressedSize, v);
        return uncompressedSize;
    }

    int appendVLong2(int64_t v) {
        uncompressedSize = Utils::encode_vlong2(rawBuffer, uncompressedSize, v);
        return uncompressedSize;
    }

    void reserveBytes(const uint8_t n) {
        uncompressedSize += n;
    }

    void append(char *bytes, int size) {
        if (size == 1) {
            rawBuffer[uncompressedSize++] = *bytes;
        } else {
            memcpy(rawBuffer + uncompressedSize, bytes, size);
            uncompressedSize += size;
        }
    }

    void overwriteAt(int pos, char byte) {
        rawBuffer[pos] = byte;
    }

    void overwriteVLong2At(int pos, int64_t number) {
        Utils::encode_vlong2(rawBuffer, pos, number);
    }

    ~ComprFileDescriptor();
};

#endif
