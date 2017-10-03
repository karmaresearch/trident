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


#ifndef STRINGBUFFER_H_
#define STRINGBUFFER_H_

#include <trident/kb/consts.h>
#include <trident/kb/statistics.h>

#include <kognac/factory.h>
#include <kognac/hashfunctions.h>
#include <kognac/utils.h>
#include <kognac/hashmap.h>

#include <thread>
#include <iostream>
#include <fstream>
#include <string>
#include <vector>

struct eqint {
    bool operator()(int i1, int i2) const {
        return i1 == i2;
    }
};

struct hashint {
    std::size_t operator()(int i) const {
        return i;
    }
};

class StringBuffer {
private:
    static char FINISH_THREAD[1];

    Stats *stats;
    std::string dir;

    char uncompressSupportBuffer[SB_BLOCK_SIZE * 2];
    char termSupportBuffer[MAX_TERM_SIZE];

    PreallocatedStratArraysFactory<char> factory;
    const bool readOnly;
    std::vector<char*> blocks;
    std::vector<long> sizeCompressedBlocks;
    long uncompressedSize;
    char *currentBuffer;
    int writingCurrentBufferSize;

    std::fstream sb;

    //Used by the compression thread
    std::mutex fileLock;
    char *bufferToCompress;
    int sizeBufferToCompress;

    std::condition_variable compressWait;
    std::mutex _compressMutex;
    std::thread compressionThread;

    //Used to check for termination
    std::condition_variable compressTerm;
    std::mutex _compressTerm;
    bool threadRunning;
    bool isThreadFinished() {
        return !threadRunning;
    }

    std::mutex sizeLock;

    int posBaseEntry;
    int sizeBaseEntry;
    int entriesSinceBaseEntry;
    int nMatchedChars;

    //Cache
    std::vector<std::pair<int, int>> cacheVector;
    int firstBlockInCache, lastBlockInCache;

    int elementsInCache;
    const int maxElementsInCache;

    void addCache(int idx);
    void compressBlocks();
    void compressLastBlock();
    void uncompressBlock(int b);

    char *getBlock(int idxBlock);

    int getFlag(int &blockId, char *&block, int &offset) {
        if (offset < SB_BLOCK_SIZE) {
            return block[offset++];
        } else {
            blockId++;
            block = getBlock(blockId);
            offset = 1;
            return block[0];
        }
    }

    int getVInt(int &blockId, char *&block, int &offset);

    void writeVInt(int n);

    void setCurrentAsBaseEntry(int size);

    int calculatePrefixWithBaseEntry(char *origBuffer, char *string, int size);

public:
    StringBuffer(string dir, bool readOnly, int factorySize, long cacheSize,
                 Stats *stats);

    long getSize();

    void append(char *string, int size);

    void get(long pos, char* outputBuffer, int &size);

    char* get(long pos, int &size);

    int cmp(long pos, char *string, int sizeString);

    ~StringBuffer();
};

#endif /* STRINGBUFFER_H_ */
