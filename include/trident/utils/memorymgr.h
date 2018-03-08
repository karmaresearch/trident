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


#ifndef BYTESTRACKER_H_
#define BYTESTRACKER_H_

#include <trident/kb/consts.h>

#include <iostream>
#include <assert.h>

using namespace std;

template<class K>
struct MemoryBlock {
    K *block;
    K** parentBlock;
    size_t bytes;
    int idx;
    int lock;
};

template<class K>
class MemoryManager {
private:
    const size_t cacheMaxSize;
    size_t bytes;

    MemoryBlock<K> **blocks;
    int start;
    int end;
    int blocksLeft;

    void removeOneBlock() {
        if (blocksLeft < MAX_N_BLOCKS_IN_CACHE) {
            if (start == MAX_N_BLOCKS_IN_CACHE) {
                start = 0;
            }

            //Check the block is not NULL
            while (blocks[start] == NULL || blocks[start]->lock > 0) {
                start++;
                if (start == MAX_N_BLOCKS_IN_CACHE) {
                    start = 0;
                }
            }
            removeBlock(start++);
        }
    }

public:
    MemoryManager(size_t cacheMaxSize) :
        cacheMaxSize(cacheMaxSize) {
            assert(cacheMaxSize > 0);
        bytes = 0;
        blocks = new MemoryBlock<K>*[MAX_N_BLOCKS_IN_CACHE];
        memset(blocks, 0, sizeof(MemoryBlock<K>*) * MAX_N_BLOCKS_IN_CACHE);
        start = end = 0;
        blocksLeft = MAX_N_BLOCKS_IN_CACHE;
    }

    void update(int idx, size_t bytes) {
        this->bytes -= blocks[idx]->bytes;
        this->bytes += bytes;
        blocks[idx]->bytes = bytes;
    }

    void removeBlock(int idx) {
        //Delete block. This will trigger the deconstructor of K which should remove the block and update all the datastructures
        bytes -= blocks[idx]->bytes;
        K *elToRemove = blocks[idx]->block;
        blocks[idx]->parentBlock[blocks[idx]->idx] = NULL;
        delete blocks[idx];
        blocks[idx] = NULL;
        blocksLeft++;
        delete elToRemove;
    }

    void removeBlockWithoutDeallocation(int idx) {
        if (blocks[idx] != NULL) {
            bytes -= blocks[idx]->bytes;
            delete blocks[idx];
            blocks[idx] = NULL;
            blocksLeft++;
        }
    }

    void addLock(int idx) {
        blocks[idx]->lock++;
    }

    bool isUsed(int idx) {
        return blocks[idx]->lock > 0;
    }

    void releaseLock(int idx) {
        blocks[idx]->lock--;
    }

    int add(size_t bytes, K *element, int idxInParentArray, K **parentArray) {
        if (this->bytes + bytes > cacheMaxSize) {
            while (this->bytes >= cacheMaxSize || blocksLeft == 0) {
                removeOneBlock();
            }
        }
        this->bytes += bytes;

        if (end == MAX_N_BLOCKS_IN_CACHE) {
        end = 0;
    }
    while (blocks[end] != NULL) {
            end = (end + 1) % MAX_N_BLOCKS_IN_CACHE;
        }

        MemoryBlock<K> *memoryBlock = new MemoryBlock<K>();
        memoryBlock->bytes = bytes;
        memoryBlock->block = element;
        memoryBlock->idx = idxInParentArray;
        memoryBlock->parentBlock = parentArray;
        memoryBlock->lock = 0;
        blocks[end] = memoryBlock;
        blocksLeft--;
        return end++;
    }

    ~MemoryManager() {
        while (blocksLeft < MAX_N_BLOCKS_IN_CACHE) {
            removeOneBlock();
        }
        delete[] blocks;
    }
};

#endif /* BYTESTRACKER_H_ */
