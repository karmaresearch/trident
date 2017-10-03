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


#include <trident/kb/cacheidx.h>

#include <string>

std::pair<std::vector<std::pair<uint64_t, uint64_t>>*,
std::vector<CacheBlock>*> CacheIdx::getIndex(uint64_t key) {
    KeyMap::iterator itr = keyMap.find(key);
    if (itr != keyMap.end()) {
        return std::make_pair(itr->second.second, itr->second.first);
    } else {
        return std::make_pair((std::vector<std::pair<uint64_t, uint64_t>>*)NULL,
                              (std::vector<CacheBlock>*)NULL);
    }
}

void CacheIdx::storeIdx(uint64_t key, std::vector<CacheBlock> *blocks,
                        std::vector<std::pair<uint64_t, uint64_t>> *pairs) {
    KeyMap::iterator itr = keyMap.find(key);
    if (itr != keyMap.end()) {
        //A merge is needed

#ifdef DEBUG
        for (std::vector<CacheBlock>::iterator itrNewBlocks = blocks->begin();
                itrNewBlocks != blocks->end(); ++itrNewBlocks) {
            for (std::vector<CacheBlock>::iterator itrBlocks = itr->second.first->begin();
                    itrBlocks != itr->second.first->end(); ++itrBlocks) {
                if (itrNewBlocks->startKey >= itrBlocks->startKey &&
                        itrNewBlocks->startKey < itrBlocks->endKey) {
                    assert(false);
                }
                if (itrNewBlocks->endKey > itrBlocks->startKey &&
                        itrNewBlocks->endKey <= itrBlocks->endKey) {
                    assert(false);
                }
                if (itrNewBlocks->startKey >= itrBlocks->startKey &&
                        itrNewBlocks->endKey <= itrBlocks->endKey) {
                    assert(false);
                }
            }
        }
#endif
        //All blocks should not be in ranges that are already existing. I just add them
        for (std::vector<CacheBlock>::iterator itrNewBlocks = blocks->begin();
                itrNewBlocks != blocks->end(); ++itrNewBlocks) {
            //Add all the pairs that are pointed by the block
            size_t newStart = itr->second.second->size();
            for (int i = itrNewBlocks->startArray; i < itrNewBlocks->endArray; ++i) {
                itr->second.second->push_back(pairs->at(i));
            }
            itrNewBlocks->startArray = newStart;
            itrNewBlocks->endArray = itr->second.second->size();
            itr->second.first->push_back(*itrNewBlocks);
        }

        //Re-Sort all the blocks
        std::sort(itr->second.first->begin(), itr->second.first->end());

        //Delete the input variables. No longer need them
        delete blocks;
        delete pairs;
    } else {
        keyMap.insert(std::make_pair(key, std::make_pair(blocks, pairs)));
    }
}

CacheIdx::~CacheIdx() {
    for (KeyMap::iterator itr = keyMap.begin(); itr != keyMap.end(); ++itr) {
        delete itr->second.first;
        delete itr->second.second;
    }
}
