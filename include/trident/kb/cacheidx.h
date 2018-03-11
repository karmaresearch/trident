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


#ifndef _CACHEIDX
#define _CACHEIDX

#include <google/dense_hash_map>
#include <vector>
#include <cstdint>
#include <climits>

struct CacheBlock {
    uint64_t startKey;
    uint64_t endKey;
    uint64_t startArray;
    uint64_t endArray;

    CacheBlock() {
        startKey = endKey = startArray = endArray = 0;
    }

    bool operator < (const CacheBlock &b1) const {
        if (startKey == b1.startKey) {
            return endKey > b1.endKey;
        }
        return startKey < b1.startKey;
    }
};

struct eqnumbers {
    bool operator()(const uint64_t v1, const uint64_t v2) const {
        return v1 == v2;
    }
};


typedef google::dense_hash_map<uint64_t,
        std::pair<std::vector<CacheBlock>*, std::vector<std::pair<uint64_t, uint64_t>>*>,
        std::hash<uint64_t>, eqnumbers> KeyMap;

class CacheIdx {
private:
    KeyMap keyMap;
public:

    CacheIdx() {
        keyMap.set_empty_key(UINT64_MAX);
    }

    std::pair<std::vector<std::pair<uint64_t, uint64_t>>*,
        std::vector<CacheBlock>*
        > getIndex(uint64_t key);

    void storeIdx(uint64_t key, std::vector<CacheBlock> *blocks,
                  std::vector<std::pair<uint64_t, uint64_t>> *pairs);

    ~CacheIdx();
};

#endif
