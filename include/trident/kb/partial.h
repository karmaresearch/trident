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

#ifndef _PARTIAL
#define _PARTIAL

#include <cstdint>
#include <fstream>
#include <google/sparse_hash_map>

class ReOrderItr;
class Querier;

using namespace std;

// For partial indices (dumps of reorderitr), we have, per index, two files:
// one containing a map from s,p,o triples to an offset in the other file,
// which then contains, at that position, the following:
// - number of keys
// for each key:
// - key value
// - number of pairs
// - values of the pairs.

// Triple structure with operator for testing for equality.
struct STriple {
    int64_t s, p, o;

    STriple() {
        s = p = o = 0;
    }

    STriple(int64_t s, int64_t p, int64_t o) {
        this->s = s;
        this->p = p;
        this->o = o;
    }

    bool operator == (const STriple &other) const {
        return s == other.s && p == other.p && o == other.o;
    }
};

// Hasher of Triple structure.
struct STripleHasher {
    std::size_t operator()(const STriple &keyTriple) const {
        std::size_t h = 0;
        h ^= keyTriple.s == -1 ? 0 : keyTriple.s;
        h ^= keyTriple.p == -1 ? 0 : (keyTriple.p << 12);
        h ^= keyTriple.o == -1 ? 0 : (keyTriple.o << 24);
        return h;
    }
};

typedef google::sparse_hash_map<STriple, uint64_t, STripleHasher> STripleMap;

// One Partial for each index
class Partial {
private:
    STripleMap index;
    std::ofstream indexStream;
    std::fstream dataStream;

public:
    Partial(int idx, std::string baseDir);

    ReOrderItr *getIterator(Querier *q, const int idx, const int64_t s, const int64_t p, const int64_t o);

    ~Partial() {
        indexStream.close();
        dataStream.close();
    }

    void dump(ReOrderItr *it);
};

#endif /* _PARTIAL */
