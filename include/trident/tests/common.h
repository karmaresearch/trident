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


#ifndef _COMMON_TESTS_H
#define _COMMON_TESTS_H

#include <trident/tests/timings.h>

#include <trident/kb/kb.h>
#include <trident/kb/querier.h>
#include <trident/iterators/pairitr.h>

struct _Triple {
    int64_t s, p, o;
    _Triple(int64_t s, int64_t p, int64_t o) {
        this->s = s;
        this->p = p;
        this->o = o;
    }
    _Triple(const _Triple &t) :
        s(t.s), p(t.p), o(t.o) {}
    _Triple() : s(0), p(0), o(0) {}
    bool operator==(const _Triple& rhs) const {
        return s == rhs.s && p == rhs.p && o == rhs.o;
    }
};

void _reorderTriple(int perm, PairItr *itr, int64_t triple[3]);

bool _less_spo(const _Triple &p1, const _Triple &p2);
bool _less_sop(const _Triple &p1, const _Triple &p2);
bool _less_ops(const _Triple &p1, const _Triple &p2);
bool _less_osp(const _Triple &p1, const _Triple &p2);
bool _less_pso(const _Triple &p1, const _Triple &p2);
bool _less_pos(const _Triple &p1, const _Triple &p2);

void _copyCurrentFirst(int perm, int64_t triple[3], int64_t v);
void _copyCurrentFirstSecond(int perm, int64_t triple[3], int64_t v1, int64_t v2);

void _test_createqueries(string inputfile, string queryfile);


class TridentTimings : public Timings {
private:
    Querier *q;
    KBConfig config;
    std::unique_ptr<KB> kb;
    string inputfile;

public:
    TridentTimings(string inputfile, string filequeries);

    void init();

    std::chrono::duration<double> launchQuery(const int perm,
            const int64_t s,
            const int64_t p,
            const int64_t o,
            const int countIgnores,
            int64_t &c,
            int64_t &junk);

    ~TridentTimings() {
        if (q != NULL)
            delete q;
    }
};

class TestTrident {
private:
    std::vector<_Triple> triples;
    Querier *q;

    bool shouldFail(int idx, int64_t first, int64_t second, int64_t third);
    void tryjumps(int perm, size_t s, size_t e);
    void test_moveto_ignoresecond(int perm, int64_t key,
                                  std::vector<uint64_t> &firsts);
public:
    TestTrident(KB *kb) {
        q = kb->query();
    }

    void prepare(string inputfile, std::vector<string> updatesDir);

    void test_existing(std::vector<int> permutations);

    void test_nonexist(std::vector<int> permutations);

    void test_moveto(std::vector<int> permutations);

    ~TestTrident() {
        delete q;
    }

};
#endif
