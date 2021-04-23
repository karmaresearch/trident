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


#include <trident/tests/common.h>
#include <trident/kb/kb.h>
#include <trident/kb/querier.h>
#include <kognac/lz4io.h>

void _test_createqueries(string inputfile, string queryfile) {

    // Load the entire KB in main memory
    std::vector<_Triple> triples;
    LZ4Reader reader(inputfile);
    while (!reader.isEof()) {
        int64_t s = reader.parseVLong();
        int64_t p = reader.parseVLong();
        int64_t o = reader.parseVLong();
        triples.push_back(_Triple(s, p, o));
        if (triples.size() > 1) {
            if (_less_spo(triples[triples.size() - 1], triples[triples.size() - 2]))
                throw 10;
        }
    }

    //Sort the triples and launch the queries
    ofstream ofile(queryfile);
    for (int perm = 0; perm < 6; ++perm) {
        LOG(INFOL) << "Testing permutation " << perm;
        if (perm != IDX_SPO) {
            //Sort the vector
            if (perm == IDX_SOP) {
                std::sort(triples.begin(), triples.end(), _less_sop);
            } else if (perm == IDX_POS) {
                std::sort(triples.begin(), triples.end(), _less_pos);
            } else if (perm == IDX_PSO) {
                std::sort(triples.begin(), triples.end(), _less_pso);
            } else if (perm == IDX_OSP) {
                std::sort(triples.begin(), triples.end(), _less_osp);
            } else if (perm == IDX_OPS) {
                std::sort(triples.begin(), triples.end(), _less_ops);
            } else {
                throw 10;
            }
        }

        LOG(INFOL) << "Scan query...";
        ofile << perm << " " << -1 << " " << -1 << " " << -1 << " " << triples.size() << " " << 0 << endl;
        int64_t prevEl = -1;
        int64_t countDistinct = 0;
        for (auto const &el : triples) {
            int64_t first = 0;
            switch (perm) {
            case IDX_SPO:
            case IDX_SOP:
                first = el.s;
                break;
            case IDX_POS:
            case IDX_PSO:
                first = el.p;
                break;
            case IDX_OSP:
            case IDX_OPS:
                first = el.o;
                break;
            }
            if (first != prevEl) {
                prevEl = first;
                countDistinct++;
            }
        }
        ofile << perm << " " << -1 << " " << -2 << " " << -2 << " " << countDistinct << " " << 1 << endl;

        //Test a scan without the second and third columns
        LOG(INFOL) << "Create detailed queries...";
        int64_t currentFirst = -1;
        int64_t currentSecond = -1;

        int64_t countFirst1 = 0;
        int64_t countFirst2 = 0;
        int64_t countSecond = 0;
        for (auto const &el : triples) {
            int64_t first, second;
            switch (perm) {
            case IDX_SPO:
                first = el.s;
                second = el.p;
                break;
            case IDX_SOP:
                first = el.s;
                second = el.o;
                break;
            case IDX_POS:
                first = el.p;
                second = el.o;
                break;
            case IDX_PSO:
                first = el.p;
                second = el.s;
                break;
            case IDX_OSP:
                first = el.o;
                second = el.s;
                break;
            case IDX_OPS:
                first = el.o;
                second = el.p;
                break;
            }


            if (first != currentFirst) {
                if (currentFirst != -1) {
                    ofile << perm << " " << currentFirst << " " << -1 << " " << -1 << " " << countFirst1 << " " << 2 << endl;
                    ofile << perm << " " << currentFirst << " " << -1 << " " << -2 << " " << countFirst2 << " " << 3 << endl;
                    ofile << perm << " " << currentFirst << " " << currentSecond << " " << -1 << " " << countSecond << " " << 4 << endl;
                }
                currentFirst = first;
                currentSecond = second;
                countFirst1 = 1;
                countFirst2 = 1;
                countSecond = 1;
            } else if (second != currentSecond) {
                if (currentSecond != -1) {
                    ofile << perm << " " << currentFirst << " " << currentSecond << " " << -1 << " " << countSecond << " " << 4 << endl;
                    countSecond = 0;
                }
                currentSecond = second;
                countFirst1++;
                countFirst2++;
                countSecond++;
            } else {
                countFirst1++;
                countSecond++;
            }
        }
        if (currentFirst != -1) {
            ofile << perm << " " << currentFirst << " " << -1 << " " << -1 << " " << countFirst1 << " " << 2 << endl;
            ofile << perm << " " << currentFirst << " " << -1 << " " << -2 << " " << countFirst2 << " " << 3 << endl;
            ofile << perm << " " << currentFirst << " " << currentSecond << " " << -1 << " " << countSecond << " " << 4 << endl;
        }
    }
    ofile.close();
}
