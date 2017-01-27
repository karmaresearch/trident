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

bool _less_spo(const _Triple &p1, const _Triple &p2) {
    if (p1.s < p2.s) {
        return true;
    } else if (p1.s == p2.s) {
        if (p1.p < p2.p) {
            return true;
        } else if (p1.p == p2.p) {
            return p1.o < p2.o;
        }
    }
    return false;
}

bool _less_sop(const _Triple &p1, const _Triple &p2) {
    if (p1.s < p2.s) {
        return true;
    } else if (p1.s == p2.s) {
        if (p1.o < p2.o) {
            return true;
        } else if (p1.o == p2.o) {
            return p1.p < p2.p;
        }
    }
    return false;
}

bool _less_pos(const _Triple &p1, const _Triple &p2) {
    if (p1.p < p2.p) {
        return true;
    } else if (p1.p == p2.p) {
        if (p1.o < p2.o) {
            return true;
        } else if (p1.o == p2.o) {
            return p1.s < p2.s;
        }
    }
    return false;
}

bool _less_pso(const _Triple &p1, const _Triple &p2) {
    if (p1.p < p2.p) {
        return true;
    } else if (p1.p == p2.p) {
        if (p1.s < p2.s) {
            return true;
        } else if (p1.s == p2.s) {
            return p1.o < p2.o;
        }
    }
    return false;
}

bool _less_osp(const _Triple &p1, const _Triple &p2) {
    if (p1.o < p2.o) {
        return true;
    } else if (p1.o == p2.o) {
        if (p1.s < p2.s) {
            return true;
        } else if (p1.s == p2.s) {
            return p1.p < p2.p;
        }
    }
    return false;
}

bool _less_ops(const _Triple &p1, const _Triple &p2) {
    if (p1.o < p2.o) {
        return true;
    } else if (p1.o == p2.o) {
        if (p1.p < p2.p) {
            return true;
        } else if (p1.p == p2.p) {
            return p1.s < p2.s;
        }
    }
    return false;
}

void _copyCurrentFirst(int perm, long triple[3], long v) {
    switch (perm) {
        case IDX_SPO:
        case IDX_SOP:
            triple[0] = v;
            break;
        case IDX_POS:
        case IDX_PSO:
            triple[1] = v;
            break;
        case IDX_OSP:
        case IDX_OPS:
            triple[2] = v;
            break;
    }
}

void _copyCurrentFirstSecond(int perm, long triple[3], long v1, long v2) {
    switch (perm) {
        case IDX_SPO:
            triple[0] = v1;
            triple[1] = v2;
            break;
        case IDX_SOP:
            triple[0] = v1;
            triple[2] = v2;
            break;
        case IDX_POS:
            triple[1] = v1;
            triple[2] = v2;
            break;
        case IDX_PSO:
            triple[1] = v1;
            triple[0] = v2;
            break;
        case IDX_OSP:
            triple[2] = v1;
            triple[0] = v2;
            break;
        case IDX_OPS:
            triple[2] = v1;
            triple[1] = v2;
            break;
    }
}

void _copyCurrentFirstSecondThird(int perm, long triple[3], long v1,
        long v2, long v3) {
    switch (perm) {
        case IDX_SPO:
            triple[0] = v1;
            triple[1] = v2;
            triple[2] = v3;
            break;
        case IDX_SOP:
            triple[0] = v1;
            triple[1] = v3;
            triple[2] = v2;
            break;
        case IDX_POS:
            triple[0] = v3;
            triple[1] = v1;
            triple[2] = v2;
            break;
        case IDX_PSO:
            triple[1] = v1;
            triple[0] = v2;
            triple[2] = v3;
            break;
        case IDX_OSP:
            triple[2] = v1;
            triple[0] = v2;
            triple[1] = v3;
            break;
        case IDX_OPS:
            triple[2] = v1;
            triple[1] = v2;
            triple[0] = v3;
            break;
    }
}

void _permute(int perm, long triple[3], long v1, long v2, long v3) {
    switch (perm) {
        case IDX_SPO:
            triple[0] = v1;
            triple[1] = v2;
            triple[2] = v3;
            break;
        case IDX_SOP:
            triple[0] = v1;
            triple[1] = v3;
            triple[2] = v2;
            break;
        case IDX_POS:
            triple[0] = v2;
            triple[1] = v3;
            triple[2] = v1;
            break;
        case IDX_PSO:
            triple[0] = v2;
            triple[1] = v1;
            triple[2] = v3;
            break;
        case IDX_OSP:
            triple[0] = v3;
            triple[1] = v1;
            triple[2] = v2;
            break;
        case IDX_OPS:
            triple[0] = v3;
            triple[1] = v2;
            triple[2] = v1;
            break;
    }
}

void _reorderTriple(int perm, PairItr *itr, long triple[3]) {
    switch (perm) {
        case IDX_SPO:
            triple[0] = itr->getKey();
            triple[1] = itr->getValue1();
            triple[2] = itr->getValue2();
            break;
        case IDX_SOP:
            triple[0] = itr->getKey();
            triple[2] = itr->getValue1();
            triple[1] = itr->getValue2();
            break;
        case IDX_POS:
            triple[1] = itr->getKey();
            triple[2] = itr->getValue1();
            triple[0] = itr->getValue2();
            break;
        case IDX_PSO:
            triple[1] = itr->getKey();
            triple[0] = itr->getValue1();
            triple[2] = itr->getValue2();
            break;
        case IDX_OSP:
            triple[2] = itr->getKey();
            triple[0] = itr->getValue1();
            triple[1] = itr->getValue2();
            break;
        case IDX_OPS:
            triple[2] = itr->getKey();
            triple[1] = itr->getValue1();
            triple[0] = itr->getValue2();
            break;
    }
}

int _cmpitr(std::vector<_Triple>::iterator &itr1,
        std::vector<_Triple>::iterator &itr2) {
    if (itr1->s < itr2->s) {
        return -1;
    } else if (itr1->s == itr2->s) {
        if (itr1->p < itr2->p) {
            return -1;
        } else if (itr1->p == itr2->p) {
            if (itr1->o < itr2->o) {
                return -1;
            } else if (itr1->o == itr2->o) {
                return 0;
            }
        }
    }
    return 1;
}

void TestTrident::prepare(string inputfile, std::vector<string> updates) {
    triples.clear();
    // Load the entire KB in main memory
    LZ4Reader reader(inputfile);
    while (!reader.isEof()) {
        long s = reader.parseVLong();
        long p = reader.parseVLong();
        long o = reader.parseVLong();
        triples.push_back(_Triple(s, p, o));
    }

    //Are there also updates? Load them all in main memory
    if (!updates.empty()) {
        for (int i = 0; i < updates.size(); ++i) {
            string update = updates[i];
            string updatefile = update + "/raw";
            if (!fs::exists(updatefile)) {
                BOOST_LOG_TRIVIAL(error) << "The update directory exists but contains no raw file.";
                throw 10;
            }
            if (fs::exists(update + "/ADD")) {
                LZ4Reader reader(updatefile);
                long count = 0;
                while (!reader.isEof()) {
                    long s = reader.parseLong();
                    long p = reader.parseLong();
                    long o = reader.parseLong();
                    triples.push_back(_Triple(s, p, o));
                    count++;
                }
                BOOST_LOG_TRIVIAL(debug) << "Loaded raw update of " << count << " triples";
            } else {
                std::vector<_Triple> rmtriples;
                LZ4Reader reader(updatefile);
                long count = 0;
                while (!reader.isEof()) {
                    long s = reader.parseLong();
                    long p = reader.parseLong();
                    long o = reader.parseLong();
                    rmtriples.push_back(_Triple(s, p, o));
                    count++;
                }
                BOOST_LOG_TRIVIAL(debug) << "Loaded raw del update of " << count << " triples";

                //Must sort the triples
                std::sort(triples.begin(), triples.end(), _less_spo);
                //Remove duplicates
                auto it = std::unique(triples.begin(), triples.end());
                triples.resize(std::distance(triples.begin(), it));

                std::sort(rmtriples.begin(), rmtriples.end(), _less_spo);
                it = std::unique(rmtriples.begin(), rmtriples.end());
                rmtriples.resize(std::distance(rmtriples.begin(), it));
                //Remove the triples from the original array
                std::vector<_Triple> newtriples;
                auto itr = triples.begin();
                auto rmitr = rmtriples.begin();
                while (itr != triples.end()) {
                    if (rmitr != rmtriples.end()) {
                        int res = _cmpitr(itr, rmitr);
                        if (res < 0) {
                            newtriples.push_back(*itr);
                            itr++;
                        } else if (res > 0) {
                            rmitr++;
                        } else { //Do not add it
                            itr++;
                        }
                    } else {
                        newtriples.push_back(*itr);
                        itr++;
                    }
                }
                triples = newtriples;
            }
        }

        //Must resort them
        std::sort(triples.begin(), triples.end(), _less_spo);
        //Remove duplicates
        auto it = std::unique(triples.begin(), triples.end());
        triples.resize(std::distance(triples.begin(), it));
    }
}

void TestTrident::test_existing(std::vector<int> permutations) {
    //Sort the triples and launch the queries
    for (auto perm : permutations) {
        BOOST_LOG_TRIVIAL(info) << "Testing permutation " << perm;
        //Sort the vector
        if (perm == IDX_SPO) {
            std::sort(triples.begin(), triples.end(), _less_spo);
        } else if (perm == IDX_SOP) {
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

        //Test the scan queries
        BOOST_LOG_TRIVIAL(info) << "Check a complete scan...";
        uint64_t card = q->getCardOnIndex(perm, -1, -1, -1);
        if (card != triples.size()) {
            BOOST_LOG_TRIVIAL(error) << "Cardinalities do not match: " << card << " " << triples.size();
            throw 10;
        }
        PairItr *currentItr = q->get(perm, -1, -1, -1);
        PairItr *scanWithoutLast = q->get(perm, -1, -1, -1);
        scanWithoutLast->ignoreSecondColumn();

        long countTriple = 0;
        long prevFirstEl = -1;
        long prevKey = -1;
        for (size_t idx = 0; idx < triples.size(); ++idx) {
            _Triple el = triples[idx];
            if (!currentItr->hasNext()) {
                throw 10; //should not happen
            }
            currentItr->next();

            long t[3];
            _reorderTriple(perm, currentItr, t);
            if (el.s != t[0] || el.p != t[1] || el.o != t[2]) {
                cout << "Mismatch! Comparing " << el.s << " " << el.p << " " << el.o << " with " <<
                    t[0] << " " << t[1] << " " << t[2] << " " << countTriple << endl;
                throw 10;
            } else {
                //cout << el.s << " " << el.p << " " << el.o << endl;
            }

            if (currentItr->getKey() != prevKey) {
                prevFirstEl = -1;
                prevKey = currentItr->getKey();
            }

            if (currentItr->getValue1() != prevFirstEl) {
                if (!scanWithoutLast->hasNext()) {
                    throw 10;
                }
                scanWithoutLast->next();
                long scanKey = scanWithoutLast->getKey();
                long scanValue1 = scanWithoutLast->getValue1();
                if (currentItr->getKey() != scanKey ||
                        currentItr->getValue1() != scanValue1) {
                    throw 10;
                }
                prevFirstEl = currentItr->getValue1();
            }

            countTriple++;
        }
        if (currentItr->hasNext()) {
            throw 10; //The iterator should be finished
        }
        if (scanWithoutLast->hasNext()) {
            throw 10;
        }
        q->releaseItr(currentItr);
        q->releaseItr(scanWithoutLast);
        currentItr = NULL;
        BOOST_LOG_TRIVIAL(info) << "All OK";

        //Test a scan without the second and third columns
        currentItr = q->getTermList(perm);
        BOOST_LOG_TRIVIAL(info) << "Check a filtered scan...";
        long prevEl = -1;
        for (auto const el : triples) {
            //cout << el.s << " " << el.p << " " << el.o << endl;
            long first = 0;
            switch (perm) {
                case IDX_SPO:
                case IDX_SOP:
                    first = el.s;
                    break;
                case IDX_POS:
                case IDX_PSO:
                    first = el.p;
                    break;
                case IDX_OPS:
                case IDX_OSP:
                    first = el.o;
                    break;
                default:
                    throw 10;
            }

            if (first != prevEl) {
                if (!currentItr->hasNext()) {
                    throw 10; //should not happen
                }
                currentItr->next();
                if (first != currentItr->getKey()) {
                    cout << "Mismatch! Comparing " << first << " " <<
                        currentItr->getKey() << endl;
                    throw 10;
                }
                prevEl = first;
            }
        }
        if (currentItr->hasNext()) {
            throw 10; //The iterator should be finished
        }
        q->releaseItr(currentItr);
        currentItr = NULL;
        BOOST_LOG_TRIVIAL(info) << "All OK";

        //Filter on the first element
        long currentFirst = -1;
        long currentSecond = -1;
        long count = 0;
        long countFirst = 0;
        long countSecond = 0;
        long pattern[3];
        pattern[0] = -1;
        pattern[1] = -1;
        pattern[2] = -1;
        PairItr *currentItrFirst = NULL;
        BOOST_LOG_TRIVIAL(info) << "Check cardinalities on the first level ...";
        for (size_t idx = 0; idx < triples.size(); ++idx) {
            _Triple el = triples[idx];
            long first, second;
            first = second = 0;
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

            //cout << first << " " << second << " " << endl;
            if (first != currentFirst) {
                if (currentItr != NULL) {
                    if (currentItr->hasNext()) {
                        throw 10; //The iterator should be finished
                    }
                    q->releaseItr(currentItr);
                    currentItr = NULL;
                }
                if (currentItrFirst != NULL) {
                    q->releaseItr(currentItrFirst);
                    currentItrFirst = NULL;
                }

                if (currentFirst != -1) {
                    _copyCurrentFirst(perm, pattern, currentFirst);
                    uint64_t card = q->getCardOnIndex(perm,
                            pattern[0],
                            pattern[1],
                            pattern[2]);
                    if (card != count) {
                        BOOST_LOG_TRIVIAL(error) << "Cardinalities do not match: " << card << " " << count;
                        throw 10;
                    }
                    uint64_t card2 = q->getCardOnIndex(perm, pattern[0],
                            pattern[1],
                            pattern[2],
                            true);
                    if (card2 != countFirst) {
                        BOOST_LOG_TRIVIAL(error) << "Cardinalities do not match: " << card2 << " " << countFirst;
                        throw 10;
                    }
                }
                currentFirst = first;
                currentSecond = second;
                count = 0;
                countFirst = 1;
                countSecond = 0;

                //To check whether all triples are the same
                _copyCurrentFirst(perm, pattern, currentFirst);
                currentItr = q->get(perm, pattern[0], pattern[1], pattern[2]);
                currentItrFirst = q->get(perm, pattern[0], pattern[1],
                        pattern[2]);
                currentItrFirst->ignoreSecondColumn();
            }

            if (second != currentSecond) {
                //Check the value with the one of currentItrFirst
                if (!currentItrFirst->hasNext()) {
                    throw 10; //should not happen
                }
                currentItrFirst->next();
                long c = currentItrFirst->getCount();
                if (currentItrFirst->getKey() != currentFirst ||
                        currentItrFirst->getValue1()
                        != currentSecond || c != countSecond) {
                    cout << "Mismatch: " << currentItrFirst->getKey() <<
                        " " << currentFirst <<
                        " " << currentItrFirst->getValue1() <<
                        " " << currentSecond <<
                        " " << c <<
                        " " << countSecond << endl;
                    throw 10;
                }
                countFirst++;
                countSecond = 0;
                currentSecond = second;
            }
            count++;
            countSecond++;

            //Check whether this triple corresponds to the currentItr
            if (!currentItr->hasNext()) {
                throw 10; //should not happen
            }
            currentItr->next();

            long t[3];
            _reorderTriple(perm, currentItr, t);
            if (el.s != t[0] || el.p != t[1] || el.o != t[2]) {
                cout << "Mismatch! Comparing " << el.s << " " << el.p << " " << el.o << " with " <<
                    t[0] << " " << t[1] << " " << t[2] << endl;
                throw 10;
            }
        }
        if (currentItr->hasNext()) {
            throw 10; //The iterator should be finished
        }
        if (currentItr != NULL)
            q->releaseItr(currentItr);
        if (currentItrFirst != NULL)
            q->releaseItr(currentItrFirst);

        if (currentFirst != -1) {
            _copyCurrentFirst(perm, pattern, currentFirst);
            uint64_t card = q->getCardOnIndex(perm, pattern[0], pattern[1], pattern[2]);
            if (card != count) {
                BOOST_LOG_TRIVIAL(error) << "Cardinalities do not match: " << card << " " << count;
                throw 10;
            }

            uint64_t card2 = q->getCardOnIndex(perm, pattern[0],
                    pattern[1],
                    pattern[2],
                    true);
            if (card2 != countFirst) {
                BOOST_LOG_TRIVIAL(error) << "Cardinalities do not match: " << card2 << " " << countFirst;
                throw 10;
            }

        }
        BOOST_LOG_TRIVIAL(info) << "All OK";

        //Filter on the second element
        currentItr = NULL;
        currentFirst = -1;
        currentSecond = -1;
        count = 0;
        pattern[0] = -1;
        pattern[1] = -1;
        pattern[2] = -1;
        long totalCount = 0;
        BOOST_LOG_TRIVIAL(info) << "Check cardinalities on the second level ...";
        for (size_t idx = 0; idx < triples.size(); ++idx) {
            _Triple el = triples[idx];
            totalCount++;
            if (totalCount % 1000000 == 0) {
                BOOST_LOG_TRIVIAL(debug) << "Processed " << totalCount << " triples";
            }
            long first, second;
            first = second = 0;
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
            if (first != currentFirst || second != currentSecond) {
                if (currentFirst != -1) {
                    _copyCurrentFirstSecond(perm, pattern, currentFirst,
                            currentSecond);
                    uint64_t card = q->getCardOnIndex(perm, pattern[0],
                            pattern[1], pattern[2]);
                    if (card != count) {
                        BOOST_LOG_TRIVIAL(error) << "Cardinalities " <<
                            "do not match: " <<
                            card << " " << count;
                        throw 10;
                    }
                }
                currentFirst = first;
                currentSecond = second;
                count = 0;

                //new iterator
                if (currentItr != NULL) {
                    if (currentItr->hasNext()) {
                        throw 10; //The iterator should be finished
                    }
                    q->releaseItr(currentItr);
                }
                _copyCurrentFirstSecond(perm, pattern, currentFirst, currentSecond);
                currentItr = q->get(perm, pattern[0], pattern[1], pattern[2]);
            }
            count++;

            //Check whether this triple corresponds to the currentItr
            if (!currentItr->hasNext()) {
                throw 10; //should not happen
            }
            currentItr->next();
            long t[3];
            _reorderTriple(perm, currentItr, t);
            if (el.s != t[0] || el.p != t[1] || el.o != t[2]) {
                cout << "Mismatch! Comparing " << el.s << " " << el.p << " " << el.o << " with " <<
                    t[0] << " " << t[1] << " " << t[2] << endl;
                throw 10;
            }
        }
        q->releaseItr(currentItr);
        if (currentFirst != -1) {
            _copyCurrentFirstSecond(perm, pattern, currentFirst, currentSecond);
            uint64_t card = q->getCardOnIndex(perm, pattern[0], pattern[1], pattern[2]);
            if (card != count) {
                BOOST_LOG_TRIVIAL(error) << "Cardinalities do not match: " << card << " " << count;
                throw 10;
            }
        }
        BOOST_LOG_TRIVIAL(info) << "All OK";

        BOOST_LOG_TRIVIAL(info) << "Check single lookups ...";
        for (auto const el : triples) {
            long first, second, third;
            first = second = third = 0;
            switch (perm) {
                case IDX_SPO:
                    first = el.s;
                    second = el.p;
                    third = el.o;
                    break;
                case IDX_SOP:
                    first = el.s;
                    second = el.o;
                    third = el.p;
                    break;
                case IDX_POS:
                    first = el.p;
                    second = el.o;
                    third = el.s;
                    break;
                case IDX_PSO:
                    first = el.p;
                    second = el.s;
                    third = el.o;
                    break;
                case IDX_OSP:
                    first = el.o;
                    second = el.s;
                    third = el.p;
                    break;
                case IDX_OPS:
                    first = el.o;
                    second = el.p;
                    third = el.s;
                    break;
            }
            _copyCurrentFirstSecondThird(perm, pattern, first, second, third);
            PairItr *itr = q->get(perm, pattern[0], pattern[1], pattern[2]);
            if (!itr->hasNext()) {
                throw 10;
            }
            itr->next();
            if (itr->getKey() != first || itr->getValue1() != second ||
                    itr->getValue2() != third) {
                throw 10;
            }
            if (itr->hasNext()) {
                throw 10;
            }
            q->releaseItr(itr);
            uint64_t card = q->getCardOnIndex(perm, pattern[0], pattern[1], pattern[2]);
            if (card != 1) {
                throw 10;
            }
        }
        BOOST_LOG_TRIVIAL(info) << "All OK";

    }
}

void TestTrident::tryjumps(int perm, size_t s, size_t e) {
    size_t n = e - s;
    if (n > 1) {
        PairItr *itr = NULL;
        for (int i = s; i < e; ) {
            _Triple el = triples[i];
            long first, second, third;
            first = second = third = 0;
            switch (perm) {
                case IDX_SPO:
                    first = el.s;
                    second = el.p;
                    third = el.o;
                    break;
                case IDX_SOP:
                    first = el.s;
                    second = el.o;
                    third = el.p;
                    break;
                case IDX_POS:
                    first = el.p;
                    second = el.o;
                    third = el.s;
                    break;
                case IDX_PSO:
                    first = el.p;
                    second = el.s;
                    third = el.o;
                    break;
                case IDX_OSP:
                    first = el.o;
                    second = el.s;
                    third = el.p;
                    break;
                case IDX_OPS:
                    first = el.o;
                    second = el.p;
                    third = el.s;
                    break;
            }

            if (!itr) {
                itr = q->getPermuted(perm, first, -1, -1, true);
                if (!itr->hasNext())
                    throw 10;
                itr->next();
            } else {
                //if (exact || third == 0) {
                itr->moveto(second, third);
                //} else { //move to element before
                //    itr->moveto(second, third - 1);
                //}
                //exact = !exact;
                if (!itr->hasNext()) {
                    throw 10;
                }
                itr->next();
            }
            if (itr->getValue1() != second || itr->getValue2() != third) {
                throw 10;
            }
            i += 3; //Skip three triples
        }
        if (itr)
            q->releaseItr(itr);
    }
}

void TestTrident::test_moveto_ignoresecond(int perm, long key,
        std::vector<uint64_t> &firsts) {
    PairItr *itr = q->getPermuted(perm, key, -1, -1, true);
    itr->ignoreSecondColumn();
    for (int i = 0; i < firsts.size(); ++i) {
        if (!itr->hasNext()) {
            throw 10;
        }
        itr->next();
        //Move to the same position
        long v1 = itr->getValue1();
        if (v1 != firsts[i])
            throw 10;

        itr->moveto(v1, 0);
        if (!itr->hasNext()) {
            throw 10;
        }
        itr->next();
        if (itr->getValue1() != v1) {
            throw 10;
        }
    }

    //Move to the center
    if (firsts.size() > 2) {
        size_t middle = firsts.size() / 2;
        PairItr *itr2 = q->getPermuted(perm, key, -1, -1, true);
        itr2->ignoreSecondColumn();
        itr2->hasNext();
        itr2->next();
        itr2->moveto(firsts[middle], 0);
        if (!itr2->hasNext())
            throw 10;
        itr2->next();
        if (itr2->getValue1() != firsts[middle])
            throw 10;
        q->releaseItr(itr2);
    }

    //Move beyond the end
    itr->moveto(firsts.back() + 1, 0);
    if (itr->hasNext()) {
        throw 10;
    }
    q->releaseItr(itr);


}

void TestTrident::test_moveto(std::vector<int> permutations) {
    BOOST_LOG_TRIVIAL(info) << "Testing moveto";
    for (auto perm : permutations) {
        BOOST_LOG_TRIVIAL(info) << "Testing permutation " << perm;
        if (perm == IDX_SPO) {
            std::sort(triples.begin(), triples.end(), _less_spo);
        } else if (perm == IDX_SOP) {
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

        std::vector<uint64_t> allfirsts;
        long prevkey = -1;
        size_t previdx = 0;
        PairItr *itr = NULL;
        for (size_t i = 0; i < triples.size(); ++i) {
            _Triple el = triples[i];
            long first, second, third;
            first = second = third = 0;
            switch (perm) {
                case IDX_SPO:
                    first = el.s;
                    second = el.p;
                    third = el.o;
                    break;
                case IDX_SOP:
                    first = el.s;
                    second = el.o;
                    third = el.p;
                    break;
                case IDX_POS:
                    first = el.p;
                    second = el.o;
                    third = el.s;
                    break;
                case IDX_PSO:
                    first = el.p;
                    second = el.s;
                    third = el.o;
                    break;
                case IDX_OSP:
                    first = el.o;
                    second = el.s;
                    third = el.p;
                    break;
                case IDX_OPS:
                    first = el.o;
                    second = el.p;
                    third = el.s;
                    break;
            }

            if (first != prevkey) {
                //Test moveto with ignoresecond
                if (!allfirsts.empty()) {
                    test_moveto_ignoresecond(perm, prevkey, allfirsts);
                }
                allfirsts.clear();
                if (itr != NULL) {
                    if (itr->hasNext()) {
                        BOOST_LOG_TRIVIAL(error) << "itr should not have more elements";
                        throw 10;
                    }
                    q->releaseItr(itr);
                }
                itr = q->getPermuted(perm, first, -1, -1, true);
                prevkey = first;

                PairItr *itr2 = q->getPermuted(perm, first, -1, -1, true);
                if (!itr2->hasNext()) {
                    throw 10;
                }
                itr2->next();
                itr2->moveto(~0u, ~0u);
                if (itr2->hasNext()) {
                    throw 10;
                }
                q->releaseItr(itr2);

                //Try to jump to higher locations
                tryjumps(perm, previdx, i);
                previdx = i;
            }

            if (allfirsts.empty() || allfirsts.back() != second) {
                allfirsts.push_back(second);
            }

            //Test the behaviour of the moveto method
            if (!itr->hasNext()) {
                BOOST_LOG_TRIVIAL(error) << "itr should not have some elements";
                throw 10;
            } else {
                itr->next();
                //Check the values are the same
                if (itr->getValue1() != second || itr->getValue2() != third) {
                    throw 10;
                }
                //Now move to the same value
                itr->moveto(second, third);
                if (!itr->hasNext()) {
                    throw 10;
                }
                itr->next();
                if (itr->getValue1() != second || itr->getValue2() != third) {
                    throw 10;
                }

                //Now move to values that are before the current point. The iterator should remain at the same position
                if (second > 0) {
                    itr->moveto(second - 1, third);
                    if (!itr->hasNext()) {
                        throw 10;
                    }
                    itr->next();
                    if (itr->getValue1() != second || itr->getValue2() != third) {
                        throw 10;
                    }
                }
                if (third > 0) {
                    itr->moveto(second, third - 1);
                    if (!itr->hasNext()) {
                        throw 10;
                    }
                    itr->next();
                    if (itr->getValue1() != second || itr->getValue2() != third) {
                        throw 10;
                    }
                }
            }
        }
    }
}

void TestTrident::test_nonexist(std::vector<int> permutations) {
    BOOST_LOG_TRIVIAL(info) << "Testing non-existing queries";
    for (auto perm : permutations) {
        BOOST_LOG_TRIVIAL(info) << "Testing permutation " << perm;
        if (perm == IDX_SPO) {
            std::sort(triples.begin(), triples.end(), _less_spo);
        } else if (perm == IDX_SOP) {
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

        //Type of queries that fail:
        //a) queries with a number above the keys,
        //queries with an existing key
        //b) but a lower bound for first term, a upper bound, and one in between,
        //c) same as before but on the second column
        BOOST_LOG_TRIVIAL(info) << "Testing a query with a max non-existing key";
        //Get the maximum number in the array for the given permutation
        long maxNumber = 0;
        long minNumber = 0;
        if (perm == IDX_SPO || perm == IDX_SOP) {
            maxNumber = triples.back().s;
            minNumber = triples.front().s;
        } else if (perm == IDX_POS || perm == IDX_PSO) {
            maxNumber = triples.back().p;
            minNumber = triples.front().p;
        } else {
            maxNumber = triples.back().o;
            minNumber = triples.front().o;
        }
        maxNumber += 1;
        PairItr *itr = q->getPermuted(perm, maxNumber, -1, -1, true);
        if (itr->hasNext()) {
            BOOST_LOG_TRIVIAL(error) << "A query with key (max) " << maxNumber << " should fail!";
            throw 10;
        }
        q->releaseItr(itr);
        if (minNumber > 0) {
            BOOST_LOG_TRIVIAL(info) << "Testing a query with a min non-existing key";
            PairItr *itr = q->getPermuted(perm, minNumber - 1, -1, -1, true);
            if (itr->hasNext()) {
                BOOST_LOG_TRIVIAL(error) << "A query with key (min) " << (minNumber - 1) << " should fail!";
                throw 10;
            }
            q->releaseItr(itr);
        }

        //Try some random keys (up to 100000)
        BOOST_LOG_TRIVIAL(info) << "Testing random non-existing keys";
        const int maxAttempts = 100000;
        int currentAttempt = 0;
        long prevkey = minNumber;
        for (size_t i = 0; i < triples.size(); ++i) {
            long currentkey;
            if (perm == IDX_SPO || perm == IDX_SOP) {
                currentkey = triples[i].s;
            } else if (perm == IDX_POS || perm == IDX_PSO) {
                currentkey = triples[i].p;
            } else {
                currentkey = triples[i].o;
            }
            if (currentkey > prevkey + 1) {
                PairItr *itr = q->getPermuted(perm, prevkey + 1, -1, -1, true);
                if (itr->hasNext()) {
                    throw 10;
                }
                q->releaseItr(itr);
                currentAttempt++;
                if (currentAttempt == maxAttempts) {
                    break;
                }
            }
            prevkey = currentkey;
        }
        BOOST_LOG_TRIVIAL(info) << "All OK (keys tested=" << currentAttempt << ")";

        //Testing some queries where the key is existing but the first term does not.
        //For each key I try three possibilities: first term is too small, too large and in-between
        BOOST_LOG_TRIVIAL(info) << "Testing non-existing first terms";
        prevkey = minNumber;
        int previdx = 0;
        long t[3];
        long ntests = 0;
        for (size_t i = 0; i < triples.size(); ++i) {
            _permute(perm, t, triples[i].s, triples[i].p, triples[i].o);
            if (t[0] != prevkey) {
                //Test minimum
                long prevt[3];
                _permute(perm, prevt, triples[previdx].s, triples[previdx].p,
                        triples[previdx].o);
                if (prevt[1] > 0) {
                    long tocheck = prevt[1] - 1;
                    shouldFail(perm, prevkey, tocheck, -1);
                    shouldFail(perm, prevkey, tocheck, 0);
                    ntests += 2;
                }
                //Test maximum
                _permute(perm, prevt, triples[i - 1].s, triples[i - 1].p,
                        triples[i - 1].o);
                shouldFail(perm, prevkey, prevt[1] + 1, -1);
                shouldFail(perm, prevkey, prevt[1] + 1, 0);

                //Test in-between
                for (int j = previdx + 1; j < i; ++j) {
                    //Find first non-existing first term
                    long curt[3];
                    _permute(perm, curt, triples[j].s, triples[j].p,
                            triples[j].o);
                    long curfirst = curt[1];
                    _permute(perm, curt, triples[j - 1].s, triples[j - 1].p,
                            triples[j - 1].o);
                    long prevfirst = curt[1];
                    if (curfirst > prevfirst + 1) {
                        shouldFail(perm, prevkey, curfirst - 1, -1);
                        shouldFail(perm, prevkey, curfirst - 1, 0);
                        ntests += 2;
                    }
                }

                prevkey = t[0];
                previdx = i;
                ntests += 2;
            }
        }
        BOOST_LOG_TRIVIAL(info) << "All OK (ntests=" << ntests << ")";

        BOOST_LOG_TRIVIAL(info) << "Testing non-existing second terms";
        ntests = 0;
        for (size_t i = 0; i < triples.size(); ++i) {
            _permute(perm, t, triples[i].s, triples[i].p, triples[i].o);
            //Do a test before and one after
            if (i % 2 == 0) {
                if (i > 0) {
                    long prevt[3];
                    _permute(perm, prevt, triples[i - 1].s, triples[i - 1].p, triples[i - 1].o);
                    if (t[0] == prevt[0] && t[1] == prevt[1]) {
                        if ((t[2] - prevt[2]) > 1) {
                            shouldFail(perm, t[0], t[1], t[2] - 1);
                            ntests++;
                        }
                    } else if (t[2] > 0) {
                        shouldFail(perm, t[0], t[1], t[2] - 1);
                        ntests++;
                    }
                }
                if (i < triples.size() - 1) {
                    long suct[3];
                    _permute(perm, suct, triples[i + 1].s, triples[i + 1].p, triples[i + 1].o);
                    if (t[0] == suct[0] && t[1] == suct[1]) {
                        if ((suct[2] - t[2]) > 1) {
                            shouldFail(perm, t[0], t[1], t[2] + 1);
                            ntests++;
                        }
                    } else if (t[2] > 0) {
                        shouldFail(perm, t[0], t[1], t[2] + 1);
                        ntests++;
                    }
                }
            }
        }
        BOOST_LOG_TRIVIAL(info) << "All OK (ntests=" << ntests << ")";
    }
}

bool TestTrident::shouldFail(int idx, long first, long second, long third) {
    PairItr *itr = q->getPermuted(idx, first, second, third, true);
    bool sf = !itr->hasNext();
    q->releaseItr(itr);
    if (!sf) {
        BOOST_LOG_TRIVIAL(error) << "Test " << idx << " " << first << " " << second << " " << third << " has failed!";
        throw 10;
    }
    return sf;
}
