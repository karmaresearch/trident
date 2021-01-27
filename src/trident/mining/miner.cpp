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


#include <trident/mining/miner.h>
#include <trident/kb/querier.h>

#include <iostream>

Miner::Miner(string kbDir, const int fptreeInternalBufferSize) :
    rootValue(std::make_pair(-1, -1)), tree(rootValue, fptreeInternalBufferSize) {
        KBConfig config;
        kb = std::unique_ptr<KB>(new KB(kbDir.c_str(), true, false, true, config));
        dict = kb->getDictMgmt();
        this->ignid = kb->getNTerms();
    }

void Miner::processPattern(std::vector<PatternElement>& buffer) {
    if (buffer.size() <= 2) {
        //Ignore groups that are too small to create a pattern to
        return;
    }
    tree.insert(buffer);
}

bool __sortBySupport(const FPattern<PatternElement> &el1,
        const FPattern<PatternElement> &el2) {
    if (el1.support != el2.support) {
        return el1.support > el2.support;
    } else {
        return el1.patternElements.size() < el2.patternElements.size();
    }
}

struct Tripl {
    int64_t s, p, o;
    int64_t count;
};

bool cmpTripl(const Tripl& x, const Tripl& y) {
    return x.s == y.s && x.p == y.p && x.o == y.o;
}

bool sortSPO(const Tripl &a, const Tripl &b) {
    if (a.s != b.s) {
        return a.s < b.s;
    } else {
        if (a.count != b.count) {
            return a.count > b.count;
        } else {
            if (a.p != b.p) {
                return a.p < b.p;
            } else {
                return a.o < b.o;
            }
        }
    }
}

bool sortPOS(const Tripl &a, const Tripl &b) {
    if (a.p != b.p) {
        return a.p < b.p;
    } else {
        if (a.o != b.o) {
            return a.o < b.o;
        } else {
            return a.s < b.s;
        }
    }
}

void Miner::mine() {
    //Load a KB
    Querier *q = kb->query();

    //First count the popularity of the rules
    PairItr *itr = q->getIterator(IDX_POS, -1, -1, -1);

    LOG(INFOL) << "Reading the input ...";
    //Collect all pairs in main memory
    std::vector<Tripl> triples;
    while (itr->hasNext()) {
        itr->next();
        Tripl t;
        t.s = itr->getValue2();
        t.p = itr->getKey();
        t.o = itr->getValue1();
        triples.push_back(t);
        Tripl t1;
        t1.s = t.s;
        t1.p = t.p;
        t1.o = ignid;
        triples.push_back(t1);
    }
    q->releaseItr(itr);
    //Release data structures
    delete q;

    //Sort and re-order and remove duplicates
    std::sort(triples.begin(), triples.end(), sortPOS);
    auto last = std::unique(triples.begin(), triples.end(), cmpTripl);
    triples.erase(last, triples.end());

    //Now we can count the frequencies
    LOG(INFOL) << "Counting the frequencies ...";
    std::pair<uint64_t, uint64_t> prevPair = std::make_pair(0, 0);
    int64_t count = 0;
    size_t prevIdx = 0;
    size_t currentIdx = 0;
    for (; currentIdx < triples.size(); ++currentIdx) {
        Tripl el = triples[currentIdx];
        if (el.p != prevPair.first || el.o != prevPair.second) {
            //Add the count to all previous triples
            for (size_t i = prevIdx; i < currentIdx; ++i) {
                triples[i].count = count;
            }
            count = 0;
            prevIdx = currentIdx;
            prevPair = make_pair(el.p, el.o);
        }
        count++;
    }
    for (size_t i = prevIdx; i < currentIdx; ++i) {
        triples[i].count = count;
    }

    //Sort all the triples by SPO
    LOG(INFOL) << "Sorting the triples by S,count ...";
    sort(triples.begin(), triples.end(), sortSPO);
    LOG(INFOL) << "Done.";

    //Create a buffer of pairs
    std::vector<PatternElement> buffer;
    buffer.reserve(100);

    //Scan the KB
    int64_t currentS = -1;
    int64_t counter = 0;
    for (const auto &triple : triples) {
        //LOG(INFOL) << triple.s << " " << triple.p << " " << triple.o << " " << triple.count;
        if (triple.s != currentS) {
            if (!buffer.empty()) {
                processPattern(buffer);
            }
            currentS = triple.s;
            buffer.clear();
        }
        buffer.push_back(make_pair(triple.p, triple.o));
        counter++;
        if (counter % 1000000 == 0) {
            LOG(INFOL) << "Processed " << counter << " pairs";
        }
    }

    if (!buffer.empty()) {
        processPattern(buffer);
    }
}

void Miner::getFrequentPatterns(const int minLen, const int maxLen, const int minSupport) {
    MyPatternContainer container(minLen, dict, this->ignid);
    tree.getFreqPatterns(container, maxLen, minSupport);
    container.printOrderedBySupport();
}

string MyPatternContainer::vector2string(const std::vector<PatternElement> &elements) {
    string out = "[";
    char textbuffer[MAX_TERM_SIZE];

    for (const auto &el : elements) {
        if (el.first != this->ignid) {
            dict->getText(el.first, textbuffer);
        } else {
            strcpy(textbuffer, "IGNORED");
        }
        out += "(" + string(textbuffer);
        if (el.second != this->ignid) {
            dict->getText(el.second, textbuffer);
        } else {
            strcpy(textbuffer, "IGNORED");
        }
        out += "," + string(textbuffer) + ") ";
    }
    out = out.substr(0, out.size() - 1);
    out += "]";
    return out;
}

void MyPatternContainer::add(const FPattern<PatternElement> &pattern) {
    if (pattern.patternElements.size() >= minLen)
        patterns.push_back(pattern);
}

void MyPatternContainer::printOrderedBySupport() {
    std::sort(patterns.begin(), patterns.end(), __sortBySupport);
    for (const auto &pattern : patterns) {
        if (pattern.patternElements.size() >= minLen) {
            string spatterns = vector2string(pattern.patternElements);
            cout << pattern.support << " " << spatterns <<  endl;
        }
    }
}
