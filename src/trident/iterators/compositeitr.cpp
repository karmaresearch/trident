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


#include <trident/iterators/compositeitr.h>

#include <boost/log/trivial.hpp>

#include <vector>

bool CompositeItr::hasNext() {
    if (!hnc) {
        assert(isSorted);
        assert(v1 >= 0);
        decreasedKey = false;
        if (lastIdx >= 0) {
            if (!ignseccol) {
                if (!children[lastIdx]->hasNext()) {
                    lastIdx--;
                    decreasedKey = true;
                }
            } else {
                bool isKeyOk = false;
                for (int i = lastIdx; i >= 0; i--) {
                    if (children[i]->getValue1() == v1) {
                        if (!children[i]->hasNext()) {
                            if (i != lastIdx) {
                                //Must swap elements
                                PairItr *box = children[lastIdx];
                                children[lastIdx] = children[i];
                                children[i] = box;
                            }
                            lastIdx--;
                            decreasedKey = true;
                        } else {
                            isKeyOk = true;
                        }
                    } else {
                        break;
                    }
                }
                if (decreasedKey && isKeyOk)
                    decreasedKey = false;
            }
        }
        hn = lastIdx >= 0;
        hnc = true;
    }
    return hn;
}

void CompositeItr::next() {
    if (!hnc) {
        hasNext();
    }

    if (!decreasedKey) {
        children[lastIdx]->next();
        if (!isSorted || ignseccol) {
            for (int i = lastIdx - 1; i >= 0; i--) {
                if (!isSorted || v1 == children[i]->getValue1()) {
                    children[i]->next();
                }
            }
        }
        rearrangeChildren();
    }

    v1 = children[lastIdx]->getValue1();
    if (!ignseccol) {
        v2 = children[lastIdx]->getValue2();
    } else {
        currentCount = 0;
        for (int i = lastIdx; i >= 0; i--) {
            if (children[i]->getValue1() == v1) {
                currentCount += children[i]->getCount();
            }
        }
    }
    hnc = false;
}

void CompositeItr::ignoreSecondColumn() {
    ignseccol = true;
    currentCount = 0;
    for (int i = children.size() - 1; i >= 0; i--) {
        children[i]->ignoreSecondColumn();
        if (v1 != -1) {
            if (children[i]->getValue1() == v1) {
                currentCount += children[i]->getCount();
            }
        }
    }
}

DiffIndex *CompositeItr::getCurrentDiff() {
    return currentdiff;
}

bool _composite_itr_sorter(PairItr *i1, PairItr *i2) {
    return i1->getValue1() > i2->getValue1() ||
           (i1->getValue1() == i2->getValue1() &&
            i1->getValue2() > i2->getValue2());
}

void CompositeItr::rearrangeChildren() {
    if (lastIdx == 1) {
        //Swap them if not in the right order
        if ((children[1]->getValue1() > children[0]->getValue1()) ||
                (!ignseccol &&
                 children[1]->getValue1() == children[0]->getValue1() &&
                 children[1]->getValue2() > children[0]->getValue2())) {
            PairItr *box = children[1];
            children[1] = children[0];
            children[0] = box;
        }
    }
    if (lastIdx > 1) {
        sort(children.begin(), children.begin() + lastIdx + 1, _composite_itr_sorter);
    }
    isSorted = true;
}

long CompositeItr::getCount() {
    return currentCount;
}

uint64_t CompositeItr::getCardinality() {
    if (!ignseccol) {
        //Sum all cardinalities
        uint64_t card = 0;
        for (int i = 0; i < children.size(); ++i) {
            card += children[i]->getCardinality();
        }
        return card;
    } else {
        uint64_t card = nfirstterms;
        //If one child is the kbitr, then add it
        for (int i = 0; i < children.size(); ++i) {
            if (children[i] == kbitr) {
                card += children[i]->getCardinality();
            }
        }
        return card;
    }
}

uint64_t CompositeItr::estCardinality() {
    if (!ignseccol) {
        //Sum all cardinalities
        uint64_t card = 0;
        for (int i = 0; i < children.size(); ++i) {
            card += children[i]->estCardinality();
        }
        return card;
    } else {
        uint64_t card = nfirstterms;
        //If one child is the kbitr, then add it
        for (int i = 0; i < children.size(); ++i) {
            if (children[i] == kbitr) {
                card += children[i]->estCardinality();
            }
        }
        return card;
    }
}

void CompositeItr::moveto(const long c1, const long c2) {
    if (c1 == v1 && (ignseccol || c2 == v2)) {
        //I might have to reconsider also the iterators that were removed
        while (lastIdx < children.size() - 1 &&
                children[lastIdx + 1]->getValue1() == c1 &&
                (ignseccol || children[lastIdx + 1]->getValue2() == c2)) {
            lastIdx++;
        }
    }

    for (int i = lastIdx; i >= 0; i--) {
        children[i]->moveto(c1, c2);
        if (!children[i]->hasNext()) {
            if (i != lastIdx) {
                //Must swap elements
                PairItr *box = children[lastIdx];
                children[lastIdx] = children[i];
                children[i] = box;
            }
            lastIdx--;
        }
        isSorted = false;
        decreasedKey = false;
    }
    hn = lastIdx >= 0;
    hnc = true;
}

void CompositeItr::init(std::vector<PairItr*> &iterators,
                        long nfirstterms, PairItr * kbitr) {
    ignseccol = false;
    v1 = v2 = -1;
    constraint1 = constraint2 = -1;
    children = iterators;
    hn = !children.empty();
    hnc =  true;
    lastIdx = children.size() - 1;
    currentdiff = NULL;
    currentCount = 1;
    decreasedKey = isSorted = false;
    for (int i = 0; i < iterators.size(); ++i)
        assert(iterators[i]->hasNext());

    this->nfirstterms = nfirstterms;
    this->kbitr = kbitr;
}

std::vector<PairItr*> CompositeItr::getChildren() {
    return children;
}
