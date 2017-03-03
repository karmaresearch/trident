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


#include <trident/iterators/rmitr.h>

#include <algorithm>
#include <assert.h>

void RmItr::init(PairItr *itr, PairItr *rmitr, long nfirstterms) {
    this->itr = itr;
    this->rmitr = rmitr;
    this->delnfirstterms = nfirstterms;
    noseccol = false;
    hnc = hn = false;
    v1 = v2 = key = -1;
    constraint1 = constraint2 = -1;
    if (rmitr->hasNext()) {
        rmitr->next();
        rmitrvalid = true;
    } else {
        rmitrvalid = false;
    }
    curCount = nextCount = 0;
}

void RmItr::moveto(const long c1, const long c2) {
    assert(v1 != -1);
    itr->moveto(c1, c2);
    rmitr->moveto(c1, c2);
    if (rmitr->hasNext()) {
        rmitr->next();
    } else {
        rmitrvalid = false;
    }
}

bool RmItr::hasNext() {
    if (hnc)
        return hn;

    bool valid = false;
    while (itr->hasNext()) {
        //Check if there is a valid element
        itr->next();
        if (rmitrvalid) {
            //Check if it's equivalent. If so, we move to the next element
            int cmpres = cmp(rmitr, itr);
            while (cmpres < 0) {
                //Move the rmitr to the right position
                if (rmitr->hasNext()) {
                    rmitr->next();
                    cmpres = cmp(rmitr, itr);
                } else {
                    break;
                }
            }
            if (cmpres < 0) {//No more triples to remove
                nextCount = itr->getCount();
                rmitrvalid = false;
                valid = true;
                break;
            } else if (cmpres > 0) { //The triple is not marked to be removed
                nextCount = itr->getCount();
                valid = true;
                break;
            } else {
                //We must continue to find the next valid element
                if (noseccol) {
                    //Check the count. If greater than 0 then it's fine
                    nextCount = itr->getCount() - rmitr->getCount();
                    if (nextCount > 0) {
                        valid = true;
                        break;
                    }
                }
            }
        } else {
            nextCount = itr->getCount();
            valid = true;
            break;
        }
    }
    hnc = true;
    hn = valid;
    return hn;
}

void RmItr::next() {
    setKey(itr->getKey());
    v1 = itr->getValue1();
    v2 = itr->getValue2();
    curCount = nextCount;
    hnc = false;
}

long RmItr::getCount() {
    return curCount;
}

uint64_t RmItr::getCardinality() {
    if (noseccol) {
        return itr->getCardinality() - delnfirstterms;
    } else {
        return itr->getCardinality() - rmitr->getCardinality();
    }
}

uint64_t RmItr::estCardinality() {
    uint64_t card = itr->estCardinality();
    if (rmitrvalid)
        card -= rmitr->estCardinality();
    return std::max((uint64_t)0, card);
}

int RmItr::cmp(PairItr *itr1, PairItr *itr2) {
    if (noseccol) {
        if (itr1->getKey() < itr2->getKey()) {
            return -1;
        } else if (itr1->getKey() == itr2->getKey()) {
            long diff = itr1->getValue1() - itr2->getValue1();
            if (diff < 0)
                return -1;
            else if (diff > 0)
                return 1;
            else
                return 0;
        } else {
            return 1;
        }
    } else {
        if (itr1->getKey() < itr2->getKey()) {
            return -1;
        } else if (itr1->getKey() == itr2->getKey()) {
            long diff = itr1->getValue1() - itr2->getValue1();
            if (diff < 0) {
                return -1;
            } else if (diff > 0) {
                return 1;
            } else {
                long diff2 = itr1->getValue2() - itr2->getValue2();
                if (diff2 < 0) {
                    return -1;
                } else if (diff2 > 0) {
                    return 1;
                } else {
                    return 0;
                }
            }
        } else {
            return 1;
        }
    }
}

void RmItr::ignoreSecondColumn() {
    itr->ignoreSecondColumn();
    rmitr->ignoreSecondColumn();
    noseccol = true;
    if (v1 == -1) {
        nextCount = itr->getCount();
        if (rmitr->getValue1() == itr->getValue1())
            nextCount -= rmitr->getCount();
    } else {
        curCount = itr->getCount();
        if (rmitr->getValue1() == itr->getValue1())
            curCount -= rmitr->getCount();
    }
}
