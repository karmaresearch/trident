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


#include <trident/iterators/compositescanitr.h>
#include <trident/kb/querier.h>

void CompositeScanItr::gotoKey(long keyToSearch) {
    throw 10; //not yet implemented
}

bool CompositeScanItr::hasNext() {
    if (!hnc) {
        if (children.size() > 1) {
            if (ignseccol)
                std::sort(children.begin(), children.end(), CompositeScanItr::_sorter_ign);
            else
                std::sort(children.begin(), children.end(), CompositeScanItr::_sorter_noign);
            hn = true;
        } else if (children.size() > 0) {
            hn = true;
        } else {
            hn = false;
        }
        hnc = true;
    }

    return hn;
}

void CompositeScanItr::next() {
    setKey(children.back()->getKey());
    v1 = children.back()->getValue1();
    if (!ignseccol) {
        v2 = children.back()->getValue2();
        if (children.back()->hasNext()) {
            children.back()->next();
        } else {
            children.pop_back();
        }
    } else {
        //Fix count and remove more terms
        count = 0;
        for (int i = children.size() - 1; i >= 0; i--) {
            long thiskey = getKey();
            long scanKey = children[i]->getKey();
            long scanValue1 = children[i]->getValue1();
            if (scanKey == thiskey &&
                    scanValue1 == v1) {
                count += children[i]->getCount();
                if (children[i]->hasNext()) {
                    children[i]->next();
                } else {
                    children.erase(children.begin() + i);
                }
            }
        }
    }
    hnc = false;
}

uint64_t CompositeScanItr::getCardinality() {
    if (ignseccol) {
        return q->getNFirstTablesPerPartition(perm);
    } else {
        return q->getInputSize();
    }
}

uint64_t CompositeScanItr::estCardinality() {
    return getCardinality();
}

void CompositeScanItr::setQuerier(Querier *q) {
    this->q = q;
}

void CompositeScanItr::moveto(const long c1, const long c2) {
    for (int i = 0; i < children.size(); ++i) {
        children[i]->moveto(c1, c2);
    }
    hnc = false;
}

void CompositeScanItr::clear() {
    for (int i = 0; i < allChildren.size(); ++i) {
        q->releaseItr(allChildren[i]);
    }
    allChildren.clear();
    children.clear();
}

void CompositeScanItr::addChild(PairItr *itr) {
    if (itr->hasNext()) {
        itr->next();
        children.push_back(itr);
        allChildren.push_back(itr);
    }
}

void CompositeScanItr::ignoreSecondColumn() {
    ignseccol = true;
    for (int i = 0; i < children.size(); ++i) {
        children[i]->ignoreSecondColumn();
        if (children[i]->getValue1() == v1 &&
                children[i]->getKey() == getKey()) {
            if (children[i]->hasNext()) {
                children[i]->next();
            } else {
                children.erase(children.begin() + i);
            }
        }
    }
}

long CompositeScanItr::getCount() {
    return count;
}

void CompositeScanItr::init(int perm) {
    hnc = false;
    hn = false;
    v1 = v2 = 0;
    constraint1 = constraint2 = -1;
    ignseccol = false;
    count = 1;
    this->perm = perm;
}
