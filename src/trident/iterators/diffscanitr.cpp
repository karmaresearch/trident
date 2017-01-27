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


#include <trident/iterators/diffscanitr.h>
#include <trident/kb/querier.h>

void DiffScanItr::gotoKey(long keyToSearch) {
    throw 10; //not yet implemented
}

bool DiffScanItr::hasNext() {
    if (hnc)
        return hn;

    hn = false;
    if (currentItr == NULL) {
        //Move to the next key
        if (root->hasNext()) {
            TermCoordinates values;
            currentkey = root->next(&values);
            currentItr = ((DiffIndex3*)diff)->getIterator(perm, currentkey, values);
            if (!currentItr->hasNext()) {
                BOOST_LOG_TRIVIAL(error) << "This should not happen";
            }
            if (ignseccol)
                currentItr->ignoreSecondColumn();
            currentItr->next();
            hn = true;
        }
    } else {
        if (currentItr->hasNext()) {
            currentItr->next();
            hn = true;
        } else {
            q->releaseItr(currentItr);
            currentItr = NULL;
            return hasNext();
        }
    }
    hnc = true;

    return hn;
}

void DiffScanItr::next() {
    setKey(currentkey);
    v1 = currentItr->getValue1();
    v2 = currentItr->getValue2();
    hnc = false;
}

uint64_t DiffScanItr::getCardinality() {
    if (ignseccol) {
        return diff->getNFirstTables(perm);
    } else {
        return diff->getSize();
    }
}

uint64_t DiffScanItr::estCardinality() {
    if (ignseccol) {
        return diff->getNFirstTables(perm);
    } else {
        return diff->getSize();
    }
}

void DiffScanItr::init(TreeItr *root, int perm, DiffIndex *diff) {
    this->perm = perm;
    this->root = std::unique_ptr<TreeItr>(root);
    this->diff = diff;
    hnc = false;
    currentItr = NULL;
    ignseccol = false;
}

void DiffScanItr::setQuerier(Querier *q) {
    this->q = q;
}

void DiffScanItr::moveto(const long c1, const long c2) {
    currentItr->moveto(c1, c2);
}

void DiffScanItr::clear() {
    root = std::unique_ptr<TreeItr>();
    if (currentItr != NULL) {
        q->releaseItr(currentItr);
        currentItr = NULL;
    }
}
