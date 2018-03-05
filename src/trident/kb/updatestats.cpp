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


#include <trident/kb/diffindex.h>
#include <trident/kb/querier.h>

UpdateStats::UpdateStats(Querier *q, int perm1,
                         int perm2, bool storep1, bool storep2) :
    perm1(perm1), perm2(perm2),
    storep1(storep1), storep2(storep2) {
    c1 = c2 = totalc1 = totalc2 = nkeys = totalkeys = 0;
    this->q = q;
    itr1 = itr2 = NULL;
    shouldCheck1 = shouldCheck2 = false;
    existingkeys = q->getTermList(perm1);
    if (!existingkeys->hasNext()) {
        currentkey = ~0u;
    } else {
        existingkeys->next();
        currentkey = existingkeys->getKey();
    }
}

UpdateStats::~UpdateStats() {
    if (itr1) {
        q->releaseItr(itr1);
    }
    if (itr2) {
        q->releaseItr(itr2);
    }
    if (existingkeys)
        q->releaseItr(existingkeys);
}

void UpdateStats_add::setKey(const int64_t key, const size_t sizetable) {
    bool isNew = false;

    if (currentkey > key) {
        nkeys++;
        isNew = true;
    } else {
        while (currentkey < key && existingkeys->hasNext()) {
            existingkeys->next();
            currentkey = existingkeys->getKey();
        }
        if (currentkey != key) {
            nkeys++;
            isNew = true;
        }
    }
    totalkeys++;

    shouldCheck1 = shouldCheck2 = !isNew;
    if (storep1 && shouldCheck1) {
        if (itr1)
            q->releaseItr(itr1);
        itr1 = q->getPermuted(perm1, key, -1, -1, true);
        itr1->ignoreSecondColumn();
        itr1->hasNext();
        itr1->next();
    }
    if (storep2 && shouldCheck2) {
        if (itr2)
            q->releaseItr(itr2);
        itr2 = q->getPermuted(perm2, key, -1, -1, true);
        itr2->ignoreSecondColumn();
        itr2->hasNext();
        itr2->next();
    }

}

void UpdateStats_add::addFirst1(const int64_t v, const int64_t count) {
    assert(v >= 0);
    totalc1++;
    if (storep1) {
        if (shouldCheck1 && itr1) {
            while (itr1->getValue1() < v && itr1->hasNext()) {
                itr1->next();
            }
            if (itr1->getValue1() < v) {
                //Release the itr
                q->releaseItr(itr1);
                itr1 = NULL;
                c1++;
                invertedpairs1.push_back(v);
            } else if (itr1->getValue1() > v) {
                c1++;
                invertedpairs1.push_back(v);
            } else {
                //Not new. Move to the next element ...
                if (itr1->hasNext()) {
                    itr1->next();
                } else {
                    q->releaseItr(itr1);
                    itr1 = NULL;
                }
            }
        } else {
            c1++;
            invertedpairs1.push_back(v);
        }
    }
}

void UpdateStats_add::addFirst2(const int64_t v, const int64_t count) {
    assert(v >= 0);
    totalc2++;
    if (storep2) {
        if (shouldCheck2 && itr2) {
            while (itr2->getValue1() < v && itr2->hasNext()) {
                itr2->next();
            }
            if (itr2->getValue1() < v) {
                //release the itr
                q->releaseItr(itr2);
                itr2 = NULL;
                c2++;
                invertedpairs2.push_back(v);
            } else if (itr2->getValue1() > v) {
                c2++;
                invertedpairs2.push_back(v);
            } else {
                //move to the next element
                if (itr2->hasNext()) {
                    itr2->next();
                } else {
                    q->releaseItr(itr2);
                    itr2 = NULL;
                }

            }
        } else {
            c2++;
            invertedpairs2.push_back(v);
        }
    }
}

void UpdateStats_rm::setKey(const int64_t key, const size_t sizetable) {
    bool isNew = false;
    while (currentkey < key && existingkeys->hasNext()) {
        existingkeys->next();
        currentkey = existingkeys->getKey();
    }

    assert(currentkey == key);
    if (existingkeys->getCount() == sizetable) {
        nkeys++;
        isNew = true;
    }
    totalkeys++;

    shouldCheck1 = shouldCheck2 = !isNew;
    if (storep1 && shouldCheck1) {
        if (itr1)
            q->releaseItr(itr1);
        itr1 = q->getPermuted(perm1, key, -1, -1, true);
        itr1->ignoreSecondColumn();
        itr1->hasNext();
        itr1->next();
    }
    if (storep2 && shouldCheck2) {
        if (itr2)
            q->releaseItr(itr2);
        itr2 = q->getPermuted(perm2, key, -1, -1, true);
        itr2->ignoreSecondColumn();
        itr2->hasNext();
        itr2->next();
    }
}

void UpdateStats_rm::addFirst1(const int64_t v, const int64_t count) {
    totalc1++;
    if (storep1) {
        if (shouldCheck1 && itr1) {
            while (itr1->getValue1() < v && itr1->hasNext()) {
                itr1->next();
            }
            assert(itr1->getValue1() == v);
            if (itr1->getCount() == count) {
                c1++;
                invertedpairs1.push_back(v);
            }
        } else {
            c1++;
            invertedpairs1.push_back(v);
        }
    }
}

void UpdateStats_rm::addFirst2(const int64_t v, const int64_t count) {
    totalc2++;
    if (storep2) {
        if (shouldCheck2 && itr2) {
            while (itr2->getValue1() < v && itr2->hasNext()) {
                itr2->next();
            }
            assert(itr2->getValue1() == v);
            if (itr2->getCount() == count) {
                c2++;
                invertedpairs2.push_back(v);
            }
        } else {
            c2++;
            invertedpairs2.push_back(v);
        }
    }
}
