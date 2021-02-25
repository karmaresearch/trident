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


#include <trident/iterators/scanitr.h>
#include <trident/tree/treeitr.h>
#include <trident/kb/querier.h>

#include <trident/binarytables/storagestrat.h>

#include <iostream>

using namespace std;

void ScanItr::init(int idx, Querier *q) {
    initializeConstraints();
    this->idx = idx;
    this->q = q;
    currentTable = m_currentTable = NULL;
    reversedItr = m_reversedItr = NULL;
    m_itr1 = m_itr2 = NULL;
    //hasNextChecked = false;

    itr1 = q->getKBTermList(idx, true);
    if (idx > 2 || itr1 == NULL) {
        itr2 = q->getKBTermList(idx - 3, false);
        if (itr1) {
            if (itr1->hasNext()) {
                itr1->next();
            } else {
                q->releaseItr(itr1);
                itr1 = NULL;
            }
        }
    } else
        itr2 = NULL;
    ignseccolumn = false;
    hnc = hn = false;
}

uint64_t ScanItr::getCardinality() {
    if (ignseccolumn) {
        return q->getNFirstTablesPerPartition(idx);
    } else {
        return q->getInputSize();
    }
}

uint64_t ScanItr::estCardinality() {
    if (ignseccolumn) {
        return q->getNFirstTablesPerPartition(idx);
    } else {
        return q->getInputSize();
    }
}

bool ScanItr::hasNext() {
    if (hnc)
        return hn;

    if (currentTable != NULL) {
        if (currentTable->hasNext()) {
            hnc = hn = true;
            return hn;
        } else {
            if (currentTable != m_currentTable) {
                q->releaseItr(currentTable);
            }
            currentTable = NULL;
        }
    } else if (reversedItr != NULL) {
        if (reversedItr->hasNext()) {
            hnc = hn = true;
            return hn;
        } else {
            if (reversedItr != m_reversedItr) {
                q->releaseItr(reversedItr);
            }
            reversedItr = NULL;
        }
    }

    //Move to the next key
    if (itr2) {
        if (itr2->hasNext()) {
            itr2->next();
            const int64_t key2 = itr2->getKey();
            //itr1 can be either equal, greater or being finished
            if (itr1) {
                if (itr1->getKey() < key2) {
                    if (itr1->hasNext()) {
                        itr1->next();
                    } else {
                        if (itr1 != m_itr1) {
                            q->releaseItr(itr1);
                        }
                        itr1 = NULL;
                    }
                }
            }
            hn = true;
        } else {
            hn = false;
        }
    } else {
        if (itr1->hasNext()) {
            itr1->next();
            hn = true;
        } else {
            hn = false;
        }
    }

    hnc = true;
    return hn;
}

void ScanItr::next() {
    if (currentTable == NULL) {
        if (reversedItr == NULL) {
            if (itr2 && (!itr1 || itr2->getKey() != itr1->getKey())) {
                const int64_t key = itr2->getKey();
                char strategy = itr2->getCurrentStrat();
                short file = itr2->getCurrentFile();
                int64_t mark = itr2->getCurrentMark();
                //Need to get reversed table
                PairItr *itr = q->getIterator(idx - 3, key, file, mark, strategy,
                        -1, -1, false, false);
                reversedItr = q->newItrOnReverse(itr, -1, -1);
                if (ignseccolumn)
                    reversedItr->ignoreSecondColumn();
                if (!reversedItr->hasNext()) {
                    throw 10; //should not happen
                }
                reversedItr->next();
                q->releaseItr(itr);
                setKey(key);
            } else {
                const int64_t key = itr1->getKey();
                char strategy = itr1->getCurrentStrat();
                short file = itr1->getCurrentFile();
                int64_t mark = itr1->getCurrentMark();
                currentTable = q->getIterator(idx, key, file, mark, strategy,
                        -1, -1, false, false);
                if (ignseccolumn)
                    currentTable->ignoreSecondColumn();
                setKey(key);
                if (!currentTable->hasNext()) {
                    throw 10;
                }
                currentTable->next();
            }
        } else {
            reversedItr->next();
        }
    } else {
        currentTable->next();
    }
    hnc = false;
}

bool ScanItr::next(int64_t &v1, int64_t &v2, int64_t &v3) {
    bool hasNext;

    if (currentTable == NULL) {
        if (reversedItr == NULL) {
            if (itr2 && (!itr1 || itr2->getKey() != itr1->getKey())) {
                const int64_t key = itr2->getKey();
                char strategy = itr2->getCurrentStrat();
                short file = itr2->getCurrentFile();
                int64_t mark = itr2->getCurrentMark();
                //Need to get reversed table
                PairItr *itr = q->getIterator(idx - 3, key, file, mark, strategy,
                        -1, -1, false, false);
                reversedItr = q->newItrOnReverse(itr, -1, -1);
                if (ignseccolumn)
                    reversedItr->ignoreSecondColumn();
                if (!reversedItr->hasNext()) {
                    throw 10; //should not happen
                }
                hasNext = reversedItr->next(v1, v2, v3);
                q->releaseItr(itr);
                setKey(key);
            } else {
                const int64_t key = itr1->getKey();
                char strategy = itr1->getCurrentStrat();
                short file = itr1->getCurrentFile();
                int64_t mark = itr1->getCurrentMark();
                currentTable = q->getIterator(idx, key, file, mark, strategy,
                        -1, -1, false, false);
                if (ignseccolumn)
                    currentTable->ignoreSecondColumn();
                setKey(key);
                if (!currentTable->hasNext()) {
                    throw 10;
                }
                hasNext = currentTable->next(v1, v2, v3);
            }
        } else {
            hasNext = reversedItr->next(v1, v2, v3);
        }
    } else {
        hasNext = currentTable->next(v1, v2, v3);
    }

    if (!hasNext) {
        hnc = false;
        if (currentTable) {
            if (currentTable != m_currentTable) {
                q->releaseItr(currentTable);
            }
            currentTable = NULL;
        } else if (reversedItr) {
            if (reversedItr != m_reversedItr) {
                q->releaseItr(reversedItr);
            }
            reversedItr = NULL;
        }
        hasNext = this->hasNext();
    }

    hnc = true;
    hn = hasNext;
    return hasNext;
}

void ScanItr::clear() {
    if (m_currentTable) {
        q->releaseItr(m_currentTable);
    }

    if (currentTable && currentTable != m_currentTable) {
        q->releaseItr(currentTable);
    }

    if (m_itr1) {
        q->releaseItr(m_itr1);
    }

    if (itr1 && itr1 != m_itr1) {
        q->releaseItr(itr1);
    }

    if (m_itr2) {
        q->releaseItr(m_itr2);
    }

    if (itr2 && itr2 != m_itr2) {
        q->releaseItr(itr2);
    }

    if (m_reversedItr) {
        q->releaseItr(m_reversedItr);
    }

    if (reversedItr && reversedItr != m_reversedItr) {
        q->releaseItr(reversedItr);
    }
}

void ScanItr::mark() {
    m_hnc = hnc;
    m_hn = hn;
    if (itr1) {
        itr1->mark();
        m_itr1 = itr1;
    }

    if (itr2) {
        itr2->mark();
        m_itr2 = itr2;
    }

    if (currentTable) {
        currentTable->mark();
        m_currentTable = currentTable;
    }

    if (reversedItr) {
        reversedItr->mark();
        m_reversedItr = reversedItr;
    }
    m_key = getKey();
}

void ScanItr::reset(const char i) {
    hnc = m_hnc;
    hn = m_hn;

    if (itr1 != m_itr1) {
        if (itr1)
            q->releaseItr(itr1);
        itr1 = m_itr1;
    }
    if (itr1)
        itr1->reset(i);

    if (itr2 != m_itr2) {
        if (itr2)
            q->releaseItr(itr2);
        itr2 = m_itr2;
    }
    if (itr2)
        itr2->reset(i);

    if (currentTable != m_currentTable) {
        if (currentTable)
            q->releaseItr(currentTable);
        currentTable = m_currentTable;
    }
    if (currentTable) {
        currentTable->reset(i);
    }

    if (reversedItr != m_reversedItr) {
        if (reversedItr)
            q->releaseItr(reversedItr);
        reversedItr = m_reversedItr;
    }
    if (reversedItr) {
        reversedItr->reset(i);
    }
    setKey(m_key);
}

void ScanItr::gotoKey(int64_t k) {
    if (k > getKey()) {
        if (currentTable) {
            //release it
            if (currentTable != m_currentTable) {
                q->releaseItr(currentTable);
            }
            currentTable = NULL;
        }
        if (reversedItr) {
            if (reversedItr != m_reversedItr) {
                q->releaseItr(reversedItr);
            }
            reversedItr = NULL;
        }

        //Move forward
        if (itr2) {
            itr2->gotoKey(k);
            //I do not advance this iteration because it will be increased
            //the first time we call the hasNext() method.
        }
        if (itr1) {
            itr1->gotoKey(k);
        }
    }
}

void ScanItr::moveto(const int64_t c1, const int64_t c2) {
    assert(reversedItr != NULL || currentTable != NULL);
    if (reversedItr) {
        reversedItr->moveto(c1, c2);
    } else {
        currentTable->moveto(c1, c2);
    }

}
