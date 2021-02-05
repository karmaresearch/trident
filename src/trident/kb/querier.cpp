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


#include <trident/kb/kb.h>
#include <trident/kb/querier.h>
#include <trident/tree/root.h>
#include <trident/binarytables/tableshandler.h>
#include <trident/iterators/emptyitr.h>
#include <trident/iterators/compositetermitr.h>

#include <kognac/factory.h>

#include <iostream>
#include <inttypes.h>
#include <cmath>

using namespace std;

int PERM_SPO[] = { 0, 1, 2 };
int INV_PERM_SPO[] = { 0, 1, 2 };
int PERM_OPS[] = { 2, 1, 0 };
int INV_PERM_OPS[] = { 2, 1, 0 };
int PERM_POS[] = { 1, 2, 0 };
int INV_PERM_POS[] = { 2, 0, 1 };

int PERM_PSO[] = { 1, 0, 2 };
int INV_PERM_PSO[] = { 1, 0, 2 };
int PERM_OSP[] = { 2, 0, 1 };
int INV_PERM_OSP[] = { 1, 2, 0 };
int PERM_SOP[] = { 0, 2, 1 };
int INV_PERM_SOP[] = { 0, 2, 1 };
EmptyItr emptyItr;

Querier::Querier(Root* tree, DictMgmt *dict, TableStorage** files,
        const int64_t inputSize, const int64_t nTerms, const int nindices,
        const int64_t *nTablesPerPartition,
        const int64_t *nFirstTablesPerPartition, KB *sampleKB,
        std::vector<std::unique_ptr<DiffIndex>> &diffIndices, bool *present)
    : inputSize(inputSize), nTerms(nTerms),
    nTablesPerPartition(nTablesPerPartition),
    nFirstTablesPerPartition(nFirstTablesPerPartition),
    // nindices(nindices),
    diffIndices(diffIndices), present(present) {
        this->tree = tree;
        this->dict = dict;
        this->files = files;
        lastKeyFound = false;
        lastKeyQueried = -1;
        strat.init(/*&listFactory, &comprFactory, &list2Factory,*/ &ncFactory, &nrFactory, &ncluFactory,
                NULL, NULL, NULL, NULL, NULL, NULL);
        aggrIndices = notAggrIndices = cacheIndices = 0;
        spo = sop = pos = pso = ops = osp = 0;

        currentValue.clear();

        if (files[0]) {
            std::string pathFirstPerm = files[0]->getPath();
            pathRawData = pathFirstPerm + std::string("raw");
            copyRawData = Utils::exists(pathRawData);
        }

        if (sampleKB != NULL) {
            sampler = std::unique_ptr<Querier>(sampleKB->query());
        }
        for (int i = 0; i < diffIndices.size(); ++i) {
            DiffIndex *diff = diffIndices[i].get();
            initDiffIndex(diff);
        }
    }

void Querier::initDiffIndex(DiffIndex *diff) {
    if (diff->getClass() == DiffIndex::DIFF3) {
        ((DiffIndex3*)diff)->setStorageStrat(&strat);
    } else {
        assert(diff->getClass() == DiffIndex::DIFF1);
        ((DiffIndex1*)diff)->setFactoryIterators(&factory13);
    }

}

char Querier::getStrategy(const int idx, const int64_t v) {
    if (lastKeyQueried != v) {
        lastKeyFound = tree->get(v, &currentValue);
        lastKeyQueried = v;
    }
    return currentValue.getStrategy(idx);
}

/*uint64_t Querier::getCard(const int idx, const int64_t v) {
  if (lastKeyQueried != v) {
  lastKeyFound = tree->get(v, &currentValue);
  lastKeyQueried = v;
  }
  return currentValue.getNElements(idx);
  }*/

uint64_t Querier::getCardOnIndex(const int idx, const int64_t first, const int64_t second,
        const int64_t third, bool skipLast) {
    //Check if first element is variable
    switch (idx) {
        case IDX_SPO:
        case IDX_SOP:
            if (first < 0 && !skipLast)
                return getInputSize();
            break;
        case IDX_POS:
        case IDX_PSO:
            if (second < 0 && !skipLast)
                return getInputSize();
            break;
        case IDX_OPS:
        case IDX_OSP:
            if (third < 0 && !skipLast)
                return getInputSize();
            break;
    }

    //The first term is a constant.
    PairItr *itr = getIterator(idx, first, second, third);
    if (itr->getTypeItr() != EMPTY_ITR && itr->hasNext()) {
        int countUnbound = 0;
        if (first < 0) countUnbound++;
        if (second < 0) countUnbound++;
        if (third < 0) countUnbound++;
        if (countUnbound >= 2) {
            if (skipLast) {
                itr->ignoreSecondColumn();
                uint64_t card = itr->getCardinality();
                releaseItr(itr);
                return card;
            } else {
                releaseItr(itr);

                int64_t nElements = 0;
                int idx2 = idx;
                if (idx2 > 2)
                    idx2 -= 3;
                if (lastKeyFound && currentValue.exists(idx2)) {
                    nElements += currentValue.getNElements(idx2);
                }

                for (size_t i = 0; i < diffIndices.size(); ++i) {
                    if (diffIndices[i]->getType() == DiffIndex::TypeUpdate::ADDITION_df) {
                        nElements += diffIndices[i]->getCard(idx2, lastKeyQueried);
                    } else {
                        nElements -= diffIndices[i]->getCard(idx2, lastKeyQueried);
                    }
                }
                return nElements;
            }
        } else if (countUnbound == 1) {
            uint64_t card;
            if (skipLast) {
                card = 1;
            } else {
                card = itr->getCardinality();
            }
            releaseItr(itr);
            return card;
        } else {
            assert(countUnbound == 0);
            releaseItr(itr);
            return 1;
        }
    } else {
        //No results
        if (itr->getTypeItr() != EMPTY_ITR) {
            releaseItr(itr);
        }
        return 0;
    }
}

int64_t Querier::getCard(const int64_t s, const int64_t p, const int64_t o, uint8_t pos) {
    if (s < 0 && p < 0 && o < 0) {
        throw 10; //not supported
    }

    //At least one variable is bound. Thus "pos" must refer to another one
    int idx = getIndex(s, p, o);
    PairItr *itr = getIterator(idx, s, p, o);
    if (itr->getTypeItr() != EMPTY_ITR && itr->hasNext()) {
        int countUnbound = 0;
        if (s < 0) countUnbound++;
        if (p < 0) countUnbound++;
        if (o < 0) countUnbound++;
        if (countUnbound == 0) {
            releaseItr(itr);
            return 1;
        } else if (countUnbound == 1) {
            uint64_t card = itr->getCardinality();
            releaseItr(itr);
            return card;
        } else if (countUnbound == 2) {
            //I must count the number of first columns
            int idx2 = IDX_SPO;
            if (s >= 0) {
                if (pos == 2) {
                    idx2 = IDX_SOP;
                }
            } else if (p >= 0) {
                if (pos == 0) {
                    idx2 = IDX_PSO;
                } else {
                    idx2 = IDX_POS;
                }
            } else { //o>= 0
                assert(o >= 0);
                if (pos == 0) {
                    idx2 = IDX_OSP;
                } else {
                    idx2 = IDX_OPS;
                }
            }
            if (idx != idx2) {
                releaseItr(itr);
                itr = getIterator(idx2, s, p, o);
            }

            itr->ignoreSecondColumn();
            int64_t nElements = itr->getCardinality();
            releaseItr(itr);
            return nElements;
        } else {
            throw 10; //Should never happen
        }
    } else {
        if (itr->getTypeItr() != EMPTY_ITR)
            releaseItr(itr);
        return 0; //EMPTY_ITR. Join will fail...
    }
}

uint64_t Querier::isAggregated(const int idx, const int64_t first, const int64_t second,
        const int64_t third) {
    if (idx != IDX_POS && idx != IDX_PSO)
        return 0;

    const int64_t key = second;
    if (key >= 0) {
        if (lastKeyQueried != key) {
            lastKeyFound = tree->get(key, &currentValue);
            lastKeyQueried = key;
        }
        if (currentValue.exists(idx)) {
            if (StorageStrat::isAggregated(currentValue.getStrategy(idx))) {
                //Is the second term bound?
                if (idx == IDX_PSO && first >= 0) {
                    return 1;
                } else if (idx == IDX_POS && third >= 0) {
                    return 1;
                } else {
                    PairItr *itr = getPairIterator(&currentValue, idx, key, -1, -1,
                            true, true);
                    itr->ignoreSecondColumn();
                    uint64_t count = itr->getCount();
                    releaseItr(itr);
                    return count;
                }
            }
        }
    } else {
        //Is it all aggregated?
    }
    return 0;
}

uint64_t Querier::isReverse(const int idx, const int64_t first, const int64_t second,
        const int64_t third) {
    if (idx < 3)
        return 0;

    int64_t key1 = 0;
    switch (idx) {
        case IDX_SPO:
        case IDX_SOP:
            if (first < 0)
                return inputSize;
            key1 = first;
            break;
        case IDX_POS:
        case IDX_PSO:
            if (second < 0)
                return inputSize;
            key1 = second;
            break;
        case IDX_OPS:
        case IDX_OSP:
            if (third < 0)
                return inputSize;
            key1 = third;
            break;
    }

    //Check key
    if (lastKeyQueried != key1) {
        lastKeyFound = tree->get(key1, &currentValue);
        lastKeyQueried = key1;
    }
    if (!currentValue.exists(idx)) {
        if (currentValue.exists(idx - 3)) {
            return currentValue.getNElements(idx - 3);
        }
    }
    return 0;
}

uint64_t Querier::estCardOnIndex(const int idx, const int64_t first, const int64_t second,
        const int64_t third) {
    //Check if first element is variable
    int64_t key1, key2;
    key1 = key2 = 0;
    switch (idx) {
        case IDX_SPO:
        case IDX_SOP:
            if (first < 0)
                return getInputSize();
            key1 = first;
            if (idx == IDX_SPO) {
                key2 = second;
            } else {
                key2 = third;
            }
            break;
        case IDX_POS:
        case IDX_PSO:
            if (second < 0)
                return getInputSize();
            key1 = second;
            if (idx == IDX_POS)
                key2 = third;
            else
                key2 = first;
            break;
        case IDX_OPS:
        case IDX_OSP:
            if (third < 0)
                return getInputSize();
            key1 = third;
            if (idx == IDX_OPS)
                key2 = second;
            else
                key2 = first;
            break;
    }

    //The first term is a constant.
    int countUnbound = 0;
    if (first < 0) countUnbound++;
    if (second < 0) countUnbound++;
    if (third < 0) countUnbound++;
    if (countUnbound == 0) {
        return 1;
    } else if (countUnbound == 1 && key2 >= 0) {
        PairItr *itr = getIterator(idx, first, second, third);
        int64_t card = itr->estCardinality();
        releaseItr(itr);
        return card;
    } else {
        if (lastKeyQueried != key1) {
            lastKeyFound = tree->get(key1, &currentValue);
            lastKeyQueried = key1;
        }
        int perm = idx;
        if (perm > 2)
            perm = perm - 3;

        int64_t nElements = 0;
        if (currentValue.exists(perm)) {
            nElements += currentValue.getNElements(perm);
        }
        for (size_t i = 0; i < diffIndices.size(); ++i) {
            nElements += diffIndices[i]->getCard(perm, key1);
        }
        return nElements;
    }
}

int64_t Querier::estCard(const int64_t s, const int64_t p, const int64_t o) {
    if (s < 0 && p < 0 && o < 0) {
        //They are all variables. Return the input size...
        return getInputSize();
    }

    int countUnbound = 0;
    if (s < 0) countUnbound++;
    if (p < 0) countUnbound++;
    if (o < 0) countUnbound++;
    if (countUnbound == 0) {
        return 1;
    } else if (countUnbound == 1) {
        int perm = getIndex(s, p, o);
        PairItr *itr = getIterator(perm, s, p, o);
        int64_t card = itr->estCardinality();
        releaseItr(itr);
        return card;
    } else if (countUnbound == 2) {
        int64_t key;
        int perm = getIndex(s, p, o);
        // int perm;
        if (s >= 0) {
            key = s;
            // perm = IDX_SPO;
        } else if (p >= 0) {
            key = p;
            // perm = IDX_POS;
        } else {
            key = o;
            // perm = IDX_OPS;
        }
        if (lastKeyQueried != key) {
            lastKeyFound = tree->get(key, &currentValue);
            lastKeyQueried = key;
        }

        int64_t nElements = 0;
        if (currentValue.exists(perm)) {
            nElements += currentValue.getNElements(perm);
        }
        for (size_t i = 0; i < diffIndices.size(); ++i) {
            nElements += diffIndices[i]->getCard(perm, key);
        }
        return nElements;
    }
    throw 10;
}

int64_t Querier::getCard(const int64_t s, const int64_t p, const int64_t o) {
    if (s < 0 && p < 0 && o < 0) {
        //They are all variables. Return the input size...
        return getInputSize();
    }
    int idx = getIndex(s, p, o);

    int countUnbound = 0;
    if (s < 0) countUnbound++;
    if (p < 0) countUnbound++;
    if (o < 0) countUnbound++;
    if (countUnbound == 2) {
        //Get the key
        int64_t key;
        if (s >= 0)
            key = s;
        else if (p >= 0)
            key = p;
        else
            key = o;
        if (lastKeyQueried != key) {
            lastKeyFound = tree->get(key, &currentValue);
            lastKeyQueried = key;
        }
        int64_t nElements = 0;
        if (lastKeyFound && currentValue.exists(idx)) {
            nElements += currentValue.getNElements(idx);
        }
        for (size_t i = 0; i < diffIndices.size(); ++i) {
            nElements += diffIndices[i]->getCard(idx, key);
        }
        return nElements;
    }

    //Two or three constants
    PairItr *itr = getIterator(idx, s, p, o);
    if (itr->getTypeItr() != EMPTY_ITR && itr->hasNext()) {
        if (countUnbound == 0) {
            releaseItr(itr);
            return 1;
        }
        itr->next();
        uint64_t card = itr->getCardinality();
        releaseItr(itr);
        return card;
    } else {
        if (itr->getTypeItr() != EMPTY_ITR)
            releaseItr(itr);
        return 0; //EMPTY_ITR. Join will fail...
    }
}

//Check whether a triple exists
bool Querier::exists(const int64_t s, const int64_t p, const int64_t o) {
    int idx = getIndex(s, p, o);
    PairItr *itr = getIterator(idx, s, p, o);
    //Use the POS index ... but we don't know that it is present.
    // PairItr *itr = get(IDX_POS, s, p, o);
    if (itr->getTypeItr() != EMPTY_ITR) {
        bool resp = itr->hasNext();
        releaseItr(itr);
        return resp;
    } else {
        return false;
    }

}

bool Querier::isEmpty(const int64_t s, const int64_t p, const int64_t o) {
    if (s < 0 && p < 0 && o < 0) {
        //They are all variables. Return the input size...
        if (inputSize != 0) {
            return false;
        }
        if (diffIndices.size() > 0) {
            for (size_t i = 0; i < diffIndices.size(); ++i) {
                if (diffIndices[i]->getSize() > 0) {
                    return false;
                }
            }
        }
        return true;
    }

    int idx = getIndex(s, p, o);
    PairItr *itr = getIterator(idx, s, p, o);
    if (itr->getTypeItr() != EMPTY_ITR) {
        const bool resp = itr->hasNext();
        releaseItr(itr);
        return !resp;
    } else {
        return true;
    }
}

// Note: getIndex may return an index that is actually not present.
int Querier::getIndex(const int64_t s, const int64_t p, const int64_t o) {

    if (s >= 0) {
        //SPO or SOP
        if (p >= 0 || (p == -2 && o < 0)) {
            return IDX_SPO;
        } else {
            return IDX_SOP;
        }
    }

    if (o >= 0) {
        //OPS or OSP.
        //We know that s < 0 here.
        if (p >= 0 || p == -2) {
            return IDX_OPS;
        } else {
            return IDX_OSP;
        }
    }

    if (p >= 0) {
        //POS or PSO
        if (o >= 0 || o == -2) {
            return IDX_POS;
        } else {
            return IDX_PSO;
        }
    }

    //No constant on the pattern. Either they are all -1, or there is a join
    if (s == -2) {
        if (p != -1 || o == -1) {
            return IDX_SPO;
        } else {
            return IDX_SOP;
        }
    }

    if (o == -2) {
        if (p != -1 || s == -1) {
            return IDX_OPS;
        } else {
            return IDX_OSP;
        }
    }

    if (p == -2) {
        if (o != -1 || s == -1) {
            return IDX_POS;
        } else {
            return IDX_PSO;
        }
    }

    //Scan
    return IDX_SPO;
}

void Querier::initNewIterator(TableStorage *storage,
        int file,
        int64_t mark,
        PairItr *t,
        int64_t v1,
        int64_t v2,
        const bool setConstraints) {
    std::pair<const char*, const char*> coord = storage->getTable(file, mark);

    assert(t->getTypeItr() == NEWROW_ITR || t->getTypeItr() == NEWCLUSTER_ITR
            || t->getTypeItr() == NEWCOLUMN_ITR);
    if (v1 != -1) {
        if (setConstraints) {
            if (v2 == -1) {
                ((AbsNewTable*)t)->setup(v1, coord.first, coord.second);
            } else {
                ((AbsNewTable*)t)->setup(v1, v2, coord.first, coord.second);
                t->setConstraint2(v2);
            }
        } else {
            ((AbsNewTable*)t)->setup(coord.first, coord.second);
            if (v2 != -1) {
                if (t->hasNext()) {
                    t->next();
                    t->moveto(v1, v2);
                }
            } else {
                if (t->hasNext()) {
                    t->next();
                    t->moveto(v1, 0);
                }
            }
        }
        t->setConstraint1(v1);
    } else {
        ((AbsNewTable*)t)->setup(coord.first, coord.second);
        t->setConstraint1(-1);
    }

    if (v2 == -1)
        t->setConstraint2(-1);
}

PairItr *Querier::getPermuted(const int idx, const int64_t el1, const int64_t el2,
        const int64_t el3, const bool constrain) {
    switch (idx) {
        case IDX_SPO:
            return getIterator(idx, el1, el2, el3, constrain);
        case IDX_SOP:
            return getIterator(idx, el1, el3, el2, constrain);
        case IDX_POS:
            return getIterator(idx, el3, el1, el2, constrain);
        case IDX_PSO:
            return getIterator(idx, el2, el1, el3, constrain);
        case IDX_OSP:
            return getIterator(idx, el2, el3, el1, constrain);
        case IDX_OPS:
            return getIterator(idx, el3, el2, el1, constrain);
    }
    LOG(ERRORL) << "Idx " << idx << " not known";
    throw 10;
}

PairItr *Querier::getTermList(const int perm) {
    PairItr *finalItr = getKBTermList(perm, false);

    //Add differential updates
    for (size_t i = 0; i < diffIndices.size(); ++i) {
        if (diffIndices[i]->getNUniqueKeys(perm) > 0) {
            if (diffIndices[i]->getType() == DiffIndex::TypeUpdate::ADDITION_df) {
                DiffTermItr *itr = factory10.get();
                diffIndices[i]->getTermListItr(perm, itr);
                if (finalItr->getTypeItr() != COMPOSITETERM_ITR) {
                    CompositeTermItr *newitr = factory9.get();
                    newitr->init(); //Initialize a compositetermitr
                    newitr->add(finalItr);
                    newitr->add(itr);
                    finalItr = newitr;
                } else {
                    ((CompositeTermItr*)finalItr)->add(itr);
                }
            } else {
                DiffTermItr *itr = factory10.get();
                diffIndices[i]->getTermListItr(IDX_SPO, itr);
                RmCompositeTermItr *newitr = factory15.get();
                newitr->init(finalItr, itr);
                finalItr = newitr;
            }
        }
    }

    return finalItr;
}

PairItr *Querier::summaryAddDiff() {
    PairItr *p = summaryDiff(IDX_SPO, DiffIndex::TypeUpdate::ADDITION_df);
    if (p == NULL) {
        return p;
    }
    PairItr *m = summaryDiff(IDX_SPO, DiffIndex::TypeUpdate::DELETE_df);
    if (m == NULL) {
        return p;
    }
    RmItr *newitr = factory14.get();
    newitr->init(p, m, 0);
    return newitr;
}

PairItr *Querier::summaryRmDiff() {
    PairItr *m = summaryDiff(IDX_SPO, DiffIndex::TypeUpdate::DELETE_df);
    if (m == NULL) {
        return m;
    }
    PairItr *p = summaryDiff(IDX_SPO, DiffIndex::TypeUpdate::ADDITION_df);
    if (p == NULL) {
        return m;
    }
    RmItr *newitr = factory14.get();
    newitr->init(m, p, 0);
    return newitr;
}

PairItr *Querier::summaryDiff(const int perm, DiffIndex::TypeUpdate tp) {

    PairItr *finalItr = NULL;

    //Add differential updates
    for (size_t i = 0; i < diffIndices.size(); ++i) {
        if (diffIndices[i]->getNUniqueKeys(perm) > 0) {
            LOG(DEBUGL) << "diffIndices " << i;
            if (diffIndices[i]->getType() == tp) {
                DiffScanItr *itr = factory11.get();
                itr->setQuerier(this);
                PairItr *it = ((DiffIndex3*)diffIndices[i].get())->getScan(perm, itr);
                LOG(DEBUGL) << "Adding iterator, hasNext = " << it->hasNext();
                if (finalItr == NULL) {
                    finalItr = it;
                } else if (finalItr->getTypeItr() != COMPOSITESCAN_ITR) {
                    CompositeScanItr *newitr = factory12.get();
                    newitr->setQuerier(this);
                    newitr->init(perm); //Initialize a compositescanitr
                    newitr->addChild(finalItr);
                    newitr->addChild(it);
                    LOG(DEBUGL) << "CompositeScanItr, hasNext = " << newitr->hasNext();
                    finalItr = newitr;
                } else {
                    ((CompositeScanItr*)finalItr)->addChild(itr);
                }
            } else {
                if (finalItr == NULL) {
                    continue;
                }
                DiffScanItr *itr = factory11.get();
                itr->setQuerier(this);
                PairItr *it = ((DiffIndex3*)diffIndices[i].get())->getScan(perm, itr);
                RmItr *newitr = factory14.get();
                newitr->init(finalItr, it, 0);
                finalItr = newitr;
            }
        }
    }

    return finalItr;
}

bool Querier::existKey(int perm, int64_t key) {
    PairItr *itr = getPermuted(perm, key, -1, -1, true);
    bool resp;
    if (itr->hasNext()) {
        resp = true;
    } else {
        resp = false;
    }
    releaseItr(itr);
    return resp;
}

TermItr *Querier::getKBTermList(const int perm, const bool enforcePerm) {
    TableStorage *storage = files[perm];
    if (storage == NULL) {
        if (! enforcePerm) {
            if (perm > 2 && files[perm-3] != NULL) {
                storage = files[perm - 3];
            } else if (perm < 3 && files[perm+3] != NULL) {
                storage = files[perm + 3];
            }
        }
    }
    if (storage != NULL) {
        TermItr *itr = factory7.get();
        itr->init(storage, nTablesPerPartition[perm], perm, tree);
        return itr;
    } else {
        return NULL;
    }
}

PairItr *Querier::get(const int idx, TermCoordinates &value,
        const int64_t key, const int64_t v1,
        const int64_t v2, const bool cons) {

    if (value.exists(idx)) {
        if (StorageStrat::isAggregated(value.getStrategy(idx))) {
            aggrIndices++;
            AggrItr *itr = factory4.get();
            itr->init(idx, getPairIterator(&value, idx, key, v1, -1,
                        true, true), this);
            itr->setKey(key);
            if (v2 != -1) {
                if (itr->hasNext()) {
                    itr->setConstraint2(v2);
                    itr->moveto(v1, v2);
                }
            }
            return itr;
        } else {
            notAggrIndices++;
            PairItr *itr = getPairIterator(&value, idx, key, v1, v2, cons,
                    false);
            return itr;
        }
    } else if (idx >= 3 && value.exists(idx - 3)) {
        PairItr *itr = get(idx - 3, value, key, -1, -1, cons);
        PairItr *itr2 = newItrOnReverse(itr, v1, v2);
        itr2->setKey(key);
        releaseItr(itr);
        return itr2;
    } else if (idx < 3 && value.exists(idx + 3)) {
        PairItr *itr = get(idx + 3, value, key, -1, -1, cons);
        PairItr *itr2 = newItrOnReverse(itr, v1, v2);
        itr2->setKey(key);
        releaseItr(itr);
        return itr2;
    } else {
        return &emptyItr;
    }
}

/*int64_t Querier::getValue1AtRow(const int perm,
  const short fileIdx,
  const int mark,
  const char strategy,
  const int64_t rowId) {
  std::pair<const char*, const char*> coord = files[perm]->getTable(fileIdx, mark);

  const int storageType = StorageStrat::getStorageType(strategy);
  if (storageType == NEWCOLUMN_ITR) {
  return NewColumnTable::s_getValue1AtRow(coord.first, rowId);
  } else if (storageType == NEWROW_ITR) {
  const char nbytes1 = (strategy >> 3) & 3;
  const char nbytes2 = (strategy >> 1) & 3;
  return FactoryNewRowTable::getValue1AtRow(nbytes1, nbytes2,
  coord.first, rowId);
  } else if(storageType == NEWCLUSTER_ITR) {
  const char nbytes1 = (strategy >> 3) & 3;
  const char nbytes2 = (strategy >> 1) & 3;
  char ncount = 1;
  if (strategy & 1)
  ncount = 4;
  return FactoryNewClusterTable::getValue1AtRow(nbytes1, nbytes2, ncount,
  coord.first, rowId);
  } else {
  throw 10;
  }
  }*/

const char *Querier::getTable(const int perm,
        const short fileIdx,
        const int64_t mark) {
    std::pair<const char*, const char*> coord = files[perm]->getTable(fileIdx, mark);
    return coord.first;
}

PairItr *Querier::getIterator(const int perm,
        const int64_t key,
        const short fileIdx,
        const int64_t mark,
        const char strategy,
        const int64_t v1,
        const int64_t v2,
        const bool constrain,
        const bool noAggr) {
    PairItr *itr = strat.getBinaryTable(strategy);
    initNewIterator(files[perm], fileIdx, mark, (PairItr*) itr, v1, v2, constrain);
    if (StorageStrat::isAggregated(strategy) && !noAggr) {
        AggrItr *itr2 = factory4.get();
        itr2->init(perm, itr, this);
        itr2->setKey(key);
        return itr2;
    } else {
        itr->setKey(key);
        return itr;
    }
}


PairItr *Querier::getIterator(const int idx, const int64_t s, const int64_t p, const int64_t o,
        const bool cons) {
    PairItr *out = NULL;
    int64_t first, second, third;
    first = second = third = 0;
    switch (idx) {
        case IDX_SPO:
            spo++;
            first = s;
            second = p;
            third = o;
            break;
        case IDX_OPS:
            ops++;
            first = o;
            second = p;
            third = s;
            break;
        case IDX_POS:
            pos++;
            first = p;
            second = o;
            third = s;
            break;
        case IDX_SOP:
            sop++;
            first = s;
            second = o;
            third = p;
            break;
        case IDX_OSP:
            osp++;
            first = o;
            second = s;
            third = p;
            break;
        case IDX_PSO:
            pso++;
            first = p;
            second = s;
            third = o;
            break;
    }

    if (present[idx] || (idx >= 3 && present[idx - 3]) || (idx < 3 && present[idx + 3])) {
        if (first >= 0) {
            if (lastKeyQueried != first) {
                lastKeyFound = tree->get(first, &currentValue);
                lastKeyQueried = first;
            }
            if (lastKeyFound) {
                out = get(idx, currentValue, first, second, third, cons);
            } else {
                out = &emptyItr;
            }
        } else {
            ScanItr *itr = factory3.get();
            itr->init(idx, this);
            out = itr;
        }
    } else {
        // TODO!
        throw 10;
    }

    if (!diffIndices.empty()) {

#ifdef MT
        LOG(DEBUGL) << "The program is compiled with support to multithread, but additional indices are run sequentially.";
#endif

        std::vector<PairItr*> iterators;
        int64_t nfirstterms = 0;
        for (int i = 0; i < diffIndices.size(); ++i) {
            PairItr *diffItr = NULL;
            if (diffIndices[i]->getType() == DiffIndex::TypeUpdate::DELETE_df) {
                int64_t delnfirstterms = 0;
                if (first >= 0) {
                    diffItr = diffIndices[i]->getIterator(idx, first, second,
                            third, delnfirstterms);
                } else {
                    DiffScanItr *newitr = factory11.get();
                    newitr->setQuerier(this);
                    diffItr = ((DiffIndex3*)diffIndices[i].get())->getScan(idx, newitr);
                }
                if (diffItr->hasNext()) {
                    if (out->hasNext()) {
                        iterators.push_back(out);
                    } else {
                        releaseItr(out);
                    }
                    assert(!iterators.empty());
                    //Merge all of them in a rmitr...
                    PairItr *mainitr;
                    if (iterators.size() > 1) {
                        if (first >= 0) {
                            CompositeItr *itr = factory8.get();
                            itr->init(iterators, nfirstterms, out);
                            itr->setKey(first);
                            mainitr = itr;
                        } else {
                            CompositeScanItr *itr = factory12.get();
                            itr->init(idx);
                            itr->setQuerier(this);
                            for (int i = 0; i < iterators.size(); ++i) {
                                itr->addChild(iterators[i]);
                            }
                            mainitr = itr;
                        }
                    } else {
                        mainitr = iterators[0];
                    }
                    nfirstterms = 0;
                    iterators.clear();
                    RmItr *rmitr = factory14.get();
                    rmitr->init(mainitr, diffItr, delnfirstterms);
                    out = rmitr;
                } else {
                    releaseItr(diffItr);
                }
                diffItr = NULL;
            } else {
                if (first >= 0) {
                    diffItr = diffIndices[i]->getIterator(idx, first, second,
                            third, nfirstterms);
                    if (second >= 0) {
                        //I must change nfirstterms, which can be either 1 or 0
                        //(depending if the second term is already existing on the KB).
                        //The previous methods always returns one, so I must decrease
                        //if it already exists
                        if (out->hasNext()) { //I check whether the KB is non-emtpy
                            nfirstterms--;
                        } else {
                            for (int j = 0; j < ((int)iterators.size() - 1); ++j) {
                                if (iterators[j]->hasNext()) {
                                    nfirstterms--;
                                    break;
                                }
                            }
                        }
                    }
                } else {
                    if (diffIndices[i]->getClass() == DiffIndex::DIFF3) {
                        DiffScanItr *newitr = factory11.get();
                        newitr->setQuerier(this);
                        diffItr = ((DiffIndex3*)diffIndices[i].get())->getScan(idx, newitr);
                    } else {
                        //Diff1 update getIterator() method can handle scans
                        diffItr = diffIndices[i]->getIterator(idx, first, second, third, nfirstterms);
                    }
                }
                if (diffItr->hasNext()) {
                    iterators.push_back(diffItr);
                } else {
                    releaseItr(diffItr);
                }
            }
        }
        if (!iterators.empty()) {
            if (out->hasNext()) {
                iterators.push_back(out);
            } else {
                releaseItr(out);
            }
            if (iterators.size() > 1) {
                //Return a composite iterator
                if (first >= 0) {
                    CompositeItr *itr = factory8.get();
                    itr->init(iterators, nfirstterms, out);
                    itr->setKey(first);
                    return itr;
                } else {
                    CompositeScanItr *itr = factory12.get();
                    itr->init(idx);
                    itr->setQuerier(this);
                    for (int i = 0; i < iterators.size(); ++i) {
                        itr->addChild(iterators[i]);
                    }
                    return itr;
                }
            } else {
                return iterators[0];
            }
        }
    }

    return out;
}

PairItr *Querier::newItrOnReverse(PairItr * oldItr, const int64_t v1, const int64_t v2) {
    std::shared_ptr<Pairs> tmpVector = std::shared_ptr<Pairs>(new Pairs());
    while (oldItr->hasNext()) {
        oldItr->next();
        if (v1 < 0 || oldItr->getValue2() == v1) {
            tmpVector->push_back(
                    std::pair<uint64_t, uint64_t>(oldItr->getValue2(),
                        oldItr->getValue1()));
        }
    }

    if (tmpVector->size() > 0) {
        std::sort(tmpVector->begin(), tmpVector->end());
        ArrayItr *itr = factory2.get();
        itr->init(tmpVector, v1, v2);
        return itr;
    } else {
        return &emptyItr;
    }
}

int *Querier::getOrder(int idx) {
    switch (idx) {
        case IDX_SPO:
            return PERM_SPO;
        case IDX_OPS:
            return PERM_OPS;
        case IDX_POS:
            return PERM_POS;
        case IDX_OSP:
            return PERM_OSP;
        case IDX_PSO:
            return PERM_PSO;
        case IDX_SOP:
            return PERM_SOP;
    }
    return NULL;
}

int *Querier::getInvOrder(int idx) {
    switch (idx) {
        case IDX_SPO:
            return INV_PERM_SPO;
        case IDX_OPS:
            return INV_PERM_OPS;
        case IDX_POS:
            return INV_PERM_POS;
        case IDX_OSP:
            return INV_PERM_OSP;
        case IDX_PSO:
            return INV_PERM_PSO;
        case IDX_SOP:
            return INV_PERM_SOP;
    }
    return NULL;
}

void Querier::releaseItr(PairItr * itr) {
    AggrItr *citr;
    switch (itr->getTypeItr()) {
        case NEWCOLUMN_ITR:
            ncFactory.release((NewColumnTable *) itr);
            break;
        case NEWROW_ITR:
            nrFactory.release((AbsNewTable *) itr);
            break;
        case NEWCLUSTER_ITR:
            ncluFactory.release((AbsNewTable *) itr);
            break;
        case ARRAY_ITR:
            itr->clear();
            factory2.release((ArrayItr*) itr);
            break;
        case SCAN_ITR:
            itr->clear();
            factory3.release((ScanItr*) itr);
            break;
        case COMPOSITESCAN_ITR:
            itr->clear();
            factory12.release((CompositeScanItr*) itr);
            break;
        case CACHE_ITR:
            itr->clear();
            factory5.release((CacheItr*)itr);
            break;
        case DIFFSCAN_ITR:
            itr->clear();
            factory11.release((DiffScanItr*)itr);
            break;
        case TERM_ITR:
            factory7.release((TermItr*)itr);
            break;
        case DIFFTERM_ITR:
            factory10.release((DiffTermItr*)itr);
            break;
        case DIFF1_ITR:
            factory13.release((Diff1Itr*)itr);
            break;
        case AGGR_ITR:
            citr = (AggrItr*) itr;
            if (citr->getMainItr() != NULL)
                releaseItr(citr->getMainItr());
            if (citr->getSecondItr() != NULL)
                releaseItr(citr->getSecondItr());
            citr->clear();
            factory4.release(citr);
            break;
        case RM_ITR:
            releaseItr(((RmItr*)itr)->getMainItr());
            releaseItr(((RmItr*)itr)->getRmItr());
            factory14.release((RmItr*)itr);
            break;
        case RMCOMPOSITETERM_ITR:
            releaseItr(((RmCompositeTermItr*)itr)->getMainItr());
            releaseItr(((RmCompositeTermItr*)itr)->getRmItr());
            factory15.release((RmCompositeTermItr*)itr);
            break;
        case COMPOSITETERM_ITR:
        case COMPOSITE_ITR:
            std::vector<PairItr*> children;
            if (itr->getTypeItr() == COMPOSITETERM_ITR) {
                children = ((CompositeTermItr*)itr)->getChildren();
                itr->clear();
                factory9.release((CompositeTermItr*)itr);
            } else {
                children = ((CompositeItr*)itr)->getChildren();
                factory8.release((CompositeItr*)itr);
            }
            for (int i = 0; i < children.size(); ++i) {
                releaseItr(children[i]);
            }
            break;
    }
}
