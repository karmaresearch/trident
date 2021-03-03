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

#include <trident/iterators/reorderitr.h>

void ReOrderItr::fillValue(google::dense_hash_map<uint64_t, std::shared_ptr<Pairs>> &keyToPair, uint64_t key, uint64_t v1, uint64_t v2) {

    if (constraint1 != NO_CONSTRAINT && v1 != constraint1) {
        return;
    }
    if (constraint2 != NO_CONSTRAINT && v2 != constraint2) {
        return;
    }

    auto itr = keyToPair.find(key);
    if (itr != keyToPair.end()) {
        itr->second->push_back(std::pair<uint64_t, uint64_t>(v1, v2));
    } else {
        std::shared_ptr<Pairs> tmpVector = std::shared_ptr<Pairs>(new Pairs());
        tmpVector->push_back(std::pair<uint64_t, uint64_t>(v1, v2));
        keyToPair[key] = tmpVector;
    }
}

void ReOrderItr::fillValuesOSP(google::dense_hash_map<uint64_t, std::shared_ptr<Pairs>> &keyToPair) {
    while (helper->hasNext()) {
        helper->next();
        uint64_t v1 = helper->getKey();
        uint64_t v2 = helper->getValue1();
        uint64_t key = helper->getValue2();
        if (p >= 0 && v2 != p) {
            if (v2 > p) {
                helper->gotoKey(v1+1);
            }
            continue;
        }
        if (o >= 0 && key != o) {
            /* Adding this actually makes it slower
            if (key > o) {
                helper->moveto(v2+1, o);
            }
            */
            continue;
        }
        fillValue(keyToPair, key, v1, v2);
    }
}

void ReOrderItr::fillValuesPSO(google::dense_hash_map<uint64_t, std::shared_ptr<Pairs>> &keyToPair) {
    while (helper->hasNext()) {
        helper->next();
        uint64_t v1 = helper->getKey();
        uint64_t key = helper->getValue1();
        uint64_t v2 = helper->getValue2();
        if (p >= 0 && key != p) {
            if (key > p) {
                helper->gotoKey(v1+1);
            }
            continue;
        }
        if (o >= 0 && v2 != o) {
            /* Adding this actually makes it slower
            if (v2 > o) {
                helper->moveto(key+1, o);
            }
            */
            continue;
        }

        fillValue(keyToPair, key, v1, v2);
    }
}

void ReOrderItr::fillValuesPOS(google::dense_hash_map<uint64_t, std::shared_ptr<Pairs>> &keyToPair) {
    while (helper->hasNext()) {
        helper->next();
        uint64_t v2 = helper->getKey();
        uint64_t key = helper->getValue1();
        uint64_t v1 = helper->getValue2();
        if (p >= 0 && key != p) {
            if (key > p) {
                helper->gotoKey(v2+1);
            }
            continue;
        }
        if (o >= 0 && v1 != o) {
            /* Adding this actually makes it slower
            if (v1 > o) {
                helper->moveto(key+1, o);
            }
            */
            continue;
        }

        fillValue(keyToPair, key, v1, v2);
    }
}

void ReOrderItr::fillValuesOPS(google::dense_hash_map<uint64_t, std::shared_ptr<Pairs>> &keyToPair) {
    while (helper->hasNext()) {
        helper->next();
        uint64_t v2 = helper->getKey();
        uint64_t v1 = helper->getValue1();
        uint64_t key = helper->getValue2();
        if (p >= 0 && v1 != p) {
            if (v1 > p) {
                helper->gotoKey(v2+1);
            }
            continue;
        }
        if (o >= 0 && key != o) {
            /* Adding this actually makes it slower
            if (key > o) {
                helper->moveto(v1+1, o);
            }
            */
            continue;
        }

        fillValue(keyToPair, key, v1, v2);
    }
}

static bool arraySort(KeyValue &v1, KeyValue &v2) {
    return v1.first < v2.first;
}

void ReOrderItr::fillValues() {
    google::dense_hash_map<uint64_t, std::shared_ptr<Pairs>> keyToPair;
    keyToPair.set_empty_key(0xFFFFFFFFFFFFFFFF);
    switch(idx) {
    case IDX_OSP:
        fillValuesOSP(keyToPair);
        break;
    case IDX_PSO:
        fillValuesPSO(keyToPair);
        break;
    case IDX_POS:
        fillValuesPOS(keyToPair);
        break;
    case IDX_OPS:
        fillValuesOPS(keyToPair);
        break;
    default:
        // Should not happen.
        throw 10;
    }

    for (auto v = keyToPair.begin(); v != keyToPair.end(); v++) {
        array.push_back(std::make_pair(v->first, v->second));
    }

    initialized = true;

    // Now sort array, only according to the keys. We'll sort the Pairs when needed.
    std::sort(array.begin(), array.end(), arraySort);
    sorted.resize(array.size());
}

uint64_t ReOrderItr::getCardinality() {
    if (! initialized) {
        fillValues();
    }
    size_t sz = 0;
    if (! ignSecondColumn) {
        for (size_t i = 0; i < array.size(); i++) {
            sz += array[i].second->size();
        }
        return sz;
    }
    for (size_t i = 0; i < array.size(); i++) {
        auto el = array[i].second;
        if (! sorted[i]) {
            std::sort(el->begin(), el->end());
            sorted[i] = true;
        }
        ArrayItr *it = new ArrayItr();
        it->init(el, NO_CONSTRAINT, NO_CONSTRAINT);
        it->ignoreSecondColumn();
        sz += it->getCardinality();
        it->clear();
        delete it;
    }
    return sz;
}

bool ReOrderItr::hasNext() {
    if (! initialized) {
        fillValues();
    }
    if (itr == NULL || ! itr->hasNext()) {
        while (nextKeyIndex < array.size()) {
            if (array[nextKeyIndex].second->size() > 0) {
                return true;
            }
            nextKeyIndex++;
        }
        return false;
    }
    return true;
}

void ReOrderItr::next() {
    if (itr == NULL || ! itr->hasNext()) {
        if (itr != NULL && itr != m_itr) {
            itr->clear();
        } else {
            itr = new ArrayItr();
        }
        key = array[nextKeyIndex].first;
        std::shared_ptr<Pairs> pairs = array[nextKeyIndex].second;
        if (! sorted[nextKeyIndex]) {
            std::sort(pairs->begin(), pairs->end());
            sorted[nextKeyIndex] = true;
        }

        itr->init(pairs, NO_CONSTRAINT, NO_CONSTRAINT);

        if (ignSecondColumn) {
            itr->ignoreSecondColumn();
        }

        if (! itr->hasNext()) {
            // Should not happen.
            throw 10;
        }
        nextKeyIndex++;
    }
    itr->next();
}

