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
#include <trident/kb/partial.h>
#include <fstream>

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

bool ReOrderItr::varFirst() {
    bool vf = array.size() > 1;
    if (vf) {
        return vf;
    }
    switch(idx) {
    case IDX_SPO:
    case IDX_SOP:
        if (s < 0) {
            vf = true;
        }
        break;
    case IDX_PSO:
    case IDX_POS:
        if (p < 0) {
            vf = true;
        }
        break;
    case IDX_OPS:
    case IDX_OSP:
        if (o < 0) {
            vf = true;
        }
        break;
    }
    return vf;
}


ReOrderItr::ReOrderItr(Querier *q, int idx, int64_t s, int64_t p, int64_t o, std::fstream &data, uint64_t offset) {
    initializeConstraints();
    this->q = q;
    this->helper = NULL;
    this->idx = idx;
    this->nextKeyIndex = 0;
    this->itr = this->m_itr = NULL;
    this->initialized = true;
    this->ignSecondColumn = false;
    this->s = s;
    this->p = p;
    this->o = o;
    data.seekg(offset, ios::beg);
    bool vf = varFirst();
    uint64_t sz = 1;
    if (vf) {
        data.read(reinterpret_cast<char*>(&sz), sizeof(uint64_t));
    }
    for (uint64_t i = 0; i < sz; i++) {
        uint64_t key;
        uint64_t npairs;
        data.read(reinterpret_cast<char*>(&key), sizeof(uint64_t));
        data.read(reinterpret_cast<char*>(&npairs), sizeof(uint64_t));
        std::shared_ptr<Pairs> tmpVector = std::shared_ptr<Pairs>(new Pairs());
        for (size_t j = 0; j < npairs; j++) {
            uint64_t v1;
            uint64_t v2;
            data.read(reinterpret_cast<char*>(&v1), sizeof(uint64_t));
            data.read(reinterpret_cast<char*>(&v2), sizeof(uint64_t));
            tmpVector->push_back(std::pair<uint64_t, uint64_t>(v1, v2));
        }
        sorted.push_back(true);
        array.push_back(std::make_pair(key, tmpVector));
    }
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

void ReOrderItr::gotoKey(int64_t newkey) {
    if (newkey <= key) {
	return;
    }
    while (nextKeyIndex < array.size()) {
	key = array[nextKeyIndex].first;
	if (key >= newkey) {
	    break;
	}
	nextKeyIndex++;
    }
    if (nextKeyIndex < array.size()) {
	if (itr != NULL && itr != m_itr) {
	    itr->clear();
	    delete itr;
	}
	itr = NULL;
    }
    hasNext();
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

void ReOrderItr::dump(ofstream &indexStream, fstream &dataStream, STripleMap &index) {
    // Two files per idx:
    // One mapping s, p, o to file index
    // and one containing the dump of the entries.

    STriple t(s, p, o);

    if (index.find(t) != index.end()) {
        return;
    }
    hasNext();  // To force initialization

    // append s, p, o, file index to index file.

    indexStream.write(reinterpret_cast<char*>(&s), sizeof(int64_t));
    indexStream.write(reinterpret_cast<char*>(&p), sizeof(int64_t));
    indexStream.write(reinterpret_cast<char*>(&o), sizeof(int64_t));
    dataStream.seekg(0, ios::end);
    uint64_t pos = dataStream.tellp();
    indexStream.write(reinterpret_cast<char*>(&pos), sizeof(uint64_t));

    // Hashtable entry for s,p,o
    // maps s,p,o to file index.
    index[t] = pos;
    
    // Number of keys now in array.size().
    // dump number of keys
    // for each key:
    // - dump key value
    // - dump number of pairs
    // - dump pairs

    bool vf = varFirst();

    if (vf) {
        // Only dump number of keys if key is variable.
        uint64_t v = array.size();
        dataStream.write(reinterpret_cast<char*>(&v), sizeof(uint64_t));
    }
    for (int i = 0; i < array.size(); i++) {
        if (vf) {
            if (idx == IDX_SPO || idx == IDX_SOP) {
                indexStream.write(reinterpret_cast<char*>(&array[i].first), sizeof(uint64_t));
                t.s = array[i].first;
            } else {
                indexStream.write(reinterpret_cast<char*>(&s), sizeof(int64_t));
            }
            if (idx == IDX_PSO || idx == IDX_POS) {
                indexStream.write(reinterpret_cast<char*>(&array[i].first), sizeof(uint64_t));
                t.p = array[i].first;
            } else {
                indexStream.write(reinterpret_cast<char*>(&p), sizeof(int64_t));
            }
            if (idx == IDX_OPS || idx == IDX_OSP) {
                indexStream.write(reinterpret_cast<char*>(&array[i].first), sizeof(uint64_t));
                t.o = array[i].first;
            } else {
                indexStream.write(reinterpret_cast<char*>(&o), sizeof(int64_t));
            }
            uint64_t pos = dataStream.tellp();
            indexStream.write(reinterpret_cast<char*>(&pos), sizeof(uint64_t));
            index[t] = pos;
        }
        dataStream.write(reinterpret_cast<char*>(&array[i].first), sizeof(uint64_t));
        auto el = array[i].second;
        if (! sorted[i]) {
            std::sort(el->begin(), el->end());
            sorted[i] = true;
        }
        uint64_t v = el->size();
        dataStream.write(reinterpret_cast<char*>(&v), sizeof(uint64_t));
        for (uint64_t j = 0; j < v; j++) {
            std::pair<uint64_t, uint64_t> p = el->at(j);
            dataStream.write(reinterpret_cast<char*>(&p.first), sizeof(uint64_t));
            dataStream.write(reinterpret_cast<char*>(&p.second), sizeof(uint64_t));
        }
    }
    dataStream.flush();
    indexStream.flush();
}
