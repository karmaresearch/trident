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

#include <trident/iterators/reordertermitr.h>

void ReOrderTermItr::fillValuesO(google::dense_hash_set<uint64_t> &keys) {
    while (helper->hasNext()) {
        helper->next();
        uint64_t key = helper->getValue2();
        keys.insert(key);
    }
}

void ReOrderTermItr::fillValuesP(google::dense_hash_set<uint64_t> &keys) {
    helper->ignoreSecondColumn();
    while (helper->hasNext()) {
        helper->next();
        uint64_t key = helper->getValue1();
        keys.insert(key);
    }
}

void ReOrderTermItr::fillValues() {
    google::dense_hash_set<uint64_t> keySet;
    keySet.set_empty_key(0xFFFFFFFFFFFFFFFF);
    switch(idx) {
    case IDX_OSP:
    case IDX_OPS:
        fillValuesO(keySet);
        break;
    case IDX_PSO:
    case IDX_POS:
        fillValuesP(keySet);
        break;
    default:
        // Should not happen.
        throw 10;
    }

    for (auto v = keySet.begin(); v != keySet.end(); v++) {
        keys.push_back(*v);
    }

    initialized = true;

    std::sort(keys.begin(), keys.end());
}

uint64_t ReOrderTermItr::getCardinality() {
    if (! initialized) {
        fillValues();
    }
    return keys.size();
}

bool ReOrderTermItr::hasNext() {
    if (! initialized) {
        fillValues();
    }
    return nextKeyIndex < keys.size();
}

void ReOrderTermItr::next() {
    key = keys[nextKeyIndex];
    nextKeyIndex++;
}
