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


#include <trident/iterators/difftermitr.h>
#include <trident/tree/treeitr.h>

bool DiffTermItr::hasNext() {
    switch (mode) {
    case 0:
        return itr->hasNext();
    case 1:
        return s != e;
    case 2:
        return remaining != 0;
    }
    throw 10;
}

void DiffTermItr::next() {
    switch (mode) {
    case 0:
        setKey(itr->next(&values));
        break;
    case 1:
        if (nbytes == 4) {
            setKey(Utils::decode_int(s));
            s += 4;
        } else if (nbytes == 8) {
            setKey(Utils::decode_long(s));
            s += 8;
        } else {
            throw 10;
        }
        break;
    case 2:
        setKey(constantvalue);
        if (remaining)
            remaining = 0;
        break;
    }
}

uint64_t DiffTermItr::getCardinality() {
    return nkeys;
}

uint64_t DiffTermItr::estCardinality() {
    return nkeys;
}

uint64_t DiffTermItr::getNUniqueKeys() {
    return nuniquekeys;
}

void DiffTermItr::gotoKey(long keyToSearch) {
    throw 10; //not yet implemented
}

long DiffTermItr::getCount() {
    return values.getNElements(perm);
}

void DiffTermItr::init(int perm, long nkeys, long nuniquekeys, TreeItr * itr) {
    initializeConstraints();
    this->perm = perm;
    this->nkeys = nkeys;
    this->nuniquekeys = nuniquekeys;

    this->mode = 0;
    this->itr = std::unique_ptr<TreeItr>(itr);
}

void DiffTermItr::init(int perm, long nkeys, long nuniquekeys,
          const char *array,
          const uint8_t nbytes) {
    initializeConstraints();
    this->perm = perm;
    this->nkeys = nkeys;
    this->nuniquekeys = nuniquekeys;

    this->mode = 1;
    this->s = array;
    this->e = array + nbytes * nkeys;
    this->nbytes = nbytes;
}

void DiffTermItr::init(int perm, long nkeys, long nuniquekeys, const long value) {
    initializeConstraints();
    this->perm = perm;
    this->nkeys = nkeys;
    this->nuniquekeys = nuniquekeys;

    this->mode = 2;
    this->constantvalue = value;
    this->remaining = 1;
}
