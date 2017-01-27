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


#include <trident/iterators/termitr.h>
#include <trident/tree/treeitr.h>
#include <trident/tree/root.h>
#include <trident/tree/coordinates.h>

void TermItr::init(TableStorage *tables, uint64_t size, int perm, Root *tree) {
    this->tables = tables;
    this->nfiles = tables->getLastCreatedFile() + 1;
    this->currentfile = 0;
    this->currentMark = -1;
    this->size = size;
    this->perm = perm;
    this->tree = tree;

    this->buffer = tables->getBeginTableCoordinates(currentfile);
    this->endbuffer = tables->getEndTableCoordinates(currentfile);
}

bool TermItr::hasNext() {
    if (buffer == endbuffer) {
        currentfile++;
        currentMark = -1;
        if (currentfile >= nfiles)
            return false;
        if (tables->doesFileHaveCoordinates(currentfile)) {
            this->buffer = tables->getBeginTableCoordinates(currentfile);
            this->endbuffer = tables->getEndTableCoordinates(currentfile);
        } else {
            this->buffer = NULL;
            this->endbuffer = NULL;
            return hasNext();
        }
    }
    return true;
}

void TermItr::next() {
    buffer += 5;
    const long key = Utils::decode_longFixedBytes(buffer, 5);
    setKey(key);
    buffer += 6;
    currentMark++;
}

char TermItr::getCurrentStrat() {
    return *(buffer - 1);
}

void TermItr::clear() {
    tables = NULL;
    buffer = endbuffer = NULL;
}

uint64_t TermItr::getCardinality() {
    return size;
}

uint64_t TermItr::estCardinality() {
    throw 10; //not supported
}

void TermItr::mark() {
    throw 10; //not implemented
}

void TermItr::reset(char i) {
    throw 10; //not implemented
}

void TermItr::gotoKey(long keyToSearch) {
    //Is the file within the file range?
    const long lastKey = Utils::decode_longFixedBytes(endbuffer - 6, 5);
    if (keyToSearch > lastKey) {
        if (currentfile < nfiles - 1) {
            int nextfile = currentfile + 1;
            bool ok = true;
            while (!tables->doesFileHaveCoordinates(nextfile)) {
                if (nextfile < nfiles - 1) {
                    nextfile++;
                } else {
                    ok = false;
                    break;
                }
            }
            if (ok) {
                currentfile = nextfile;
                this->buffer = tables->getBeginTableCoordinates(currentfile);
                this->endbuffer = tables->getEndTableCoordinates(currentfile);
                //read the first key
                currentMark = -1;
                //setKey(Utils::decode_long(buffer, 2));
                gotoKey(keyToSearch);
                return;
            }
        }
        //Move to the very last position
        const size_t elemsAhead = (endbuffer - buffer) / 11;
        buffer = endbuffer - 11;
        currentMark += elemsAhead;
        setKey(lastKey);
        return;
    }

    //Binary search
    const char* start = buffer;
    const char* end = endbuffer;
    while (start < end) {
        const size_t nels = (end - start) / 11;
        const char *middle = start + (nels / 2) * 11;
        const long k = Utils::decode_long(middle + 5);
        if (k < keyToSearch) {
            start = middle + 11;
        } else if (k > keyToSearch) {
            end = middle;
        } else {
            start = middle;
            currentMark += (start - buffer) / 11;
            buffer = start;
            return;
        }
    }
    currentMark += (start - buffer) / 11;
    buffer = end;
}

long TermItr::getCount() {
    TermCoordinates values;
    bool resp = tree->get(key, &values);
    if (!resp) {
        BOOST_LOG_TRIVIAL(error) << "Every key should be in the tree...";
        throw 10;
    }
    if (perm > 2) {
        return values.getNElements(perm - 3);
    } else {
        return values.getNElements(perm);
    }
}

void TermItr::moveto(const long c1, const long c2) {
    throw 10; //not implemented
}
