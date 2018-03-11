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


#ifndef NODE_H_
#define NODE_H_

#include <trident/tree/coordinates.h>
#include <trident/kb/consts.h>

#include <kognac/consts.h>

#include <vector>
#include <iostream>

#define STATE_MODIFIED 0
#define STATE_UNMODIFIED 1

using namespace std;

class IntermediateNode;
class TreeContext;

class Node {
private:
    int64_t id;
    IntermediateNode *parent;
    TreeContext * const context;
    int currentSize;
    bool deallocate;

    int64_t firstKey;
    int64_t *keys;
    bool consecutive;
    int consecutiveStep;
    char state;

protected:
    int pos(int64_t key);

    int pos(tTerm *key, int size);

    int64_t localSmallestNumericKey() {
        return firstKey;
    }

    int64_t localLargestNumericKey();

    tTerm *localSmallestTextualKey(int *size);

    tTerm *localLargestTextualKey(int *size);

    int64_t keyAt(int pos);

    bool shouldSplit();

    void split(Node *node);

    void removeKeyAtPos(int pos);

public:

    Node(TreeContext *c) :
        id(0), context(c), currentSize(0) {
        this->parent = NULL;
        deallocate = true;
        keys = NULL;
        firstKey = 0;
        consecutive = true;
        consecutiveStep = 1;
//      endStrings = NULL;
//      strings = NULL;
//      wStrings = NULL;
//      sStrings = 0;
        state = STATE_UNMODIFIED;
    }

    void setId(int64_t id) {
        this->id = id;
    }

    void doNotDeallocate() {
        deallocate = false;
    }

    bool shouldDeallocate() {
        return deallocate;
    }

    void setParent(IntermediateNode *p) {
        parent = p;
    }

    IntermediateNode *getParent() {
        return parent;
    }

    int getCurrentSize() {
        return currentSize;
    }

    void setCurrentSize(int size) {
        this->currentSize = size;
    }

    int64_t getId() {
        return id;
    }

    TreeContext *getContext() const {
        return context;
    }

    char getState() {
        return state;
    }

    void setState(const char state) {
        this->state = state;
    }

    void removeLastKey() {
        removeKeyAtPos(currentSize - 1);
    }

    void removeFirstKey() {
        removeKeyAtPos(0);
    }

    virtual Node *getChild(const int p) {
        return NULL;
    }

    void putkeyAt(nTerm key, int pos);

    void putkeyAt(tTerm *key, int sizeKey, int pos);

    /*** VIRTUAL METHODS ***/

    virtual bool canHaveChildren() {
        return false;
    }

    virtual Node *getChildForKey(int64_t key) {
        return NULL;
    }

    virtual int unserialize(char* bytes, int pos) = 0;

    virtual int serialize(char* bytes, int pos);

    virtual int64_t smallestNumericKey() = 0;

    virtual int64_t largestNumericKey() = 0;

    virtual tTerm *smallestTextualKey(int *size) = 0;

    virtual tTerm *largestTextualKey(int *size) = 0;

    virtual Node *getChildForKey(tTerm *key, int sizeKey) {
        return NULL;
    }

    virtual bool get(nTerm key, int64_t &coordinates) = 0;

    virtual bool get(tTerm *key, int sizeKey, nTerm *value) = 0;

    virtual bool get(nTerm key, TermCoordinates *value) = 0;

    virtual Node *put(nTerm key, int64_t coordinatesTTerm) = 0;

    virtual Node *put(tTerm *key, int sizeKey, nTerm value) = 0;

    virtual Node *append(tTerm *key, int sizeKey, nTerm value) = 0;

    virtual Node *append(nTerm key, int64_t coordinates) = 0;

    virtual Node *append(nTerm key, TermCoordinates *value) = 0;

    virtual Node *putOrGet(tTerm *key, int sizeKey, nTerm &value, bool &insertResult) = 0;

    virtual Node *put(nTerm key, TermCoordinates *value) = 0;

    virtual ~Node();
};

#endif /* NODE_H_ */
