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


#ifndef LEAF_H_
#define LEAF_H_

#include <trident/tree/coordinates.h>
#include <trident/tree/node.h>
#include <trident/kb/consts.h>

#include <kognac/factory.h>
#include <kognac/utils.h>

#include <iostream>
#include <inttypes.h>

using namespace std;

#define MAX_INSTANTIED_POS 10

class Leaf: public Node {
private:
    char *rawNode;
    Coordinates **detailPermutations1;
    Coordinates **detailPermutations2;

    bool scanThroughArray;
    int nInstantiatedPositions;
    short instantiatedPositions[MAX_INSTANTIED_POS];

    //Numeric values
    nTerm *numValue;
    int sNumValue;

    void clearFirstLevel(int size);

    Coordinates *parseInternalLine(const int pos);

    Node *insertAtPosition(int p, tTerm *key, int sizeKey, nTerm value);

    Node *insertAtPosition(int p, nTerm key, int64_t coordinates);

    Node *insertAtPosition(int p, nTerm key, TermCoordinates *value);

    Coordinates *addCoordinates(Coordinates* initial, TermCoordinates *value);

    /***** UNSERIALIZATION METHODS *****/
    int unserialize_numberlist(char *bytes, int pos);
    int unserialize_values(char *bytes, int pos, int previousSize);

    int serialize_numberlist(char *bytes, int pos);
    int serialize_values(char *bytes, int pos);

public:
    Leaf(TreeContext *context);

    Leaf(const Leaf& other);

    int unserialize(char *bytes, int pos);

    int serialize(char *bytes, int pos);

    bool get(nTerm key, int64_t &coordinates);

    bool get(tTerm *key, const int sizeKey, nTerm *value);

    bool get(nTerm key, TermCoordinates *value);

    Node *put(nTerm key, int64_t coordinateTerms);

    Node *put(tTerm *key, int sizeKey, nTerm value);

    Node *append(tTerm *key, int sizeKey, nTerm value);

    Node *append(nTerm key, int64_t coordinateTerms);

    Node *append(nTerm key, TermCoordinates *value);

    Node *putOrGet(tTerm *key, int sizeKey, nTerm &value,
                   bool &insertResult);

    Node *put(nTerm key, TermCoordinates *value);

    int64_t getKey(int pos);

    void getValueAtPos(int pos, nTerm *value);

    void getValueAtPos(int pos, TermCoordinates *value);

    int64_t smallestNumericKey();

    int64_t largestNumericKey();

    tTerm *smallestTextualKey(int *size);

    tTerm *largestTextualKey(int *size);

    Leaf *getRightSibling();

    void free();

    ~Leaf();
};

#endif /* LEAF_H_ */
