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


#ifndef INTERMEDIATENODE_H_
#define INTERMEDIATENODE_H_

#include <trident/tree/node.h>
#include <trident/kb/consts.h>

class IntermediateNode: public Node {
private:
    Node **children;
    int64_t *idChildren;
    int lastUpdatedChild;

    Node *updateChildren(Node *split, int p,
                         void (*insertAverage)(Node*, int p, Node*, Node*));
    void ensureChildIsLoaded(int p);

public:

    IntermediateNode(TreeContext *context) :
        Node(context) {
        children = NULL;
        idChildren = NULL;
        lastUpdatedChild = -1;
    }

    IntermediateNode(TreeContext *context, Node *child1, Node *child2);

    bool get(nTerm key, int64_t &coordinates);

    bool get(tTerm *key, const int sizeKey, nTerm *value);

    bool get(nTerm key, TermCoordinates *value);

    Node *put(nTerm key, int64_t coordinatesTerm);

    Node *put(tTerm *key, int sizeKey, nTerm value);

    Node *put(nTerm key, TermCoordinates *value);

    Node *append(tTerm *key, int sizeKey, nTerm value);

    Node *append(nTerm key, int64_t coordinatesTerm);

    Node *append(nTerm key, TermCoordinates *value);

    Node *putOrGet(tTerm *key, int sizeKey, nTerm &value,
                   bool &insertResult);

    int unserialize(char *bytes, int pos);

    int serialize(char *bytes, int pos);

    bool canHaveChildren() {
        return true;
    }

    int64_t smallestNumericKey();

    int64_t largestNumericKey();

    tTerm *smallestTextualKey(int *size);

    tTerm *largestTextualKey(int *size);

    Node *getChildForKey(int64_t key);

    Node *getChildForKey(tTerm *key, const int sizeKey);

    Node *getChildAtPos(int pos);

    int getPosChild(Node *child);

    void cacheChild(Node* child);

    Node *getChild(const int p);

    ~IntermediateNode();
};

#endif /* INTERMEDIATENODE_H_ */
