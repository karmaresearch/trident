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


#include <trident/tree/treeitr.h>
#include <trident/tree/leaf.h>
#include <trident/tree/treecontext.h>
#include <trident/tree/cache.h>

TreeItr::TreeItr(Node *root, Leaf *firstLeaf) {
    this->root = root;
    currentLeaf = firstLeaf;
    currentPos = 0;

    //I temporary remove this leaf from the caching system.
    //cache = root->getContext()->getCache();
    //cache->unregisterNode(currentLeaf);
}

bool TreeItr::hasNext() {
    if (currentPos >= currentLeaf->getCurrentSize()) {

        Node *n = currentLeaf->getRightSibling();

        if (n == currentLeaf) {
//          cache->registerNode(currentLeaf);
            return false;
        }

//      cache->registerNode(currentLeaf);
        currentLeaf = (Leaf *) n;
//      cache->unregisterNode(currentLeaf);

        currentPos = 0;
    }
    return true;
}

long TreeItr::next(TermCoordinates *value) {
    currentLeaf->getValueAtPos(currentPos, value);
    return currentLeaf->getKey(currentPos++);
}

long TreeItr::next(long &value) {
    nTerm v;
    currentLeaf->getValueAtPos(currentPos, &v);
    value = v;
    return currentLeaf->getKey(currentPos++);
}
