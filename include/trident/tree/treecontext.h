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


#ifndef TREECONTEXT_H_
#define TREECONTEXT_H_

#include <trident/tree/intermediatenode.h>
#include <trident/tree/leaf.h>

#include <kognac/factory.h>

#include <mutex>

class Cache;
class StringBuffer;

class TreeContext {
private:
    Cache* const cache;
    StringBuffer* const buffer;
    const bool readOnly;
    const int maxNElementsPerNode;
    const int minNElementsPerNode;
    const bool textualKeys;
    const bool textualValues;
    PreallocatedFactory<Coordinates>* const leavesElFactory;
    PreallocatedArraysFactory<Coordinates*>* const leavesBufferFactory;
    PreallocatedArraysFactory<long>* const nodeKeyFactory;
    long nodeCounter;

#ifdef MT
    std::recursive_mutex mutex;
#endif

public:
    TreeContext(Cache *cache, StringBuffer *buffer, bool readOnly,
                int maxElementsPerNode, bool textualKeys, bool textualValues,
                PreallocatedFactory<Coordinates> *leavesElFactory,
                PreallocatedArraysFactory<Coordinates*> *leavesBufferFactory,
                PreallocatedArraysFactory<long> *nodeKeyFactory) :
        cache(cache), buffer(buffer), readOnly(readOnly), maxNElementsPerNode(
            maxElementsPerNode), minNElementsPerNode(
                maxElementsPerNode / 2), textualKeys(textualKeys), textualValues(
                    textualValues), leavesElFactory(leavesElFactory), leavesBufferFactory(
                        leavesBufferFactory), nodeKeyFactory(nodeKeyFactory) {
        nodeCounter = 0;
    }

    long getNewNodeID() {
        return nodeCounter++;
    }

    Cache * const getCache() const {
        return cache;
    }

    StringBuffer * const getStringBuffer() const {
        return buffer;
    }

    const bool textKeys() const {
        return textualKeys;
    }

    const bool isReadOnly() const {
        return readOnly;
    }

    const int getMaxElementsPerNode() const {
        return maxNElementsPerNode;
    }

    const int getMinElementsPerNode() const {
        return minNElementsPerNode;
    }

    const bool textValues() const {
        return textualValues;
    }

    PreallocatedFactory<Coordinates>* const getIlFactory() const {
        return leavesElFactory;
    }

    PreallocatedArraysFactory<Coordinates*>* const getIlBufferFactory() const {
        return leavesBufferFactory;
    }

    PreallocatedArraysFactory<long>* const getNodesKeyFactory() const {
        return nodeKeyFactory;
    }

#ifdef MT
    std::recursive_mutex &getMutex() {
        return mutex;
    }
#endif
};

#endif /* TREECONTEXT_H_ */
