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


#ifndef LEAFFACTORY_H_
#define LEAFFACTORY_H_

#include <trident/tree/treecontext.h>

class LeafFactory {
private:
    TreeContext *context;

    std::vector<Leaf> preallocatedElements;
    int preallocatedSize;
    int maxSize;
    Leaf **elements;
    int size;

public:

    LeafFactory(TreeContext *context, int preallocatedSize, int maxSize) :
        preallocatedElements(preallocatedSize, Leaf(context)), maxSize(
            maxSize) {
        this->context = context;
        this->preallocatedSize = preallocatedSize;
        elements = new Leaf*[maxSize];
        size = 0;
    }

    Leaf *get() {
        if (size > 0) {
            return elements[--size];
        } else {
            if (preallocatedSize > 0) {
                Leaf * leaf = &(preallocatedElements[--preallocatedSize]);
                leaf->doNotDeallocate();
                return leaf;
            } else {
                return new Leaf(context);
            }
        }
    }

    void release(Leaf *el) {
        el->free();
        if (size < maxSize) {
            elements[size++] = el;
        } else if (el->shouldDeallocate()) {
            delete el;
        }
    }

    ~LeafFactory() {
        for (int i = 0; i < size; ++i) {
            if (elements[i]->shouldDeallocate())
                delete elements[i];
        }
        delete[] elements;
    }
};

#endif /* LEAFFACTORY_H_ */
