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


#include <trident/tree/cache.h>
#include <trident/tree/intermediatenode.h>
#include <trident/tree/leaf.h>
#include <trident/tree/treecontext.h>

#include <boost/chrono.hpp>

#include <iostream>
#include <string>
#include <lz4.h>

using namespace std;
namespace timens = boost::chrono;

void Cache::init(TreeContext *context, std::string path, int fileMaxSize,
                 int maxNFiles, long cacheMaxSize, int sizeLeavesFactory,
                 int sizePreallLeavesFactory, int nodeMinBytes) {
//      BOOST_LOG_TRIVIAL(debug) << "file_max_size: " << fileMaxSize << " cache_max_size: " << cacheMaxSize << " size_leaf_factory: " << sizeLeavesFactory <<
//      " preall: " << sizePreallLeavesFactory <<
//      " nodes_min_bytes: " << nodeMinBytes;

    this->context = context;
    this->factory = new LeafFactory(context, sizePreallLeavesFactory,
                                    sizeLeavesFactory);
    this->manager = new NodeManager(context, nodeMinBytes, fileMaxSize,
                                    maxNFiles, cacheMaxSize, path);
}

Node *Cache::getNodeFromCache(long id) {
    CachedNode *cachedVersion = manager->getCachedNode(id);
    char* b = manager->get(cachedVersion);

    Node *n = NULL;
    if (cachedVersion->children) {
        n = new IntermediateNode(context);
    } else {
        n = factory->get();
    }
    n->setId(cachedVersion->id);

    if (compressedNodes) {
//      timens::system_clock::time_point start = timens::system_clock::now();
        LZ4_decompress_safe(b, supportBuffer, cachedVersion->nodeSize,
                            SIZE_SUPPORT_BUFFER);
//      boost::chrono::duration<double> sec = boost::chrono::system_clock::now()
//              - start;
//      std::cerr << "Decompressing node " << id << " of size "
//              << cachedVersion->nodeSize << "= " << sec.count() * 1000000
//              << " microseconds." << endl;

// Unserialize buffer
//      start = timens::system_clock::now();
        n->unserialize(supportBuffer, 0);
//      sec = boost::chrono::system_clock::now() - start;
//      std::cerr << "Unserializing node " << id << " = "
//              << sec.count() * 1000000 << " microseconds\n";

    } else { // Unserialize buffer
//      timens::system_clock::time_point start = timens::system_clock::now();
        n->unserialize(b, 0);
//      boost::chrono::duration<double> sec = boost::chrono::system_clock::now()
//              - start;
//      std::cerr << "Unserializing node " << id << " = "
//              << sec.count() * 1000000 << " microseconds\n";
    }

//  boost::chrono::duration<double> sec = boost::chrono::system_clock::now() - start;
//  std::cerr << "Time getting node " << id << " is " << sec.count() * 1000000 << " microseconds." << endl;

    return n;
}

void Cache::flushNode(Node *node, const bool registerNode) {
    if (node->getState() == STATE_MODIFIED) {
        //Serialize the node
        int sizeBuffer = node->serialize(supportBuffer, 0);
        if (compressedNodes) {
#if LZ4_VERSION_MAJOR > 1 || LZ4_VERSION_MINOR > 2 || (LZ4_VERSION_MINOR == 2 && LZ4_VERSION_RELEASE >= 9)
	    // LZ4_compress_default does not exist before lz4 version 129.
            int sizeCompressedBuffer = LZ4_compress_default(supportBuffer,
                                                    supportBuffer2, sizeBuffer, SIZE_SUPPORT_BUFFER);
#else
            int sizeCompressedBuffer = LZ4_compress(supportBuffer,
                                                    supportBuffer2, sizeBuffer);
#endif
            if (sizeCompressedBuffer == 0 || sizeCompressedBuffer > SIZE_SUPPORT_BUFFER) {
                BOOST_LOG_TRIVIAL(error) << "Failed compressing buffer (size=0)";
                throw 10;
            }
            manager->put(node, supportBuffer2, sizeCompressedBuffer);
        } else {
            //Write the new node on disk
            manager->put(node, supportBuffer, sizeBuffer);
        }
    }

    if (node->getParent() != NULL && registerNode)
        node->getParent()->cacheChild(node);

    if (!node->canHaveChildren()) {
        //It's a leaf
        factory->release((Leaf *) node);
    } else {
        delete node;
    }
}

void Cache::registerNode(Node *node) {
    if (node->canHaveChildren()) {
        return;
    }

    if (registeredNodes.full()) {
        Node *n = registeredNodes.front();
        registeredNodes.pop_front();

        if (n->getParent() != NULL) {
            flushNode(n, true);
        }
    }
    registeredNodes.push_back(node);
}

void Cache::flushAllCache() {
    while (!registeredNodes.empty()) {
        Node *n = registeredNodes.front();
        registeredNodes.pop_front();
        flushNode(n, true);
    }
}

IntermediateNode *Cache::newIntermediateNode() {
    return new IntermediateNode(context);
}

IntermediateNode *Cache::newIntermediateNode(Node *child1, Node *child2) {
    return new IntermediateNode(context, child1, child2);
}
