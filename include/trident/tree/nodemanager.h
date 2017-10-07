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


#ifndef NODEMANAGER_H_
#define NODEMANAGER_H_

#include <trident/tree/treecontext.h>
#include <trident/files/filemanager.h>
#include <trident/files/filedescriptor.h>
#include <trident/utils/memoryfile.h>

#include <string>
#include <unordered_map>

using namespace std;

class TreeContext;

typedef struct CachedNode {
    long id;
    CachedNode *previous;
    CachedNode *next;
    int nodeSize;
    int availableSize;
    int posIndex;
    short fileIndex;
    bool children;
} CachedNode;

struct StoredNodesKeyHasher {
    std::size_t operator()(long n) const {
        return (int) n;
    }
};
struct StoredNodesKeyCmp {
    bool operator()(long o1, long o2) const {
        return o1 == o2;
    }
};

#define NODE_SIZE 25

class NodeManager {
private:
    const bool readOnly;
    const string path;
    const int nodeMinSize;

    //Used during the writing
    std::unordered_map<long, CachedNode*, StoredNodesKeyHasher,
          StoredNodesKeyCmp> storedNodes;

    //Used in cache it's read-only
    CachedNode *readOnlyStoredNodes;
    bool *nodesLoaded;
    //bip::file_mapping *mapping;
    //bip::mapped_region *mapped_rgn;
    std::unique_ptr<MemoryMappedFile> mappedFile;
    char *rawInput;

    std::vector<CachedNode*> firstElementsPerFile;
    FileManager<FileDescriptor, FileDescriptor> *manager;
    MemoryManager<FileDescriptor> *bytesTracker;

    short lastCreatedFile;
    CachedNode *lastNodeInserted;

    static void unserializeNodeFrom(CachedNode *node, char *buffer, int pos);

    static int serializeTo(CachedNode *node, char *buffer);

public:
    NodeManager(TreeContext *context, int nodeMinBytes, int fileMaxSize,
                int maxNFiles, long cacheMaxSize, std::string path);

    char* get(CachedNode *node);

    void put(Node *node, char *buffer, int sizeBuffer);

    CachedNode *getCachedNode(long id);

    static void compressSpace(string path);

    ~NodeManager();
};

#endif /* NODEMANAGER_H_ */
