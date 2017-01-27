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


#ifndef FILEDESCRIPTOR_H_
#define FILEDESCRIPTOR_H_

#include <trident/utils/memorymgr.h>

#include <boost/interprocess/file_mapping.hpp>
#include <boost/interprocess/mapped_region.hpp>

#include <string>

namespace bip = boost::interprocess;

//#define SMALLEST_INCR 1*1024*1024
#define SMALLEST_INCR (uint64_t)16*1024*1024

class Stats;
class FileDescriptor {

private:

    const std::string filePath;

    const bool readOnly;

    const int id;

    bip::file_mapping *mapping;

    bip::mapped_region* mapped_rgn;

    char* buffer;

    uint64_t size;

    uint64_t sizeFile;

    MemoryManager<FileDescriptor> *tracker;
    int memoryTrackerId;
    FileDescriptor **parentArray;

    void mapFile(uint64_t requiredIncrement);

public:
    FileDescriptor(bool readOnly, int id, std::string file, uint64_t fileMaxSize,
                   MemoryManager<FileDescriptor> *tracker, FileDescriptor **parents,
                   Stats * const stats);

    FileDescriptor(bool readOnly, int id, std::string file, uint64_t fileMaxSize,
                   FileDescriptor **parents, Stats * const stats) :
        FileDescriptor(readOnly, id, file, fileMaxSize, NULL, parents, stats) {
    }

    char* getBuffer(uint64_t offset, uint64_t *length);

    char* getBuffer(uint64_t offset, uint64_t *length, int &memoryBlock, const int sesID);

    uint64_t getFileLength();

    int getId() {
        return id;
    }

    bool isUsed();

    void shiftFile(uint64_t pos, uint64_t diff);

    void append(char *bytes, const uint64_t size);

    uint64_t appendVLong(const long v);

    uint64_t appendVLong2(const long v);

    void appendLong(const long v);

    void appendInt(const long v);

    void appendShort(const long v);

    void appendLong(const uint8_t nbytes, const uint64_t v);

    void overwriteAt(uint64_t pos, char byte);

    void overwriteVLong2At(uint64_t pos, long number);

    void reserveBytes(const uint8_t n);

    ~FileDescriptor();
};

#endif /* FILEDESCRIPTOR_H_ */
