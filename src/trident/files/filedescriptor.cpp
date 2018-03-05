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


#include <trident/files/filedescriptor.h>
#include <trident/files/filemanager.h>
#include <trident/kb/consts.h>

#include <kognac/utils.h>

#include <algorithm>
#include <string>
#include <iostream>
#include <fstream>
#include <cmath>

#include <unistd.h>

using namespace std;

void FileDescriptor::mapFile(uint64_t requiredIncrement) {
    if (requiredIncrement > 0) {
        mappedFile->flushAll();
        mappedFile = NULL;
        //mapped_rgn->flush();
        //delete mapped_rgn;
        Utils::resizeFile(filePath, sizeFile + requiredIncrement);
    }
    //mapped_rgn = new bip::mapped_region(*mapping,
    //                                    readOnly ? bip::read_only : bip::read_write);
    //buffer = static_cast<char*>(mapped_rgn->get_address());
    //sizeFile = (uint64_t)mapped_rgn->get_size();

    mappedFile = std::unique_ptr<MemoryMappedFile>(new MemoryMappedFile(filePath, readOnly));
    buffer = mappedFile->getData();
    sizeFile = mappedFile->getLength();

    if (tracker) {
        if (memoryTrackerId != -1) {
            tracker->update(memoryTrackerId, sizeFile);
        } else {
            memoryTrackerId = tracker->add(sizeFile, this, id,
                                           parentArray);
        }
    }
}

FileDescriptor::FileDescriptor(bool readOnly, int id, std::string file,
                               uint64_t fileMaxSize, MemoryManager<FileDescriptor> *tracker,
                               FileDescriptor **parents, Stats * const stats) :
    filePath(file), readOnly(readOnly), id(id), parentArray(parents) {
    this->tracker = tracker;
    memoryTrackerId = -1;

    bool newFile = false;
    if (!readOnly && !Utils::exists(file)) {
        ofstream oFile(file);
        oFile.seekp(1024 * 1024);
        oFile.put(0);
        newFile = true;
    }
    //mapping = new bip::file_mapping(file.c_str(),
    //                                readOnly ? bip::read_only : bip::read_write);
    //mapped_rgn = NULL;
    
    size = 0;
    mapFile(0);

    if (!newFile) {
        size = sizeFile;
    }
}

char* FileDescriptor::getBuffer(uint64_t offset, uint64_t *length) {
    if (*length > size - offset) {
        *length = size - offset;
    }
    return buffer + offset;
}

uint64_t FileDescriptor::getFileLength() {
    return size;
}

void FileDescriptor::shiftFile(uint64_t pos, uint64_t diff) {
    if (this->size + diff > sizeFile) {
        uint64_t increment = std::max(SMALLEST_INCR, std::max(diff, (uint64_t) sizeFile));
        mapFile(increment);
    }
    memmove(buffer + pos + diff, buffer + pos, size - pos);
    size += diff;
}

void FileDescriptor::append(char *bytes, const uint64_t size) {
    if (size + this->size > sizeFile) {
        uint64_t increment = std::max(SMALLEST_INCR, std::max((uint64_t) size, (uint64_t) sizeFile));
        mapFile(increment);
    }
    memcpy(buffer + this->size, bytes, size);
    this->size += size;
}

char* FileDescriptor::getBuffer(uint64_t offset, uint64_t *length,
                                int &memoryBlock, const int sesID) {
    //sedID is used only by the comprfiledescriptor.
    memoryBlock = memoryTrackerId;
    if (*length > size - offset) {
        *length = size - offset;
    }
    return buffer + offset;
}

uint64_t FileDescriptor::appendVLong(const int64_t v) {
    if (8 + this->size > sizeFile) {
        uint64_t increment = std::max(SMALLEST_INCR, std::max((uint64_t) 8, (uint64_t) sizeFile));
        mapFile(increment);
    }
    const uint16_t bytesUsed = Utils::encode_vlong(buffer + size, v);
    this->size += bytesUsed;
    return this->size;
}

uint64_t FileDescriptor::appendVLong2(const int64_t v) {
    if (8 + this->size > sizeFile) {
        uint64_t increment = std::max(SMALLEST_INCR, std::max((uint64_t) 8, (uint64_t) sizeFile));
        mapFile(increment);
    }
    const uint16_t nbytes = Utils::encode_vlong2(buffer + size, v);
    this->size += nbytes;
    return this->size;
}

void FileDescriptor::appendLong(const int64_t v) {
    if (8 + this->size > sizeFile) {
        uint64_t increment = std::max(SMALLEST_INCR, std::max((uint64_t) 8, (uint64_t) sizeFile));
        mapFile(increment);
    }
    Utils::encode_long(buffer + size, v);
    this->size += 8;
}

void FileDescriptor::appendInt(const int64_t v) {
    if (4 + this->size > sizeFile) {
        uint64_t increment = std::max(SMALLEST_INCR, std::max((uint64_t) 4, (uint64_t) sizeFile));
        mapFile(increment);
    }
    Utils::encode_int(buffer + size, v);
    this->size += 4;
}

void FileDescriptor::appendShort(const int64_t v) {
    if (2 + this->size > sizeFile) {
        uint64_t increment = std::max(SMALLEST_INCR, std::max((uint64_t) 2, (uint64_t) sizeFile));
        mapFile(increment);
    }
    Utils::encode_short(buffer + size, v);
    this->size += 2;
}

void FileDescriptor::appendLong(const uint8_t nbytes, const uint64_t v) {
    if (nbytes + this->size > sizeFile) {
        uint64_t increment = std::max(SMALLEST_INCR, std::max((uint64_t)nbytes,
                                      (uint64_t) sizeFile));
        mapFile(increment);
    }
    Utils::encode_longNBytes(buffer + size, nbytes, v);
    this->size += nbytes;
}


void FileDescriptor::reserveBytes(const uint8_t n) {
    if (n + this->size > sizeFile) {
        uint64_t increment = std::max(SMALLEST_INCR, std::max((uint64_t)n, (uint64_t) sizeFile));
        mapFile(increment);
    }
    this->size += n;
}

void FileDescriptor::overwriteAt(uint64_t pos, char byte) {
    buffer[pos] = byte;
}

void FileDescriptor::overwriteVLong2At(uint64_t pos, int64_t number) {
    Utils::encode_vlong2(buffer + pos, number);
}

bool FileDescriptor::isUsed() {
    if (tracker) {
        if (tracker->isUsed(memoryTrackerId))
            return true;
        else
            return false;
    } else {
        return true;
    }
}

FileDescriptor::~FileDescriptor() {
    if (tracker) {
        tracker->removeBlockWithoutDeallocation(memoryTrackerId);
    }

    buffer = NULL;
    if (mappedFile != NULL) {
        mappedFile->flushAll();
        mappedFile = NULL;
    }
    //if (mapped_rgn != NULL) {
    //    mapped_rgn->flush(0, size);
    //    delete mapped_rgn;
    //    mapped_rgn = NULL;
    //}

    //delete mapping;

    if (!readOnly) {
        if (size == 0) {
            //Remove the file
            Utils::remove(filePath);
        } else if (size < sizeFile) {
            Utils::resizeFile(filePath, size);
        }
    }
}
