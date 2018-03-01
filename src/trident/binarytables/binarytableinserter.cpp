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


#include <trident/binarytables/binarytableinserter.h>

string BinaryTableInserter::getRootDir() {
    return manager->getCacheDir();
}

uint64_t BinaryTableInserter::writeVLong2(long t) {
    uint64_t prevPos = currentPos;
    currentPos = manager->appendVLong2(t);
    return currentPos - prevPos;
}

void BinaryTableInserter::writeLong(const uint8_t nbytes, const long v) {
    manager->appendLong(nbytes, v);
    currentPos += nbytes;
}

void BinaryTableInserter::overwriteVLong2(short file, uint64_t pos, long number) {
    manager->overwriteVLong2At(file, pos, number);
}

//reserve "bytes" consecutive bytes
void BinaryTableInserter::reserveBytes(const uint8_t bytes) {
    manager->reserveBytes(bytes);
    currentPos += bytes;
}

void BinaryTableInserter::createNewFileIfCurrentIsTooLarge() {
    if (manager->sizeLastFile() + 17 >= manager->getFileMaxSize()) {
        currentFile = manager->createNewFile();
        currentPos = 0;
    }
}

long BinaryTableInserter::getNBytesFrom(short file, uint64_t pos) {
    assert(getCurrentFile() >= file);
    if (getCurrentFile() == file) {
        assert(getCurrentPosition() >= pos);
        return (long)(getCurrentPosition() - pos);
    } else {
        uint64_t diff = getCurrentPosition();
        short f = getCurrentFile();
        f--;
        while (f > file) {
            diff += manager->sizeFile(f);
            f--;
        }
        diff += manager->sizeFile(f) - pos;
        return (long)diff;
    }
}

void BinaryTableInserter::appendPair(const long t1, const long t2) {
    append(t1, t2);
}

void BinaryTableInserter::setup(FileManager<FileDescriptor, FileDescriptor> *manager,
                                FileIndex *index,
                                int perm) {
    this->manager = manager;
    this->index = index;
    this->perm = perm;
}

void BinaryTableInserter::cleanup() {
}

void BinaryTableInserter::setBasePosition(short file, uint64_t pos) {
    this->baseFile = file;
    this->basePos = pos;
    this->currentFile = file;
    this->currentPos = pos;
}

uint64_t BinaryTableInserter::getCurrentPosition() {
    return currentPos;
}

short BinaryTableInserter::getCurrentFile() {
    return currentFile;
}
