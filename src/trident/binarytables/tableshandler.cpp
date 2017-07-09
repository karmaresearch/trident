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


#include <trident/binarytables/tableshandler.h>
//#include <trident/binarytables/binarytable.h>

#include <kognac/utils.h>

#include <boost/filesystem.hpp>
#include <boost/chrono.hpp>
#include <boost/interprocess/file_mapping.hpp>
#include <boost/interprocess/mapped_region.hpp>

#include <iostream>
#include <string>
#include <fstream>
#include <stdlib.h>
#include <stdio.h>

using namespace std;
namespace fs = boost::filesystem;
namespace timens = boost::chrono;
namespace bip = boost::interprocess;

void FileMarks::parse(string path) {
    if (!fs::exists(path)) {
        BOOST_LOG_TRIVIAL(error) << "File non-existent: " << path;
        throw 10;
    }

    this->filemapping = new bip::file_mapping(path.c_str(), bip::read_only);
    this->mappedrgn = new bip::mapped_region(*(this->filemapping),
            bip::read_only);
    char *raw_input = static_cast<char*>(this->mappedrgn->get_address());

    //Size marks
    this->sizefile = fs::file_size(path.substr(0, path.size() - 4));
    this->sizeMarks = Utils::decode_long(raw_input, 0);
    this->begin = raw_input + 8;
    this->end = this->begin + this->sizeMarks * 11;
}

TableStorage::TableStorage(bool readOnly, string pathDir, long maxFileSize,
        int maxNFiles,
        MemoryManager<FileDescriptor> *bytesTracker,
        Stats &stats, int perm) :
    readOnly(readOnly), marks(), marksLoaded(), stats(stats), perm(perm) {
        strcpy(this->pathDir, pathDir.c_str());
        this->sizePathDir = strlen(this->pathDir);
        this->pathDir[sizePathDir++] = '/';

        //Determine the highest number of a file
        fs::path dir(pathDir);
        fs::directory_iterator end_iter;
        lastCreatedFile = 0;
        sizeLastCreatedFile = 0;
        if (fs::exists(dir) && fs::is_directory(dir)) {
            for (fs::directory_iterator dir_iter(dir); dir_iter != end_iter;
                    ++dir_iter) {
                int idx = atoi(dir_iter->path().filename().c_str());
                if (lastCreatedFile < idx)
                    lastCreatedFile = (short) idx;
            }

            cache = new FileManager<FileDescriptor, FileDescriptor>(pathDir,
                    readOnly, maxFileSize, maxNFiles, lastCreatedFile,
                    bytesTracker, &stats);

            sizeLastCreatedFile = cache->sizeFile(lastCreatedFile);
        } else {
            //Create the directory if it does not exist
            if (!readOnly) {
                fs::create_directories(pathDir);
            }

            cache = new FileManager<FileDescriptor, FileDescriptor>(pathDir,
                    readOnly, maxFileSize, maxNFiles, lastCreatedFile,
                    bytesTracker, &stats);
        }
        indicesWritten = false;
        insertHandler = NULL;
        nTriplesInserted = 0;
        fileCurrentIndex = -1;
        filePreviousIndex = 0;
    }

bool TableStorage::doesFileHaveCoordinates(short file) {
    if (!marksLoaded[file]) {
        sprintf(pathDir + sizePathDir, "%d.idx", file);
        FileMarks *m = new FileMarks();
        m->parse(string(pathDir));
        marks[file] = m;
        marksLoaded[file] = true;
    }
    return marks[file] != NULL;
}

const char *TableStorage::getBeginTableCoordinates(short file) {
    if (!marksLoaded[file]) {
        sprintf(pathDir + sizePathDir, "%d.idx", file);
        FileMarks *m = new FileMarks();
        m->parse(string(pathDir));
        marks[file] = m;
        marksLoaded[file] = true;
    }
    return marks[file]->getBeginTableCoordinates();
}

const char *TableStorage::getEndTableCoordinates(short file) {
    if (!marksLoaded[file]) {
        sprintf(pathDir + sizePathDir, "%d.idx", file);
        FileMarks *m = new FileMarks();
        m->parse(string(pathDir));
        marks[file] = m;
        marksLoaded[file] = true;
    }
    return marks[file]->getEndTableCoordinates();
}

std::string TableStorage::getPath() {
    return std::string(pathDir, sizePathDir);
}

std::pair<const char*, const char*> TableStorage::getTable(short file, long mark) {
    //I assume all the table is in one file
    if (!marksLoaded[file]) {
#ifdef MT        
        std::unique_lock<std::mutex> lock(mutex);
        if (!marksLoaded[file]) {
#endif
            sprintf(pathDir + sizePathDir, "%d.idx", file);
            FileMarks *m = new FileMarks();
            m->parse(string(pathDir));
            marks[file] = m;
            marksLoaded[file] = true;
#ifdef MT        
        }
        lock.unlock();
#endif
    }
    std::pair<uint64_t,uint64_t> coord = marks[file]->getPos(mark);
    uint64_t realLen = (int) - 1;
    const char *start = cache->getBuffer(file, coord.first, &realLen);
    const uint64_t len = coord.second - coord.first;
    const char *end = start + len;
    return make_pair(start, end);
}

long TableStorage::startAppend(const long key,
        const char strat,
        BinaryTableInserter *handler) {

    //Create a new file if the previous is too large
    if (cache->sizeFile(lastCreatedFile) > cache->getFileMaxSize()) {
        cache->createNewFile();
        lastCreatedFile = cache->getIdLastFile();
        sizeLastCreatedFile = cache->sizeFile(lastCreatedFile);
    }

    // Mark a new entry
    if (marksToStore.size() <= lastCreatedFile) {
        marksToStore.push_back(std::vector<WrittenMarks>());
        createdMarks.push_back(0);
    }
    if (marksToStore[lastCreatedFile].size() >= 10000000) {
        //Empty it
        sprintf(pathDir + sizePathDir, "%d.idx", lastCreatedFile);
        storeFileIndex(marksToStore[lastCreatedFile], string(pathDir));
        marksToStore[lastCreatedFile] = std::vector<WrittenMarks>();
    }
    marksToStore[lastCreatedFile].push_back(WrittenMarks());
    WrittenMarks &lastMark = marksToStore[lastCreatedFile].back();
    createdMarks[lastCreatedFile]++;
    lastMark.key = key;
    lastMark.strat = strat;
    lastMark.pos = sizeLastCreatedFile;

    // Record the file/position to store a possible index
    fileCurrentIndex = lastCreatedFile;
    handler->setup(cache, NULL, perm);
    handler->setBasePosition(lastCreatedFile, sizeLastCreatedFile);
    handler->startTableAppend();
    insertHandler = handler;
    return createdMarks[lastCreatedFile] - 1;
}

void TableStorage::setStrategy(const char strat) {
    marksToStore[lastCreatedFile].back().strat = strat;
}

void TableStorage::append(long t1, long t2) {
    nTriplesInserted++;
    insertHandler->appendPair(t1, t2);
}

long TableStorage::getNTriplesInserted() {
    return nTriplesInserted;
}

void TableStorage::stopAppend() {
    insertHandler->stopTableAppend();
    lastCreatedFile = insertHandler->getCurrentFile();
    sizeLastCreatedFile = insertHandler->getCurrentPosition();
    insertHandler->cleanup();
    insertHandler = NULL;

    //Should I write the previous entries and free some memory?
    if (filePreviousIndex != fileCurrentIndex &&
            !marksToStore[filePreviousIndex].empty()) {
        sprintf(pathDir + sizePathDir, "%d.idx", filePreviousIndex);
        storeFileIndex(marksToStore[filePreviousIndex], string(pathDir));
        marksToStore[filePreviousIndex] = std::vector<WrittenMarks>();
    }
    filePreviousIndex = fileCurrentIndex;
}

void TableStorage::storeFileIndices() {
    for (int i = 0; i < marksToStore.size(); ++i) {
        if (marksToStore[i].size() > 0) {
            sprintf(pathDir + sizePathDir, "%d.idx", i);
            storeFileIndex(marksToStore[i], string(pathDir));
        }
    }
}

void TableStorage::storeFileIndex(const std::vector<WrittenMarks> &input,
        string pathFile) {
    char supportArray[16];
    ofstream oFile;
    if (fs::exists(pathFile)) {
        {
            bip::file_mapping mapping(pathFile.c_str(), bip::read_write);
            bip::mapped_region mapped_rgn(mapping, bip::read_write, 0, 8);
            char *buffer = static_cast<char*>(mapped_rgn.get_address());
            long existing_n = Utils::decode_long(buffer);
            Utils::encode_long(buffer, existing_n + input.size());
            mapped_rgn.flush(0, 8);
        }
        oFile.open(pathFile, std::ios_base::app);
    } else {
        oFile.open(pathFile, std::ios_base::app);
        Utils::encode_long(supportArray, 0, input.size());
        oFile.write(supportArray, 8);
    }
    for(long i = 0; i < input.size(); ++i) {
        const WrittenMarks &m = input[i];
        Utils::encode_longNBytes(supportArray, 5, m.pos);
        Utils::encode_longNBytes(supportArray+5, 5,  m.key);
        supportArray[10] = m.strat;
        oFile.write(supportArray, 11);
    }
    oFile.close();
}

void TableStorage::stopInsert() {
    assert(!readOnly);
    if (!readOnly) {
        storeFileIndices();
        cache->empty();
        indicesWritten = true;
    }
}

std::vector<const char*> TableStorage::loadAllFiles() {
    std::vector<const char*> files;
    for(int i = 0; i <= cache->getIdLastFile(); ++i) {
        uint64_t length = std::numeric_limits<uint64_t>::max();
        files.push_back(cache->getBuffer(i, 0, &length));
    }
    return files;
}

TableStorage::~TableStorage() {
    if (!readOnly && !indicesWritten) {
        storeFileIndices();
    }
    for (int i = 0; i <= lastCreatedFile; ++i) {
        if (marks[i] != NULL) {
            delete marks[i];
        }
    }
    delete cache;
}
