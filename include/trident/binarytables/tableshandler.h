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


#ifndef TWOTERMSTORAGE_H_
#define TWOTERMSTORAGE_H_

#include <trident/binarytables/fileindex.h>
#include <trident/binarytables/binarytableinserter.h>
#include <trident/kb/consts.h>
#include <trident/kb/statistics.h>
#include <trident/files/filemanager.h>
#include <trident/utils/memoryfile.h>

#include <string>
#include <iostream>
#include <mutex>

#define MAX_LENGTH_PATHFILE 1024
#define MAX_N_SECTORS 10000
using namespace std;

class FileMarks {
    private:
        const char *begin;
        const char *end;

        int64_t sizeMarks;
        int64_t sizefile;
        std::unique_ptr<MemoryMappedFile> mappedFile;

    public:
        const char *getBeginTableCoordinates() {
            return begin;
        }

        const char *getEndTableCoordinates() {
            return end;
        }

        void parse(string path);

        std::pair<uint64_t, uint64_t> getPos(const int64_t mark) {
            const uint64_t startpos = Utils::decode_longFixedBytes(begin + 11 * mark, 5);
            uint64_t endpos;
            if (mark == sizeMarks - 1) {
                endpos = sizefile;
            } else {
                endpos = Utils::decode_longFixedBytes(begin + 11 * (mark + 1), 5);
            }
            return std::make_pair(startpos, endpos);
        }

        ~FileMarks() {
        }
};

struct WrittenMarks {
    int64_t key;
    int64_t pos;
    char  strat;
};

class TableStorage {
    private:
        const bool readOnly;
        char pathDir[MAX_LENGTH_PATHFILE];
        int sizePathDir;
        int64_t nTriplesInserted;

        FileMarks *marks[MAX_N_FILES];
        bool marksLoaded[MAX_N_FILES];

#ifdef MT
        std::mutex mutex;
#endif

        FileManager<FileDescriptor, FileDescriptor> *cache;

        short lastCreatedFile;
        int64_t sizeLastCreatedFile;

        //Statistics
        Stats stats;

        //permutation ID
        int perm;

        //*** INSERT ***
        bool indicesWritten;
        BinaryTableInserter* insertHandler;
        std::vector<std::vector<WrittenMarks>> marksToStore;
        std::vector<int64_t> createdMarks;
        int fileCurrentIndex;
        int filePreviousIndex;
        //*** END INSERT ***

        void storeFileIndices();
        void storeFileIndex(const std::vector<WrittenMarks> &input,
                string pathFile);

    public:
        TableStorage(bool readOnly, std::string pathDir, int64_t maxFileSize,
                int maxNFiles, MemoryManager<FileDescriptor> *bytesTracker,
                Stats &stats, int perm);

        std::string getPath();

        std::pair<const char*, const char*> getTable(short file, int64_t mark);

        int64_t startAppend(const int64_t key,
                const char strat,
                BinaryTableInserter* handler);

        void setStrategy(const char strat);

        void append(int64_t v1, int64_t v2);

        void stopAppend();

        int64_t getNTriplesInserted();

        short getLastCreatedFile() {
            return lastCreatedFile;
        }

        int64_t getLastFileSize() {
            return sizeLastCreatedFile;
        }

        bool doesFileHaveCoordinates(short file);

        const char *getBeginTableCoordinates(short file);

        const char *getEndTableCoordinates(short file);

        std::vector<const char*> loadAllFiles();

        void stopInsert();

        ~TableStorage();
};

#endif
