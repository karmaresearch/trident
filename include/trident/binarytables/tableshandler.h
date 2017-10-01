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
//#include <trident/binarytables/binarytable.h>
#include <trident/binarytables/binarytableinserter.h>
#include <trident/kb/consts.h>
#include <trident/kb/statistics.h>
#include <trident/files/filemanager.h>

#include <boost/filesystem.hpp>
#include <boost/interprocess/file_mapping.hpp>
#include <boost/interprocess/mapped_region.hpp>

#include <string>
#include <iostream>
#include <mutex>

namespace fs = boost::filesystem;
namespace bip = boost::interprocess;

#define MAX_LENGTH_PATHFILE 1024
#define MAX_N_SECTORS 10000
using namespace std;

class FileMarks {
    private:
        const char *begin;
        const char *end;

        long sizeMarks;
        long sizefile;
        bip::file_mapping *filemapping;
        bip::mapped_region *mappedrgn;

    public:
        const char *getBeginTableCoordinates() {
            return begin;
        }

        const char *getEndTableCoordinates() {
            return end;
        }

        void parse(string path);

        std::pair<uint64_t, uint64_t> getPos(const long mark) {
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
            delete mappedrgn;
            delete filemapping;
        }
};

struct WrittenMarks {
    long key;
    long pos;
    char  strat;
};

class TableStorage {
    private:
        const bool readOnly;
        char pathDir[MAX_LENGTH_PATHFILE];
        int sizePathDir;
        long nTriplesInserted;

        FileMarks *marks[MAX_N_FILES];
        bool marksLoaded[MAX_N_FILES];

#ifdef MT
        std::mutex mutex;
#endif

        FileManager<FileDescriptor, FileDescriptor> *cache;

        short lastCreatedFile;
        long sizeLastCreatedFile;

        //Statistics
        Stats stats;

        //permutation ID
        int perm;

        //*** INSERT ***
        bool indicesWritten;
        BinaryTableInserter* insertHandler;
        std::vector<std::vector<WrittenMarks>> marksToStore;
        std::vector<long> createdMarks;
        int fileCurrentIndex;
        int filePreviousIndex;
        //*** END INSERT ***

        void storeFileIndices();
        void storeFileIndex(const std::vector<WrittenMarks> &input,
                string pathFile);

    public:
        TableStorage(bool readOnly, std::string pathDir, long maxFileSize,
                int maxNFiles, MemoryManager<FileDescriptor> *bytesTracker,
                Stats &stats, int perm);

        std::string getPath();

        std::pair<const char*, const char*> getTable(short file, long mark);

        long startAppend(const long key,
                const char strat,
                BinaryTableInserter* handler);

        void setStrategy(const char strat);

        void append(long v1, long v2);

        void stopAppend();

        long getNTriplesInserted();

        short getLastCreatedFile() {
            return lastCreatedFile;
        }

        long getLastFileSize() {
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
