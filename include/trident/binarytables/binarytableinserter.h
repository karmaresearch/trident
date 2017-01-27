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


#ifndef _BINARYTABLEINSERTER_H
#define _BINARYTABLEINSERTER_H

#include <trident/files/filemanager.h>
#include <trident/files/comprfiledescriptor.h>
#include <trident/files/filedescriptor.h>

#include <trident/binarytables/fileindex.h>

class BinaryTableInserter {
    private:
        FileManager<FileDescriptor, FileDescriptor> *manager;
        short currentFile;
        uint64_t currentPos;
        uint64_t basePos;
        short baseFile;

    protected:
        FileIndex *index;
        int perm;

        uint64_t writeLong(long t) {
            manager->appendLong(t);
            currentPos += 8;
            return 8;
        }

        uint64_t writeLongInt(long t) {
            manager->appendLong(5, t);
            currentPos += 5;
            return 5;
        }


        uint64_t writeInt(long t) {
            manager->appendInt(t);
            currentPos += 4;
            return 4;
        }

        uint64_t writeByte(char t) {
            manager->append(&t, 1);
            currentPos++;
            return 1;
        }

        uint64_t writeShort(long t) {
            manager->appendShort(t);
            currentPos += 2;
            return 2;
        }

        string getRootDir();

        void writeLong(const uint8_t nbytes, const long v);

        uint64_t writeVLong(long t) {
            uint64_t prevPos = currentPos;
            currentPos = manager->appendVLong(t);
            return currentPos - prevPos;
        }

        uint64_t writeVLong2(long t);

        void overwriteVLong2(short file, uint64_t pos, long number);

        void overwriteBAt(char b, short file, uint64_t pos) {
            manager->overwriteAt(file, pos, b);
        }

        void reserveBytes(const uint8_t bytes);

        void createNewFileIfCurrentIsTooLarge();

        uint64_t getRelativePosition() {
            if (getCurrentFile() != baseFile) {
                return getCurrentPosition();
            } else {
                return getCurrentPosition() - basePos;
            }
        }

        long getNBytesFrom(short file, uint64_t pos);

        uint64_t getFileSize(const short idFile) {
            assert(idFile <= manager->getIdLastFile());
            return manager->sizeFile(idFile);
        }

        void setPosition(const uint64_t pos);

        uint64_t getBasePosition() {
            return basePos;
        }

        //void setPosition(const short file, const int pos);

        virtual void startAppend() = 0;

        virtual void append(long t1,
                long t2) = 0;

        virtual void stopAppend() = 0;

    public:
        void setup(FileManager<FileDescriptor, FileDescriptor> *cache,
                FileIndex *index, int perm);

        void setBasePosition(short file, uint64_t pos);

        void startTableAppend() {
            startAppend();
        }

        void appendPair(const long t1,
                const long t2);

        void stopTableAppend() {
            stopAppend();
        }


        virtual int getType() = 0;


        uint64_t getCurrentPosition();

        short getCurrentFile();

        void cleanup();

        virtual ~BinaryTableInserter() {
        }
};
#endif
