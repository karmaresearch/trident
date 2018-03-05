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


#ifndef FILEMANAGER_H_
#define FILEMANAGER_H_

#include <trident/utils/memorymgr.h>
#include <trident/kb/consts.h>
#include <trident/kb/statistics.h>

#include <kognac/logs.h>
#include <kognac/logs.h>

#include <list>
#include <string>
#include <sstream>
#include <vector>
#include <iostream>
#include <mutex>
#include <assert.h>

using namespace std;

template<class T, class K>
class FileManager {
    private:
        const bool readOnly;
        const std::string cacheDir;
        const int64_t fileMaxSize;
        const int maxFiles;
        int lastFileId;
        int nOpenedFiles;

        MemoryManager<K> *bytesTracker;
        T *openedFiles[MAX_N_FILES];
        list<int> trackerOpenedFiles;

        //Cache the uncompressed size of file used during the writing
        vector<uint64_t> cacheFileSize;

        int sessions[MAX_SESSIONS];
        int lastSession;

        Stats* const stats;

#ifdef MT
        std::mutex mutex;
#endif

        bool isFileLoaded(const int id) {
            return openedFiles[id] != NULL;
        }

        void load_file(const int id) {
            if (!isFileLoaded(id)) {
#ifdef MT
                std::unique_lock<std::mutex> lock(mutex);
                if (!isFileLoaded(id)) {
#endif
                    if (nOpenedFiles >= maxFiles) {
                        //Take the last opened file
                        int idxFileToRemove = trackerOpenedFiles.front();
                        int firstFileRemoved = -1;
                        trackerOpenedFiles.pop_front();
                        bool rem = true;
                        assert(!trackerOpenedFiles.empty());
                        while (openedFiles[idxFileToRemove] == NULL ||
                                openedFiles[idxFileToRemove]->isUsed()) {

                            if (openedFiles[idxFileToRemove] != NULL) {
                                //It means the file is still used
                                trackerOpenedFiles.push_back(idxFileToRemove);
                                if (firstFileRemoved == -1) {
                                    firstFileRemoved = idxFileToRemove;
                                }
                            } else {
                                nOpenedFiles--;
                            }

                            if (!trackerOpenedFiles.empty()) {
                                idxFileToRemove = trackerOpenedFiles.front();
                                trackerOpenedFiles.pop_front();
                            } else {
                                rem = false;
                            }

                            if (idxFileToRemove == firstFileRemoved) {
                                rem = false;
                                break;
                            }
                        }
                        if (rem) {
                            //LOG(DEBUGL) << "Deleting map for file " << idxFileToRemove;
                            delete openedFiles[idxFileToRemove];
                            openedFiles[idxFileToRemove] = NULL;
                            nOpenedFiles--;
                        }
                    }
                    std::stringstream filePath;
                    filePath << cacheDir << "/" << id;
                    T* f = new T(readOnly, id, filePath.str(), fileMaxSize,
                            bytesTracker, openedFiles, stats);
                    openedFiles[id] = f;
                    trackerOpenedFiles.push_back(id);
                    nOpenedFiles++;
#ifdef MT
                }
                lock.unlock();
#endif
            }
        }
    public:
        FileManager(std::string path, bool readOnly, int64_t fileMaxSize,
                int maxNumberFiles, int lastFileId, MemoryManager<K> *bytesTracker,
                Stats * const stats) :
            readOnly(readOnly), cacheDir(path), fileMaxSize(fileMaxSize),
            maxFiles(maxNumberFiles), lastFileId(lastFileId), bytesTracker(bytesTracker),
            stats(stats) {
                for (int i = 0; i < MAX_N_FILES; ++i) {
                    openedFiles[i] = NULL;
                }

                lastSession = 0;
                for (int i = 0; i < MAX_SESSIONS; ++i) {
                    sessions[i] = FREE_SESSION;
                }
                nOpenedFiles = 0;
            }

        string getCacheDir() {
            return cacheDir;
        }

        char* getBuffer(short id, uint64_t offset, uint64_t *length) {
            load_file(id);
            return openedFiles[id]->getBuffer(offset, length);
        }

        char* getBuffer(short id, uint64_t offset, uint64_t *length, int sessionId) {
            load_file(id);
            int memoryBlock;

            char *result = openedFiles[id]->getBuffer(offset, length,
                    memoryBlock, sessionId);

            if (sessionId != EMPTY_SESSION && sessions[sessionId] != memoryBlock) {
                //Tell the memory manager that we don't need this block anymore
                if (sessions[sessionId] >= 0) {
                    bytesTracker->releaseLock(sessions[sessionId]);
                }

                sessions[sessionId] = memoryBlock;

                if (memoryBlock >= 0) {
                    bytesTracker->addLock(memoryBlock);
                }
            }

            return result;
        }

        int newSession() {
            int cnt = 0;
            while (sessions[lastSession] != FREE_SESSION) {
                lastSession = (lastSession + 1) % MAX_SESSIONS;
                cnt++;
                if (cnt > MAX_SESSIONS) {
                    LOG(ERRORL) << "Max number of sessions is reached";
                    throw 10;
                }
            }
            sessions[lastSession] = EMPTY_SESSION;
            cnt = lastSession;
            lastSession = (lastSession + 1) % MAX_SESSIONS;
            // LOG(DEBUGL) << "This = " << this << ", Open session " << cnt;
            return cnt;
        }

        void closeSession(int idx) {
            //Release the lock
            if (sessions[idx] >= 0) {
                bytesTracker->releaseLock(sessions[idx]);
            }
            // LOG(DEBUGL) << "This = " << this << ", Close session " << idx;
            sessions[idx] = FREE_SESSION;
        }

        uint64_t sizeFile(const int idx) {
            if (isFileLoaded(idx)) {
                return openedFiles[idx]->getFileLength();
            }

            if (idx < cacheFileSize.size() && cacheFileSize[idx] != 0) {
                return cacheFileSize[idx];
            }

            load_file(idx);
            uint64_t size =  openedFiles[idx]->getFileLength();

            if (readOnly || idx < lastFileId) {
                if (idx >= cacheFileSize.size()) {
                    cacheFileSize.resize(idx + 1, 0);
                }
                cacheFileSize[idx] = size;
            }
            return size;
        }

        uint64_t sizeLastFile() {
            return sizeFile(lastFileId);
        }


        void shiftRemainingFile(int idx, uint64_t pos, uint64_t diff) {
            load_file(idx);
            openedFiles[idx]->shiftFile(pos, diff);
        }

        int getIdLastFile() {
            return lastFileId;
        }

        uint64_t getFileMaxSize() {
            return fileMaxSize;
        }

        short createNewFile() {
            lastFileId++;
            if (lastFileId == MAX_N_FILES) {
                LOG(ERRORL) << "Max number of files is reached";
                throw 10;
            }
            load_file(lastFileId);
            return (short) lastFileId;
        }

        void append(char *bytes, int size) {
            load_file(lastFileId);
            openedFiles[lastFileId]->append(bytes, size);
        }

        uint64_t appendVLong(int64_t n) {
            load_file(lastFileId);
            return openedFiles[lastFileId]->appendVLong(n);
        }

        uint64_t appendVLong2(int64_t n) {
            load_file(lastFileId);
            return openedFiles[lastFileId]->appendVLong2(n);
        }

        void appendLong(int64_t n) {
            load_file(lastFileId);
            openedFiles[lastFileId]->appendLong(n);
        }

        void appendInt(int64_t n) {
            load_file(lastFileId);
            openedFiles[lastFileId]->appendInt(n);
        }

        void appendShort(int64_t n) {
            load_file(lastFileId);
            openedFiles[lastFileId]->appendShort(n);
        }

        void appendLong(const uint8_t nbytes, const uint64_t n) {
            load_file(lastFileId);
            openedFiles[lastFileId]->appendLong(nbytes, n);
        }

        void reserveBytes(const uint8_t bytes) {
            load_file(lastFileId);
            openedFiles[lastFileId]->reserveBytes(bytes);
        }

        void overwriteAt(short file, uint64_t pos, char byte) {
            load_file(file);
            openedFiles[file]->overwriteAt(pos, byte);
        }

        void overwriteVLong2At(short file, uint64_t pos, int64_t number) {
            load_file(file);
            openedFiles[file]->overwriteVLong2At(pos, number);
        }

        void empty() {
            for (int i = 0; i < MAX_N_FILES; ++i) {
                if (openedFiles[i] != NULL) {
                    delete openedFiles[i];
                    openedFiles[i] = NULL;
                }
            }
        }

        ~FileManager() {
            empty();
        }
};

#endif /* FILEMANAGER_H_ */
