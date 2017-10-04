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


#include <trident/tree/stringbuffer.h>

#include <kognac/logs.h>
#include <kognac/utils.h>

#include <lz4.h>
#include <lz4hc.h>
#include <string>
#include <cstring>
#include <algorithm>

using namespace std;

char StringBuffer::FINISH_THREAD[1];

StringBuffer::StringBuffer(string dir, bool readOnly, int factorySize,
                           long cacheSize, Stats *stats) :
    dir(dir), factory(SB_BLOCK_SIZE, 2, factorySize), readOnly(readOnly), maxElementsInCache(
        max(5, (int) (cacheSize / SB_BLOCK_SIZE))) {

    uncompressedSize = 0;
    writingCurrentBufferSize = 0;
    bufferToCompress = NULL;
    sizeBufferToCompress = 0;
    elementsInCache = 0;
    this->stats = stats;
    firstBlockInCache = -1;
    lastBlockInCache = -1;
    assert(maxElementsInCache > 0);

    sizeBaseEntry = 0;
    posBaseEntry = 0;
    entriesSinceBaseEntry = 0;
    nMatchedChars = 0;

    ios_base::openmode mode =
        readOnly ?
        std::fstream::in :
        std::fstream::in | std::fstream::out | std::fstream::app;
    if (!Utils::exists(dir)) {
        Utils::create_directories(dir);
    }
    sb.open((dir + string("/sb")).c_str(), mode);

    if (readOnly) {
        //Load the size of the compressed blocks and initialize the other vector
        std::ifstream file(dir + string("/sb.idx"), std::ios::binary);
        file.read(reinterpret_cast<char*>(&uncompressedSize), sizeof(long));
        while (!file.eof()) {
            long pos;
            if (file.read(reinterpret_cast<char*>(&pos), sizeof(long))) {
                sizeCompressedBlocks.push_back(pos);
            }
        }
        blocks.resize(sizeCompressedBlocks.size());
        cacheVector.resize(sizeCompressedBlocks.size());
        file.close();
    } else {
        currentBuffer = factory.get();
        blocks.push_back(currentBuffer);
        addCache(0);

        //Start a compression thread
        threadRunning = true;
        compressionThread = std::thread(
                                std::bind(&StringBuffer::compressBlocks, this));
    }
}

void StringBuffer::addCache(int idx) {
    if (idx == lastBlockInCache) {
        return;
    }

    assert(maxElementsInCache > 4);
    if (elementsInCache == maxElementsInCache) {
        assert(firstBlockInCache != -1);
        int idxToRemove = firstBlockInCache;
        firstBlockInCache = cacheVector[idxToRemove].second;
        if (firstBlockInCache == -1)
            lastBlockInCache = -1;
        cacheVector[firstBlockInCache].first = -1;

        //Do not remove one of the last three blocks inserted
        while (!readOnly && idxToRemove >= blocks.size() - 3) {
            //Put back the previous vector
            if (lastBlockInCache != -1)
                cacheVector[lastBlockInCache].second = idxToRemove;
            cacheVector[idxToRemove].first = lastBlockInCache;
            cacheVector[idxToRemove].second = -1;
            lastBlockInCache = idxToRemove;
            if (firstBlockInCache == -1) {
                firstBlockInCache = idxToRemove;
            }

            //Take the new one
            idxToRemove = firstBlockInCache;
            firstBlockInCache = cacheVector[idxToRemove].second;
            if (idxToRemove == lastBlockInCache)
                lastBlockInCache = -1;
            cacheVector[firstBlockInCache].first = -1;
        }

        elementsInCache--;
        factory.release(blocks[idxToRemove]);
        blocks[idxToRemove] = NULL;
    }

    //Add idx in the cache
    if (idx == cacheVector.size()) { //It's a new block (writing mode)
        if (lastBlockInCache == -1) {
            assert(firstBlockInCache == -1);
            assert(cacheVector.size() == 0);
            firstBlockInCache = lastBlockInCache = 0;
            cacheVector.push_back(std::make_pair(-1, -1));
        } else {
            cacheVector.push_back(std::make_pair(lastBlockInCache, -1));
            cacheVector[lastBlockInCache].second = cacheVector.size() - 1;
            lastBlockInCache = cacheVector.size() - 1;
        }
    } else {
        assert(idx < cacheVector.size());
        if (lastBlockInCache == -1) {
            assert(elementsInCache == 0);
            assert(firstBlockInCache == -1);
            cacheVector[idx].first = -1;
            cacheVector[idx].second = -1;
            firstBlockInCache = lastBlockInCache = idx;
        } else {
            cacheVector[lastBlockInCache].second = idx;
            cacheVector[idx].first = lastBlockInCache;
            cacheVector[idx].second = -1;
            lastBlockInCache = idx;
        }
    }
    elementsInCache++;

    /*    int c = 0;
        int f = firstBlockInCache;
        while (f != -1) {
            f = cacheVector[f].second;
            c++;
        }
        LOG(ERRORL) << "Elements in cache " << elementsInCache << " First " << firstBlockInCache << " Last " << lastBlockInCache << " idx " << idx << " count " << c;*/
}

void StringBuffer::compressBlocks() {
    threadRunning = true;
    const int maxSize = LZ4_compressBound(SB_BLOCK_SIZE);
    char *compressedBuffer = new char[maxSize];
    while (true) {

        std::unique_lock<std::mutex> lock(_compressMutex);
        while (bufferToCompress == NULL) {
            compressWait.wait(lock);
        }
        char *currentUncompressedBuffer = bufferToCompress;
        int sCurrentUncompressedBuffer = sizeBufferToCompress;
        bufferToCompress = NULL;
        lock.unlock();

        compressWait.notify_one();

        if (currentUncompressedBuffer == FINISH_THREAD) {
            break;
        }

#if LZ4_VERSION_MAJOR > 1 || LZ4_VERSION_MINOR > 2 || (LZ4_VERSION_MINOR == 2 && LZ4_VERSION_RELEASE >= 9)
        // LZ4_compress_HC does not exist in older lz4 versions
        int cs = LZ4_compress_HC(currentUncompressedBuffer, compressedBuffer,
                                sCurrentUncompressedBuffer, maxSize,  4);
#else
        int cs = LZ4_compressHC2_limitedOutput(currentUncompressedBuffer, compressedBuffer,
                                sCurrentUncompressedBuffer, maxSize,  4);
#endif

        long totalSize = 0;

        sizeLock.lock();
        if (sizeCompressedBlocks.size() == 0) {
            sizeCompressedBlocks.push_back(cs);
        } else {
            totalSize = sizeCompressedBlocks[sizeCompressedBlocks.size() - 1];
            sizeCompressedBlocks.push_back(totalSize + cs);
        }
        sizeLock.unlock();

        fileLock.lock();
        //Since I opened the file with app, all changes happen at the end
        sb.seekp(totalSize);
        sb.write(compressedBuffer, cs);
        fileLock.unlock();
    }
    delete[] compressedBuffer;
    threadRunning = false;
    compressTerm.notify_one();
}

void StringBuffer::setCurrentAsBaseEntry(int size) {
    posBaseEntry = writingCurrentBufferSize;
    sizeBaseEntry = size;
    nMatchedChars = entriesSinceBaseEntry = 0;
}

int StringBuffer::calculatePrefixWithBaseEntry(char *origBuffer, char *string,
        int size) {
    if (size > 0 && entriesSinceBaseEntry < 1024) {
        int commonCharacters = Utils::commonPrefix((tTerm*) origBuffer,
                               posBaseEntry, posBaseEntry + sizeBaseEntry, (tTerm *) string, 0,
                               size);
        if (commonCharacters > 4 && commonCharacters >= this->nMatchedChars) {
            return commonCharacters;
        }
    }
    return 0;
}

void StringBuffer::writeVInt(int n) {
    int remSize = SB_BLOCK_SIZE - writingCurrentBufferSize;
    if (remSize < 4) {
        int nByteSize = Utils::numBytes2(n);
        if (remSize < nByteSize) {
            char supportBuffer[4];
            int offset = 0;
            int i = 0;
            offset = Utils::encode_vint2(supportBuffer, offset, n);
            while (writingCurrentBufferSize < SB_BLOCK_SIZE) {
                currentBuffer[writingCurrentBufferSize++] = supportBuffer[i++];
            }
            uncompressedSize += i;
            compressLastBlock();
            while (i < offset) {
                currentBuffer[writingCurrentBufferSize++] = supportBuffer[i++];
                uncompressedSize++;
            }

        } else {
            writingCurrentBufferSize = Utils::encode_vint2(currentBuffer,
                                       writingCurrentBufferSize, n);
            uncompressedSize += nByteSize;
        }
    } else {
        int oldSize = writingCurrentBufferSize;
        writingCurrentBufferSize = Utils::encode_vint2(currentBuffer,
                                   writingCurrentBufferSize, n);
        uncompressedSize += writingCurrentBufferSize - oldSize;
    }
}

void StringBuffer::append(char *string, int size) {

    char *origBuffer = currentBuffer;

    writeVInt(size);

    //Insert a flag of 0
    int remSize = SB_BLOCK_SIZE - writingCurrentBufferSize;
    if (remSize == 0) {
        compressLastBlock();
    }

    //Check if it is worth to refer to a previous entry
    int prefixSize = calculatePrefixWithBaseEntry(origBuffer, string, size);
    if (prefixSize == 0) {
        currentBuffer[writingCurrentBufferSize++] = 0;
        setCurrentAsBaseEntry(size);
    } else {
        currentBuffer[writingCurrentBufferSize++] = 1;
        writeVInt(posBaseEntry);
        writeVInt(prefixSize);
        size -= prefixSize;
        string += prefixSize;
        entriesSinceBaseEntry++;
        nMatchedChars = prefixSize;
    }

    remSize = SB_BLOCK_SIZE - writingCurrentBufferSize;
    if (remSize >= size) {
        memcpy(currentBuffer + writingCurrentBufferSize, string, size);
        writingCurrentBufferSize += size;
    } else {
        if (remSize > 0) {
            memcpy(currentBuffer + writingCurrentBufferSize, string, remSize);
            uncompressedSize += remSize;
            size -= remSize;
            writingCurrentBufferSize += remSize;
        }
        compressLastBlock();
        memcpy(currentBuffer, string + remSize, size);
        writingCurrentBufferSize = size;
    }
    uncompressedSize += size + 1;
}

long StringBuffer::getSize() {
    return uncompressedSize;
}

void StringBuffer::compressLastBlock() {
    std::unique_lock<std::mutex> lock(_compressMutex);
    while (bufferToCompress != NULL) {
        compressWait.wait(lock);
    }
    bufferToCompress = currentBuffer;
    sizeBufferToCompress = writingCurrentBufferSize;
    lock.unlock();

//Notify the compression thread in case it is waiting
    compressWait.notify_one();

    addCache(blocks.size());

    currentBuffer = factory.get();
    blocks.push_back(currentBuffer);
    writingCurrentBufferSize = 0;

    sizeBaseEntry = 0;
    entriesSinceBaseEntry = 0;
}

void StringBuffer::uncompressBlock(int b) {
    long start = 0;
    int length = 0;

    if (!readOnly) {
        sizeLock.lock();
        if (b > 0) {
            start = sizeCompressedBlocks[b - 1];
        }
        length = sizeCompressedBlocks[b] - start;
        sizeLock.unlock();

        fileLock.lock();
        sb.seekg(start);
        sb.read(uncompressSupportBuffer, length);
        if (!sb) {
            LOG(ERRORL) << "error: only " << sb.gcount() << " could be read";
        }
        fileLock.unlock();
    } else {
        if (b > 0) {
            start = sizeCompressedBlocks[b - 1];
        }
        length = sizeCompressedBlocks[b] - start;

        sb.seekg(start);
        sb.read(uncompressSupportBuffer, length);
        if (!sb) {
            LOG(ERRORL) << "error: only " << sb.gcount() << " could be read";
        }
    }

    char *uncompressedBuffer = factory.get();
    int sizeUncompressed = SB_BLOCK_SIZE;
    if (b == sizeCompressedBlocks.size() - 1) {
        sizeUncompressed = uncompressedSize % SB_BLOCK_SIZE;
    }
    int bytesUncompressed = LZ4_decompress_fast(uncompressSupportBuffer,
                            uncompressedBuffer,
                            sizeUncompressed);
    stats->incrNReadIndexBlocks();
    stats->addNReadIndexBytes(length);
    if (bytesUncompressed < 0) {
        LOG(ERRORL) << "Decompression of block "
                                 << b
                                 << " has failed. Read at pos "
                                 << start
                                 << " with length "
                                 << length;
    }

    blocks[b] = uncompressedBuffer;
}

int StringBuffer::getVInt(int &blockId, char *&block, int &offset) {
    //Retrieve the size of the string.
    //I use five instead of 4 because there is also a flag after it.
    if (SB_BLOCK_SIZE - offset < 4) {
        char supportBuffer[4];
        int offsetSupportBuffer = 0;
        for (int i = offset; i < SB_BLOCK_SIZE; ++i) {
            supportBuffer[offsetSupportBuffer++] = block[i];
        }

        char *nextBlock = NULL;
        if (blockId < blocks.size() - 1) {
            nextBlock = getBlock(blockId + 1);
            int startNextBlock = 0;
            while (offsetSupportBuffer < 4) {
                supportBuffer[offsetSupportBuffer++] =
                    nextBlock[startNextBlock++];
            }
        }

        int supportOffset = 0;
        int size = Utils::decode_vint2(supportBuffer, &supportOffset);

        //Update current position
        offset += supportOffset;
        if (offset >= SB_BLOCK_SIZE) {
            offset -= SB_BLOCK_SIZE;
            blockId++;
            block = nextBlock;
        }
        return size;
    } else {
        return Utils::decode_vint2(block, &offset);
    }
}

void StringBuffer::get(long pos, char* outputBuffer, int &size) {
    int idxBlock = pos / SB_BLOCK_SIZE;
    int initialIdx = idxBlock;

    char *block = getBlock(idxBlock);
    int start = pos - idxBlock * SB_BLOCK_SIZE;

    //Get the size
    size = getVInt(idxBlock, block, start);
    int sizeToCopy = size;
    //Ignore the flag
    int flag = getFlag(idxBlock, block, start);
    if (flag == 1) {
        int posPrefix = getVInt(idxBlock, block, start);
        int sizePrefix = getVInt(idxBlock, block, start);

        if (initialIdx != idxBlock) {
            char *baseTermBlock = getBlock(initialIdx);
            memcpy(outputBuffer, baseTermBlock + posPrefix, sizePrefix);
            block = getBlock(idxBlock); // It could be that the block got offloaded
        } else {
            memcpy(outputBuffer, block + posPrefix, sizePrefix);
        }
        //Copy the remaining size - sizePrefix bytes
        sizeToCopy -= sizePrefix;
        outputBuffer += sizePrefix;
    }

    if (start == SB_BLOCK_SIZE) {
        start = 0;
        idxBlock++;
        block = getBlock(idxBlock);
    }

    //Check whether the string is inside the block or not
    if (start + sizeToCopy > SB_BLOCK_SIZE) {
        int remSize = SB_BLOCK_SIZE - start;
        memcpy(outputBuffer, block + start, remSize);
        idxBlock++;
        block = getBlock(idxBlock);
        memcpy(outputBuffer + remSize, block, sizeToCopy - remSize);
    } else {
        memcpy(outputBuffer, block + start, sizeToCopy);
    }
}

char* StringBuffer::get(long pos, int &size) {
    get(pos, termSupportBuffer, size);
    termSupportBuffer[size] = '\0';
    return termSupportBuffer;
}

char *StringBuffer::getBlock(int idxBlock) {
    assert(idxBlock >= 0);
    char *block = blocks[idxBlock];
    if (block == NULL) {
        addCache(idxBlock);
        uncompressBlock(idxBlock);
        block = blocks[idxBlock];
    } else {
        //Update the cache. Move the block to the end
        assert(lastBlockInCache != -1);
        if (idxBlock != lastBlockInCache) {
            int prev = cacheVector[idxBlock].first;
            int succ = cacheVector[idxBlock].second;
            //Remove the block from the list
            if (prev != -1) {
                cacheVector[prev].second = succ;
            } else {
                firstBlockInCache = succ;
            }
            if (succ != -1) {
                cacheVector[succ].first = prev;
            }
            //Copy at the end
            cacheVector[lastBlockInCache].second = idxBlock;
            cacheVector[idxBlock].first = lastBlockInCache;
            cacheVector[idxBlock].second = -1;
            lastBlockInCache = idxBlock;
        }
    }
    return block;
}

int StringBuffer::cmp(long pos, char *string, int sizeString) {
    int startBlock = pos / SB_BLOCK_SIZE;
    const int initialBlock = startBlock;
    char *block = getBlock(startBlock);

    int blockStartPos = pos % SB_BLOCK_SIZE;
    int stringStart = 0;

    //Get the size of the term
    int size = getVInt(startBlock, block, blockStartPos);
    int flag = getFlag(startBlock, block, blockStartPos);
    if (flag == 1) {
        int posBaseTerm = getVInt(startBlock, block, blockStartPos);
        int sizeBaseTerm = getVInt(startBlock, block, blockStartPos);
        if (initialBlock != startBlock) {
            int result = Utils::prefixEquals(blocks[initialBlock] + posBaseTerm,
                                             sizeBaseTerm, string, sizeString);
            if (result != 0) {
                return result;
            }
        } else {
            //Do the comparison
            int result = Utils::prefixEquals(block + posBaseTerm, sizeBaseTerm,
                                             string, sizeString);
            if (result != 0) {
                return result;
            }
        }
        string += sizeBaseTerm;
        sizeString -= sizeBaseTerm;
        size -= sizeBaseTerm;
    }

    int stringBeginningCmp = stringStart;
    if (blockStartPos + size <= SB_BLOCK_SIZE) {
        for (int i = 0; i < size && stringStart < sizeString; ++i) {
            if (block[blockStartPos + i] != string[stringStart++]) {
                return ((int) block[blockStartPos + i] & 0xff) - ((int) string[stringStart - 1] & 0xff);
            }
        }
    } else {
        int remSize = SB_BLOCK_SIZE - blockStartPos;
        for (int i = 0; i < remSize && stringStart < sizeString; ++i) {
            if (block[blockStartPos + i] != string[stringStart++]) {
                return ((int) block[blockStartPos + i] & 0xff) - ((int) string[stringStart - 1] & 0xff);
            }
        }
        startBlock++;
        block = getBlock(startBlock);
        size -= remSize;
        stringBeginningCmp = stringStart;
        for (int i = 0; i < size && stringStart < sizeString; ++i) {
            if (block[i] != string[stringStart++]) {
                return ((int) block[i] & 0xff) - ((int) string[stringStart - 1] & 0xff);
            }
        }
    }
    return size - sizeString + stringBeginningCmp;
}

StringBuffer::~StringBuffer() {
    if (!readOnly) {
        int usedSize = uncompressedSize % SB_BLOCK_SIZE;
        if (usedSize > 0) {
            compressLastBlock();
        }

        std::unique_lock<std::mutex> lock(_compressMutex);
        while (bufferToCompress != NULL) {
            compressWait.wait(lock);
        }
        lock.unlock();

        compressionThread.detach();
        bufferToCompress = FINISH_THREAD;
        compressWait.notify_one();

        std::unique_lock<std::mutex> lock2(_compressTerm);
        compressTerm.wait(lock2, std::bind(&StringBuffer::isThreadFinished, this));

        sb.flush();
        sb.close();

        //Write all the indices
        std::ofstream file(dir + string("/sb.idx"), std::ios::binary);
        file.write(reinterpret_cast<char*>(&uncompressedSize), sizeof(long));
        for (int i = 0; i < sizeCompressedBlocks.size(); ++i) {
            file.write(reinterpret_cast<char*>(&(sizeCompressedBlocks[i])),
                       sizeof(long));
        }
        file.close();
    }

    //Check whether there are buffers to be released
    for (int i = 0; i < blocks.size(); ++i) {
        char *block = blocks[i];
        if (block != NULL) {
            factory.release(block);
        }
    }
}
