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


#include <trident/kb/permsorter.h>
#include <trident/utils/parallel.h>
#include <kognac/utils.h>
#include <kognac/compressor.h>

#include <thread>
#include <functional>
#include <array>

struct __PermSorter_sorter {
    char *rawinput;
    const int o1;
    const int o2;
    const int o3;

    __PermSorter_sorter(char *rawinput, const int o1, const int o2,
            const int o3) : rawinput(rawinput), o1(o1), o2(o2), o3(o3) {
    }

    bool operator()(long a, long b) const {
        char *start_a = rawinput + a * 15;
        char *start_b = rawinput + b * 15;
        int ret = memcmp(start_a + o1, start_b + o1, 5);
        if (ret < 0) {
            return true;
        } else if (ret == 0) {
            ret = memcmp(start_a + o2, start_b + o2, 5);
            if (ret < 0) {
                return true;
            } else if (ret == 0) {
                ret = memcmp(start_a + o3, start_b + o3, 5);
                return ret < 0;
            }
        }
        return false;
    }
};

typedef std::array<unsigned char, 15> __PermSorter_triple;

bool __PermSorter_triple_sorter(const __PermSorter_triple &a,
        const __PermSorter_triple &b) {
    return a < b;
}

void PermSorter::sortPermutation(char *start, char *end, int nthreads) {
    __PermSorter_triple *sstart = (__PermSorter_triple*) start;
    __PermSorter_triple *send = (__PermSorter_triple*) end;
    std::chrono::system_clock::time_point starttime = std::chrono::system_clock::now();
    ParallelTasks::sort_int(sstart, send, &__PermSorter_triple_sorter, nthreads);
    std::chrono::duration<double> duration = std::chrono::system_clock::now() - starttime;
    LOG(DEBUGL) << "Time sorting: " << duration.count() << "s.";
}

void PermSorter::writeTermInBuffer(char *buffer, const long n) {
    buffer[0] = (n >> 32) & 0xFF;
    buffer[1] = (n >> 24) & 0xFF;
    buffer[2] = (n >> 16) & 0xFF;
    buffer[3] = (n >> 8) & 0xFF;
    buffer[4] = n & 0xFF;
}

long PermSorter::readTermFromBuffer(char *buffer) {
    long n = 0;
    n += (long) (buffer[0] & 0xFF) << 32;
    n += (long) (buffer[1] & 0xFF) << 24;
    n += (buffer[2] & 0xFF) << 16;
    n += (buffer[3] & 0xFF) << 8;
    n += buffer[4] & 0xFF;
    return n;
}

void PermSorter::dumpPermutation_seq(
        char *start,
        char *end,
        MultiDiskLZ4Writer *writer,
        int currentPart) {
    while (start < end) {
        Triple t;
        t.s = PermSorter::readTermFromBuffer(start);
        t.p = PermSorter::readTermFromBuffer(start + 5);
        t.o = PermSorter::readTermFromBuffer(start + 10);
        t.writeTo(currentPart, writer);
        start+=15;
    }
    writer->setTerminated(currentPart);
}

bool PermSorter::isMax(char *input, long idx) {
    char *start = input + idx * 15;
    for(int i = 0; i < 15; ++i) {
        if (~(start[i]))
            return false;
    }
    return true;
}

void PermSorter::dumpPermutation(char *input, long end,
        int parallelProcesses,
        int maxReadingThreads,
        string out) {
    //Set up the multidiskwriters...
    MultiDiskLZ4Writer **writers = new MultiDiskLZ4Writer*[maxReadingThreads];
    int partsPerWriter = parallelProcesses / maxReadingThreads;
    int currentPart = 0;
    for(int i = 0; i < maxReadingThreads; ++i) {
        std::vector<string> files;
        for(int j = 0; j < partsPerWriter; ++j) {
            string o = out + string(".") + to_string(currentPart++);
            files.push_back(o);
        }
        writers[i] = new MultiDiskLZ4Writer(files, 3, 4);
    }

    //Find out the total size of the written elements
    long realSize = end;
    while (realSize > 0) {
        if (PermSorter::isMax(input, realSize - 1)) {
            realSize--;
        } else {
            break;
        }
    }

    std::thread *threads = new std::thread[parallelProcesses];
    long chunkSize = max((long)1, (long)(realSize / parallelProcesses));
    long currentEnd = 0;
    for(int i = 0; i < parallelProcesses; ++i) {
        long nextEnd;
        if (i == parallelProcesses - 1) {
            nextEnd = realSize;
        } else {
            nextEnd = currentEnd + chunkSize;
            if (nextEnd > realSize)
                nextEnd = realSize;
        }
        MultiDiskLZ4Writer *currentWriter = writers[i / partsPerWriter];
        int currentPart = i % partsPerWriter;
        if (nextEnd > currentEnd) {
            threads[i] = std::thread(PermSorter::dumpPermutation_seq,
                    input + currentEnd * 15,
                    input + nextEnd * 15,
                    currentWriter,
                    currentPart);
        } else {
            currentWriter->setTerminated(currentPart);
        }
        currentEnd = nextEnd;
    }
    for(int i = 0; i < parallelProcesses; ++i) {
        if (threads[i].joinable())
            threads[i].join();
    }
    for(int i = 0; i < maxReadingThreads; ++i) {
        delete writers[i];
    }
    delete[] writers;
    delete[] threads;
}

void PermSorter::sortChunks_seq(const int idReader,
        MultiDiskLZ4Reader *reader,
        std::vector<std::unique_ptr<char[]>> *rawTriples,
        long startIdx,
        long endIdx,
        long *count,
        std::vector<std::pair<string, char>> additionalPermutations,
        bool outputSPO) {
    char *start = rawTriples->at(0).get() + startIdx;
    char *end = rawTriples->at(0).get() + endIdx;
    char *current = start;

    long i = 0;
    if (outputSPO) {
        while (!reader->isEOF(idReader) && current < end) {
            long first = reader->readLong(idReader);
            long second = reader->readLong(idReader);
            long third = reader->readLong(idReader);
            PermSorter::writeTermInBuffer(current, first);
            PermSorter::writeTermInBuffer(current + 5, second);
            PermSorter::writeTermInBuffer(current + 10, third);
            current += 15;
            i++;
            if (i % 1000000000 == 0) {
                LOG(DEBUGL) << "Processed " << i << " triples";
            }
        }
    } else {
        while (!reader->isEOF(idReader) && current < end) {
            long first = reader->readLong(idReader);
            long second = reader->readLong(idReader);
            long third = reader->readLong(idReader);
            PermSorter::writeTermInBuffer(current, first);
            PermSorter::writeTermInBuffer(current + 5, third);
            PermSorter::writeTermInBuffer(current + 10, second);
            current += 15;
            i++;
            if (i % 1000000000 == 0) {
                LOG(DEBUGL) << "Processed " << i << " triples";
            }
        }
    }
    *count = i;
    memset(current, 0xFF, end - current);
    LOG(DEBUGL) << "Loaded " << i << " triples. Now creating other permutations";
    /************* SOP ************/
    int idxSOP = 0;
    for(auto p : additionalPermutations) {
        if (p.second == IDX_SOP) {
            break;
        }
        idxSOP++;
    }
    if (idxSOP < additionalPermutations.size()) { //Found SOP
        //Copy all array, and swap the second and third element
        char *startSOP = rawTriples->at(idxSOP + 1).get() + startIdx;
        memcpy(startSOP, start, endIdx - startIdx);
        for(long j = 0; j < i; ++j) {
            //swap p and o
            char tmp[5];
            tmp[0] = startSOP[5]; //copy p in tmp
            tmp[1] = startSOP[6];
            tmp[2] = startSOP[7];
            tmp[3] = startSOP[8];
            tmp[4] = startSOP[9];
            startSOP[5] = startSOP[10]; //copy o in p
            startSOP[6] = startSOP[11];
            startSOP[7] = startSOP[12];
            startSOP[8] = startSOP[13];
            startSOP[9] = startSOP[14];
            startSOP[10] = tmp[0]; //copy p in o
            startSOP[11] = tmp[1];
            startSOP[12] = tmp[2];
            startSOP[13] = tmp[3];
            startSOP[14] = tmp[4];
            startSOP += 15;
        }
    }
    /************* PSO ************/
    int idxPSO = 0;
    for(auto p : additionalPermutations) {
        if (p.second == IDX_PSO) {
            break;
        }
        idxPSO++;
    }
    if (idxPSO < additionalPermutations.size()) { //Found PSO
        char *startPSO = rawTriples->at(idxPSO + 1).get() + startIdx;
        memcpy(startPSO, start, endIdx - startIdx);
        for(long j = 0; j < i; ++j) {
            //swap s and p
            char tmp[5];
            tmp[0] = startPSO[0]; //copy s in tmp
            tmp[1] = startPSO[1];
            tmp[2] = startPSO[2];
            tmp[3] = startPSO[3];
            tmp[4] = startPSO[4];
            startPSO[0] = startPSO[5]; //copy p in s
            startPSO[1] = startPSO[6];
            startPSO[2] = startPSO[7];
            startPSO[3] = startPSO[8];
            startPSO[4] = startPSO[9];
            startPSO[5] = tmp[0]; //copy s in p
            startPSO[6] = tmp[1];
            startPSO[7] = tmp[2];
            startPSO[8] = tmp[3];
            startPSO[9] = tmp[4];
            startPSO += 15;
        }
    }
    /************* POS ************/
    int idxPOS = 0;
    for(auto p : additionalPermutations) {
        if (p.second == IDX_POS) {
            break;
        }
        idxPOS++;
    }
    if (idxPOS < additionalPermutations.size()) { //Found POS
        if (idxPSO >= additionalPermutations.size()) {
            LOG(ERRORL) << "I require also PSO";
            throw 10;
        }
        char *startPSO = rawTriples->at(idxPSO + 1).get() + startIdx;
        char *startPOS = rawTriples->at(idxPOS + 1).get() + startIdx;
        memcpy(startPOS, startPSO, endIdx - startIdx);
        for(long j = 0; j < i; ++j) {
            //swap s and o
            char tmp[5];
            tmp[0] = startPOS[5]; //copy s in tmp
            tmp[1] = startPOS[6];
            tmp[2] = startPOS[7];
            tmp[3] = startPOS[8];
            tmp[4] = startPOS[9];
            startPOS[5] = startPOS[10]; //copy o in s
            startPOS[6] = startPOS[11];
            startPOS[7] = startPOS[12];
            startPOS[8] = startPOS[13];
            startPOS[9] = startPOS[14];
            startPOS[10] = tmp[0]; //copy s in o
            startPOS[11] = tmp[1];
            startPOS[12] = tmp[2];
            startPOS[13] = tmp[3];
            startPOS[14] = tmp[4];
            startPOS += 15;
        }
    }

    /************* OPS ************/
    int idxOPS = 0;
    for(auto p : additionalPermutations) {
        if (p.second == IDX_OPS) {
            break;
        }
        idxOPS++;
    }
    if (idxOPS < additionalPermutations.size()) { //Found OPS
        char *startOPS = rawTriples->at(idxOPS + 1).get() + startIdx;
        memcpy(startOPS, start, endIdx - startIdx);
        for(long j = 0; j < i; ++j) {
            //swap s and o
            char tmp[5];
            tmp[0] = startOPS[0]; //copy s in tmp
            tmp[1] = startOPS[1];
            tmp[2] = startOPS[2];
            tmp[3] = startOPS[3];
            tmp[4] = startOPS[4];
            startOPS[0] = startOPS[10]; //copy o in s
            startOPS[1] = startOPS[11];
            startOPS[2] = startOPS[12];
            startOPS[3] = startOPS[13];
            startOPS[4] = startOPS[14];
            startOPS[10] = tmp[0]; //copy s in o
            startOPS[11] = tmp[1];
            startOPS[12] = tmp[2];
            startOPS[13] = tmp[3];
            startOPS[14] = tmp[4];
            startOPS += 15;
        }
    }
    /************* OSP (labelled graph) ************/
    if (outputSPO) {
        int idxOSP = 0;
        for(auto p : additionalPermutations) {
            if (p.second == IDX_OSP) {
                break;
            }
            idxOSP++;
        }
        if (idxOSP < additionalPermutations.size()) { //Found OSP
            if (idxOPS >= additionalPermutations.size()) {
                LOG(ERRORL) << "I require also OPS";
                throw 10;
            }
            char *startOPS = rawTriples->at(idxOPS + 1).get() + startIdx;
            char *startOSP = rawTriples->at(idxOSP + 1).get() + startIdx;
            memcpy(startOSP, startOPS, endIdx - startIdx);
            for(long j = 0; j < i; ++j) {
                //swap p and s
                char tmp[5];
                tmp[0] = startOSP[5]; //copy p in tmp
                tmp[1] = startOSP[6];
                tmp[2] = startOSP[7];
                tmp[3] = startOSP[8];
                tmp[4] = startOSP[9];
                startOSP[5] = startOSP[10]; //copy s in p
                startOSP[6] = startOSP[11];
                startOSP[7] = startOSP[12];
                startOSP[8] = startOSP[13];
                startOSP[9] = startOSP[14];
                startOSP[10] = tmp[0]; //copy s in o
                startOSP[11] = tmp[1];
                startOSP[12] = tmp[2];
                startOSP[13] = tmp[3];
                startOSP[14] = tmp[4];
                startOSP += 15;
            }
        }
    } else {
        /************* OSP (unlabelled graph) ************/
        int idxOSP = 0;
        for(auto p : additionalPermutations) {
            if (p.second == IDX_OSP) {
                break;
            }
            idxOSP++;
        }
        if (idxOSP < additionalPermutations.size()) { //Found OSP
            char *startOSP = rawTriples->at(idxOSP + 1).get() + startIdx;
            memcpy(startOSP, start, endIdx - startIdx);
            for(long j = 0; j < i; ++j) {
                //swap o and s
                char tmp[5];
                tmp[0] = startOSP[5]; //copy o in tmp
                tmp[1] = startOSP[6];
                tmp[2] = startOSP[7];
                tmp[3] = startOSP[8];
                tmp[4] = startOSP[9];
                startOSP[5] = startOSP[0]; //copy s in o
                startOSP[6] = startOSP[1];
                startOSP[7] = startOSP[2];
                startOSP[8] = startOSP[3];
                startOSP[9] = startOSP[4];
                startOSP[0] = tmp[0]; //copy s in o
                startOSP[1] = tmp[1];
                startOSP[2] = tmp[2];
                startOSP[3] = tmp[3];
                startOSP[4] = tmp[4];
                startOSP += 15;
            }
        }
    }
}

struct _Offset {
    char first;
    char second;
    char third;
};

void PermSorter::sortChunks(string inputdir,
        int maxReadingThreads,
        int parallelProcesses,
        long estimatedSize,
        bool outputSPO,
        std::vector<std::pair<string, char>> &additionalPermutations) {

    LOG(DEBUGL) << "Start sortChunks";
    //calculate the number of elements
    long mem = Utils::getSystemMemory() * 0.8;
    int nperms = additionalPermutations.size() + 1;
    long nelements = mem / (15 * nperms);
    long elementsMainMem = max((long)parallelProcesses,
            min(nelements, (long)(estimatedSize * 1.2)));
    //Make sure elementsMainMem is a multiple of parallelProcesses
    elementsMainMem += parallelProcesses - (elementsMainMem % parallelProcesses);

    /*** SORT THE ORIGINAL FILES IN BLOCKS OF N RECORDS ***/
    vector<string> unsortedFiles = Utils::getFiles(inputdir);
    MultiDiskLZ4Reader **readers = new MultiDiskLZ4Reader*[maxReadingThreads];
    std::vector<std::vector<string>> inputsReaders(parallelProcesses);
    int currentPart = 0;
    for(int i = 0; i < unsortedFiles.size(); ++i) {
        if (Utils::exists(unsortedFiles[i])) {
            inputsReaders[currentPart].push_back(unsortedFiles[i]);
            currentPart = (currentPart + 1) % parallelProcesses;
        }
    }
    auto itr = inputsReaders.begin();
    int filesPerReader = parallelProcesses / maxReadingThreads;
    for(int i = 0; i < maxReadingThreads; ++i) {
        readers[i] = new MultiDiskLZ4Reader(filesPerReader,
                3, 4);
        readers[i]->start();
        for(int j = 0; j < filesPerReader; ++j) {
            if (itr->empty()) {
                LOG(DEBUGL) << "Part " << j << " is empty";
            } else {
                LOG(DEBUGL) << "Part " << i << " " << j << " " << itr->at(0);
            }
            if (itr != inputsReaders.end()) {
                readers[i]->addInput(j, *itr);
                itr++;
            } else {
                std::vector<string> emptyset;
                readers[i]->addInput(j, emptyset);
            }
        }
    }
    std::thread *threads = new std::thread[parallelProcesses];
    LOG(DEBUGL) << "Creating vectors of "
        << elementsMainMem << " elements. Each el is 15 bytes";

    std::vector<std::unique_ptr<char[]>> rawTriples;
    rawTriples.push_back(std::unique_ptr<char[]>(new char[elementsMainMem * 15]));
    for(int i = 0; i < additionalPermutations.size(); ++i) {
        rawTriples.push_back(std::unique_ptr<char[]>(new char[elementsMainMem * 15]));
    }

    LOG(DEBUGL) << "Creating vectors of " << elementsMainMem << ". done";
    long maxInserts = max((long)1, (long)(elementsMainMem / parallelProcesses));
    bool isFinished = false;
    int iter = 0;

    while (!isFinished) {
        LOG(DEBUGL) << "Load in parallel all the triples from disk to the main memory";
        std::vector<long> counts(parallelProcesses);
        for (int i = 0; i < parallelProcesses; ++i) {
            MultiDiskLZ4Reader *reader = readers[i % maxReadingThreads];
            int idReader = i / maxReadingThreads;
            threads[i] = std::thread(
                    std::bind(&sortChunks_seq, idReader, reader,
                        &rawTriples,
                        (i * 15 * maxInserts),
                        ((i+1) * 15 * maxInserts),
                        &(counts[i]),
                        additionalPermutations,
                        outputSPO));
        }
        for (int i = 0; i < parallelProcesses; ++i) {
            threads[i].join();
        }

        LOG(DEBUGL) << "Fill the empty holes with new data";
        int curPart = 0;
        std::vector<std::pair<int, int>> openedStreams;
        for(int i = 0; i < parallelProcesses; ++i) {
            int idxReader = i % maxReadingThreads;
            int idxPart = i / maxReadingThreads;
            if (!readers[idxReader]->isEOF(idxPart)) {
                openedStreams.push_back(make_pair(idxReader, idxPart));
            }
        }
        std::vector<_Offset> offsets;
        for(auto p : additionalPermutations) {
            _Offset o;
            switch (p.second) {
                case IDX_SOP:
                    o.first = 0;
                    o.second = 10;
                    o.third = 5;
                    break;
                case IDX_OPS:
                    o.first = 10;
                    o.second = 5;
                    o.third = 0;
                    break;
                case IDX_OSP:
                    o.first = 5;
                    o.second = 10;
                    o.third = 0;
                    break;
                case IDX_POS:
                    o.first = 10;
                    o.second = 0;
                    o.third = 5;
                    break;
                case IDX_PSO:
                    o.first = 5;
                    o.second = 0;
                    o.third = 10;
                    break;
                default:
                    throw 10;
            }
            offsets.push_back(o);
        }
        while (curPart < parallelProcesses && !openedStreams.empty()) {
            if (counts[curPart] < maxInserts) {
                int idxCurPair = 0;

                const long startp = curPart * maxInserts * 15;
                char *start = rawTriples[0].get() + startp;
                while (idxCurPair < openedStreams.size() &&
                        counts[curPart] < maxInserts) {
                    auto pair = openedStreams[idxCurPair];
                    const long first = readers[pair.first]->readLong(pair.second);
                    const long second = readers[pair.first]->readLong(pair.second);
                    const long third = readers[pair.first]->readLong(pair.second);

                    const long starto = counts[curPart] * 15;
                    if (outputSPO) {
                        PermSorter::writeTermInBuffer(start + starto, first);
                        PermSorter::writeTermInBuffer(start + starto + 5, second);
                        PermSorter::writeTermInBuffer(start + starto + 10, third);
                    } else {
                        PermSorter::writeTermInBuffer(start + starto, first);
                        PermSorter::writeTermInBuffer(start + starto + 5, third);
                        PermSorter::writeTermInBuffer(start + starto + 10, second);
                    }

                    //Copy the triples also in the other permutations
                    for(int i = 0; i < additionalPermutations.size(); ++i) {
                        char *startperm = rawTriples[i + 1].get() + startp;
                        const _Offset o = offsets[i];
                        PermSorter::writeTermInBuffer(startperm + starto + o.first, first);
                        PermSorter::writeTermInBuffer(startperm + starto + o.second, second);
                        PermSorter::writeTermInBuffer(startperm + starto + o.third, third);
                    }

                    if (readers[pair.first]->isEOF(pair.second)) {
                        openedStreams.erase(openedStreams.begin() + idxCurPair);
                        if (idxCurPair == openedStreams.size()) {
                            idxCurPair = 0;
                        }
                    } else {
                        idxCurPair = (idxCurPair + 1) % openedStreams.size();
                    }
                    counts[curPart]++;
                }
            } else {
                curPart++;
            }
        }
        LOG(DEBUGL) << "Finished filling holes";

        LOG(DEBUGL) << "Start sorting. Processes per permutation=" << max(1, (int)(parallelProcesses / nperms));
        int nthreads = max(1, (int)(parallelProcesses / 6));
        std::thread *threads = new std::thread[additionalPermutations.size()];
        for(int i = 0; i < additionalPermutations.size(); ++i) {
            threads[i] = std::thread(
                    std::bind(&PermSorter::sortPermutation,
                        rawTriples[i+1].get(), rawTriples[i+1].get() + 15 * maxInserts * parallelProcesses, nthreads));
        }
        PermSorter::sortPermutation(rawTriples[0].get(), rawTriples[0].get() + 15 * maxInserts * parallelProcesses, nthreads);
        for(int i = 0; i < additionalPermutations.size(); ++i) {
            threads[i].join();
        }
        delete[] threads;
        LOG(DEBUGL) << "End sorting";

        LOG(DEBUGL) << "Start dumping";
        long maxValue = maxInserts * parallelProcesses;
        int j = 1;
        for(auto perm : additionalPermutations) {
            string outputFile = perm.first + string("/sorted-") + to_string(iter++);
            PermSorter::dumpPermutation(rawTriples[j++].get(),
                    maxValue,
                    parallelProcesses,
                    maxReadingThreads,
                    outputFile);
        }
        string outputFile = inputdir + string("/sorted-") + to_string(iter++);
        PermSorter::dumpPermutation(rawTriples[0].get(),
                maxValue,
                parallelProcesses,
                maxReadingThreads,
                outputFile);
        LOG(DEBUGL) << "End dumping";

        //Are all files read?
        int i = 0;
        for(; i < parallelProcesses; ++i) {
            if (!readers[i % maxReadingThreads]->isEOF(i / maxReadingThreads)) {
                break;
            }
        }
        isFinished = i == parallelProcesses;
        if (!isFinished) {
            LOG(DEBUGL) << "One round is not enough";
        }
    }

    for(int i = 0; i < maxReadingThreads; ++i) {
        delete readers[i];
    }
    for(auto inputFile : unsortedFiles)
        Utils::remove(inputFile);
    delete[] readers;
    delete[] threads;
}
