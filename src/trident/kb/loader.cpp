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


#include <trident/loader.h>
#include <trident/kb/memoryopt.h>
#include <trident/kb/kb.h>
#include <trident/kb/schema.h>
#include <trident/kb/permsorter.h>

#include <trident/tree/nodemanager.h>

#include <trident/utils/tridentutils.h>

#include <kognac/lz4io.h>
#include <kognac/utils.h>
#include <kognac/triplewriters.h>
#include <kognac/compressor.h>
#include <kognac/hashcompressor.h>
#include <kognac/sorter.h>
#include <kognac/schemaextractor.h>
#include <kognac/kognac.h>

#include <boost/filesystem.hpp>
#include <boost/timer.hpp>
#include <boost/iostreams/filtering_stream.hpp>
#include <boost/iostreams/filter/gzip.hpp>
#include <boost/algorithm/string.hpp>

#include <tbb/parallel_sort.h>
#include <tbb/task_scheduler_init.h>

#include <mutex>
#include <condition_variable>
#include <sstream>
#include <limits>
#include <fstream>
#include <iostream>
#include <cstdio>
#include <unordered_map>
#include <cstdlib>

namespace fs = boost::filesystem;
namespace io = boost::iostreams;

bool _sorter_spo(const Triple &a, const Triple &b) {
    if (a.s < b.s) {
        return true;
    } else if (a.s == b.s) {
        if (a.p < b.p) {
            return true;
        } else if (a.p == b.p) {
            if (a.o < b.o) {
                return true;
            }
        }
    }
    return false;
}

bool _sorter_sop(const Triple &a, const Triple &b) {
    if (a.s < b.s) {
        return true;
    } else if (a.s == b.s) {
        if (a.o < b.o) {
            return true;
        } else if (a.o == b.o) {
            if (a.p < b.p) {
                return true;
            }
        }
    }
    return false;
}

bool _sorter_ops(const Triple &a, const Triple &b) {
    if (a.o < b.o) {
        return true;
    } else if (a.o == b.o) {
        if (a.p < b.p) {
            return true;
        } else if (a.p == b.p) {
            if (a.s < b.s) {
                return true;
            }
        }
    }
    return false;
}

bool _sorter_osp(const Triple &a, const Triple &b) {
    if (a.o < b.o) {
        return true;
    } else if (a.o == b.o) {
        if (a.s < b.s) {
            return true;
        } else if (a.s == b.s) {
            if (a.p < b.p) {
                return true;
            }
        }
    }
    return false;
}

bool _sorter_pos(const Triple &a, const Triple &b) {
    if (a.p < b.p) {
        return true;
    } else if (a.p == b.p) {
        if (a.o < b.o) {
            return true;
        } else if (a.o == b.o) {
            if (a.s < b.s) {
                return true;
            }
        }
    }
    return false;
}

bool _sorter_pso(const Triple &a, const Triple &b) {
    if (a.p < b.p) {
        return true;
    } else if (a.p == b.p) {
        if (a.s < b.s) {
            return true;
        } else if (a.s == b.s) {
            if (a.o < b.o) {
                return true;
            }
        }
    }
    return false;
}

void FlatTreeWriter::write(const long key,
        long n_sop,
        char strat_sop,
        short file_sop,
        long pos_sop,
        long n_osp,
        char strat_osp,
        short file_osp,
        long pos_osp) {
    char supportBuffer[40];
    //key (5 bytes), [nelements (5bytes) strat (1 byte), file (2 bytes), pos (5 bytes)]*2. Written little endian
    char *cKey = (char*) &key;
    supportBuffer[0] = cKey[0];
    supportBuffer[1] = cKey[1];
    supportBuffer[2] = cKey[2];
    supportBuffer[3] = cKey[3];
    supportBuffer[4] = cKey[4];

    if (n_sop > 0) {
        char *cnels = (char*) &n_sop;
        supportBuffer[5] = cnels[0];
        supportBuffer[6] = cnels[1];
        supportBuffer[7] = cnels[2];
        supportBuffer[8] = cnels[3];
        supportBuffer[9] = cnels[4];
        supportBuffer[10] = strat_sop;
        char *cfile = (char*) &file_sop;
        supportBuffer[11] = cfile[0];
        supportBuffer[12] = cfile[1];
        cnels = (char*) &pos_sop;
        supportBuffer[13] = cnels[0];
        supportBuffer[14] = cnels[1];
        supportBuffer[15] = cnels[2];
        supportBuffer[16] = cnels[3];
        supportBuffer[17] = cnels[4];
    } else {
        supportBuffer[5] = 0;
        supportBuffer[6] = 0;
        supportBuffer[7] = 0;
        supportBuffer[8] = 0;
        supportBuffer[9] = 0;
    }
    if (n_osp > 0) {
        char *cnels = (char*) &n_osp;
        supportBuffer[18] = cnels[0];
        supportBuffer[19] = cnels[1];
        supportBuffer[20] = cnels[2];
        supportBuffer[21] = cnels[3];
        supportBuffer[22] = cnels[4];
        supportBuffer[23] = strat_osp;
        char *cfile = (char*) &file_osp;
        supportBuffer[24] = cfile[0];
        supportBuffer[25] = cfile[1];
        cnels = (char*) &pos_osp;
        supportBuffer[26] = cnels[0];
        supportBuffer[27] = cnels[1];
        supportBuffer[28] = cnels[2];
        supportBuffer[29] = cnels[3];
        supportBuffer[30] = cnels[4];
    } else {
        supportBuffer[18] = 0;
        supportBuffer[19] = 0;
        supportBuffer[20] = 0;
        supportBuffer[21] = 0;
        supportBuffer[22] = 0;
    }
    if (!undirected) {
        ofs.write(supportBuffer, 31);
    } else {
        ofs.write(supportBuffer, 18); //I only use SOP, since the other is always 0
    }
}

long Loader::parseSnapFile(string inputtriples,
        string inputdict,
        string *permDirs,
        int nperms,
        int signaturePerm,
        string fileNameDictionaries,
        int maxReadingThreads,
        int parallelProcesses) {
    long counter = 0;
    std::map<long, long> dictionary;
    LOG(DEBUG) << "Loading input graph from " << inputtriples;

    //Create the permutations
    std::vector<Triple> triples;
    ifstream rawFile2;
    rawFile2.open(inputtriples);
    boost::iostreams::filtering_istream compressedFile2;
    if (boost::algorithm::ends_with(inputtriples, ".gz")) {
        compressedFile2.push(io::gzip_decompressor());
    }
    compressedFile2.push(rawFile2);
    string line, origline;

    while(std::getline(compressedFile2, line)) {
        origline = line;
        char delim = '\t';
        if (line[0] != '#') {
            long s,o;
            auto pos = line.find(delim);
            if (pos == string::npos) {
                if (delim == '\t') {
                    delim = ' ';
                    pos = line.find(delim);
                    if (pos == string::npos) {
                        LOG(ERROR) << "Failed parsing the SNAP file (no delim)";
                        throw 10;
                    }
                }
            }
            string ss = line.substr(0, pos);
            s = stol(ss);
            line = line.substr(pos + 1);
            o = stol(line);

            if (dictionary.count(s)) {
                s = dictionary.find(s)->second;
            } else {
                dictionary.insert(std::make_pair(s, counter));
                s = counter++;
            }

            if (dictionary.count(o)) {
                o = dictionary.find(o)->second;
            } else {
                dictionary.insert(std::make_pair(o, counter));
                o = counter++;
            }
            triples.push_back(Triple(s, 0, o));
        }
    }
    LOG(DEBUG) << "Loaded a vocabulary of " << dictionary.size();
    int detailPerms[6];
    Compressor::parsePermutationSignature(signaturePerm, detailPerms);
    for(int i = 0; i < nperms; ++i) {
        int perm = detailPerms[i];
        LZ4Writer writer(permDirs[i] + "/input-00");
        for (auto t : triples) {
            switch (perm) {
                case IDX_SPO:
                    writer.writeLong(t.s);
                    writer.writeLong(t.p);
                    writer.writeLong(t.o);
                    break;
                case IDX_OPS:
                    writer.writeLong(t.o);
                    writer.writeLong(t.p);
                    writer.writeLong(t.s);
                    break;
                case IDX_SOP:
                    writer.writeLong(t.s);
                    writer.writeLong(t.o);
                    writer.writeLong(t.p);
                    break;
                case IDX_OSP:
                    writer.writeLong(t.o);
                    writer.writeLong(t.s);
                    writer.writeLong(t.p);
                    break;
                case IDX_PSO:
                    writer.writeLong(t.p);
                    writer.writeLong(t.s);
                    writer.writeLong(t.o);
                    break;
                case IDX_POS:
                    writer.writeLong(t.p);
                    writer.writeLong(t.o);
                    writer.writeLong(t.s);
                    break;
            }
        }
    }

    //Store the dictionary. I must ensure that the data is ordered by key.
    std::map<long, long> inverseDict;
    for(const auto &it : dictionary) {
        inverseDict.insert(std::make_pair(it.second, it.first));
    }
    LZ4Writer outputDict(fileNameDictionaries);
    char *support = new char[MAX_TERM_SIZE];
    for(const auto &it : inverseDict) {
        outputDict.writeLong(it.first);
        string text = to_string(it.second);
        Utils::encode_short(support, text.length());
        memcpy(support + 2, text.c_str(), text.length());
        outputDict.writeString(support, text.length() + 2);
    }
    delete[] support;
    return triples.size();
}

void Loader::createPermsAndDictsFromFiles_seq(DiskReader *reader,
        DiskLZ4Writer *writer, int id, long *output) {
    size_t sizebuffer = 0;
    bool gzipped = false;
    char *buffer = reader->getfile(sizebuffer, gzipped);
    std::vector<char> uncompressedByteArray;

    long processedtriples = 0;
    while (buffer != NULL) {
        //Read the file
        char *input = NULL;
        size_t sizeinput = 0;
        if (gzipped) {
            //LOG(DEBUG) << "Uncompressing buffer ...";
            io::filtering_ostream os;
            os.push(io::gzip_decompressor());
            os.push(io::back_inserter(uncompressedByteArray));
            io::write(os, buffer, sizebuffer);
            os.flush();
            input = &(uncompressedByteArray[0]);
            sizeinput = uncompressedByteArray.size();
            //LOG(DEBUG) << "Uncompressing buffer (done)";
        } else {
            input = buffer;
            sizeinput = sizebuffer;
        }
        if (sizeinput == 0) {
            LOG(DEBUG) << "This should not happen";
            throw 10;
        }

        //Read every char unit all file is processed
        char *start = input;
        char *end = input + sizeinput;
        char *tkn = start;
        int pos = 0;
        long triple[3];
        while (start < end) {
            if (*start < 48 || *start > 57) {
                if (start > tkn) {
                    triple[pos++] = std::stol(string(tkn, start - tkn));
                }
                tkn = start + 1;
            }
            if (*start == '\n') {
                if (pos > 1) {
                    if (pos == 2) {
                        //Add an empty predicate
                        writer->writeLong(id, triple[0]);
                        writer->writeLong(id, 0);
                        writer->writeLong(id, triple[1]);
                    } else {
                        writer->writeLong(id, triple[0]);
                        writer->writeLong(id, triple[1]);
                        writer->writeLong(id, triple[2]);
                    }
                } else {
                    LOG(ERROR) << "Strange, should not happen... " << pos;
                }
                pos = 0;
                processedtriples++;
                if (processedtriples % 1000000000 == 0) {
                    LOG(DEBUG) << "Processed " << processedtriples;
                }
            }
            start++;
        }
        if (start > tkn) {
            triple[pos++] = std::stol(tkn);
        }
        tkn = start + 1;
        if (pos > 1) {
            if (pos == 2) {
                //Add an empty predicate
                writer->writeLong(id, triple[0]);
                writer->writeLong(id, 0);
                writer->writeLong(id, triple[1]);
            } else {
                writer->writeLong(id, triple[0]);
                writer->writeLong(id, triple[1]);
                writer->writeLong(id, triple[2]);
            }
        }

        if (gzipped) {
            uncompressedByteArray.clear();
        }
        reader->releasefile(buffer);
        buffer = reader->getfile(sizebuffer, gzipped);
    }
    writer->setTerminated(id);
    *output = processedtriples;
}

long Loader::createPermsAndDictsFromFiles(string inputtriples,
        string inputdict,
        string *permDirs,
        int nperms,
        int signaturePerm,
        string fileNameDictionaries,
        int nreadThreads,
        int nthreads) {

    //Create the dictionary output file
    if (inputdict != "") {
        LOG(DEBUG) << "Start converting dictionary file(s)";
        //Check whether it is a file or a sequence of files...
        std::vector<string> inputfiles;
        if (fs::exists(inputdict)) {
            inputfiles.push_back(inputdict);
        } else {
            fs::path path(inputdict);
            inputfiles = Utils::getFilesWithPrefix(path.parent_path().string(), path.filename().string());
            std::sort(inputfiles.begin(), inputfiles.end());
            for(int i = 0; i < inputfiles.size(); ++i) {
                inputfiles[i] = path.parent_path().string() + string("/") + inputfiles[i];
            }
        }

        char *support = new char[MAX_TERM_SIZE + 2];
        ifstream rawFile;
        LZ4Writer outputDict(fileNameDictionaries);
        for (auto f : inputfiles) {
            LOG(DEBUG) << "Converting " << f;
            rawFile.open(f);
            boost::iostreams::filtering_istream compressedFile;
            compressedFile.push(io::gzip_decompressor());
            compressedFile.push(rawFile);
            string line;
            while (std::getline(compressedFile,line)) {
                int pos = line.find(' ');
                string sId = line.substr(0, pos);
                long id = stol(sId);
                line = line.substr(pos + 1);
                pos = line.find(' ');
                string sLen = line.substr(0, pos);
                int len = stoi(sLen);
                line = line.substr(pos + 1);
                outputDict.writeLong(id);

                Utils::encode_short(support, len);
                memcpy(support + 2, line.c_str(), len);
                outputDict.writeString(support, len + 2);
            }
            rawFile.close();
        }
        delete[] support;
    } else {
        LOG(DEBUG) << "No dict file was provided";
    }

    //Create the permutations
    long ntriples = 0;
    if (fs::exists(inputtriples)) {
        ifstream rawFile2;
        rawFile2.open(inputtriples);
        boost::iostreams::filtering_istream compressedFile2;
        compressedFile2.push(io::gzip_decompressor());
        compressedFile2.push(rawFile2);
        string line;
        LOG(DEBUG) << "Start converting triple file";
        LZ4Writer writer(permDirs[0] + "/input-0");
        while(std::getline(compressedFile2, line)) {
            long s,p,o;
            int pos = line.find(' ');
            string ss = line.substr(0, pos);
            s = stol(ss);
            line = line.substr(pos + 1);
            pos = line.find(' ');
            string sp = line.substr(0, pos);
            line = line.substr(pos + 1);
            p = stol(sp);
            o = stol(line);
            for(int i = 0; i < nperms; ++i) {
                writer.writeLong(s);
                writer.writeLong(p);
                writer.writeLong(o);
            }
            ntriples++;
        }
    } else {
        // Load all the files in parallel
        fs::path path(inputtriples);
        vector<FileInfo> *files = Compressor::splitInputInChunks(path.parent_path().string(), nreadThreads, path.filename().string());
        std::thread *threads = new std::thread[nthreads];
        std::thread *threadReaders = new std::thread[nreadThreads];
        DiskReader **readers = new DiskReader*[nreadThreads];
        MultiDiskLZ4Writer **writers = new MultiDiskLZ4Writer*[nreadThreads];
        int j = 0;
        for (int i = 0; i < nreadThreads; ++i) {
            readers[i] = new DiskReader(max(2,
                        (int)(nthreads / nreadThreads) * 2), &files[i]);
            threadReaders[i] = std::thread(std::bind(&DiskReader::run, readers[i]));
            std::vector<string> files;
            int threadsPerPart = nthreads / nreadThreads;
            for(int m = 0; m < threadsPerPart; ++m) {
                files.push_back(permDirs[0] + "/input-" + to_string(j++));
            }
            writers[i] = new MultiDiskLZ4Writer(
                    files,
                    threadsPerPart, 3);
        }

        long *outputs = new long[nthreads];
        for(int i = 0; i < nthreads; ++i) {
            outputs[i] = 0;
            threads[i] = std::thread(
                    std::bind(&Loader::createPermsAndDictsFromFiles_seq,
                        readers[i % nreadThreads],
                        writers[i % nreadThreads],
                        i / nreadThreads,
                        outputs + i));
        }

        //Delete the datastructures
        for (int i = 0; i < nthreads; ++i) {
            if (threads[i].joinable())
                threads[i].join();
            ntriples += outputs[i];
        }
        for (int i = 0; i < nreadThreads; ++i) {
            delete readers[i];
            delete writers[i];
        }
        delete[] readers;
        delete[] writers;
        delete[] threadReaders;
        delete[] files;
    }
    return ntriples;
}

void Loader::parallelmerge(FileMerger<Triple> *merger,
        int buffersize,
        std::vector<long*> *buffers,
        std::mutex *m_buffers,
        std::condition_variable *cond_buffers,
        std::list <
        std::pair<long*, int >> *exchangeBuffers,
        std::mutex *m_exchange,
        std::condition_variable *cond_exchange) {
    long *buffer = NULL;
    std::unique_lock<std::mutex> l(*m_buffers);
    while (buffers->empty()) {
        cond_buffers->wait(l);
    }
    buffer = buffers->back();
    buffers->pop_back();
    l.unlock();

    int i = 0;
    long prevKey = -1;
    while (!merger->isEmpty()) {
        Triple t = merger->get();
        if (i == buffersize) {
            //Push the buffer to the consumer
            std::unique_lock<std::mutex> le(*m_exchange);
            exchangeBuffers->push_back(make_pair(buffer, i));
            le.unlock();
            cond_exchange->notify_one();

            //Get a new buffer
            l.lock();
            while (buffers->empty()) {
                cond_buffers->wait(l);
            }
            buffer = buffers->back();
            buffers->pop_back();
            l.unlock();
            i = 0;
        }
        buffer[i++] = t.s;
        buffer[i++] = t.p;
        buffer[i++] = t.o;
        buffer[i++] = t.count;

        if (t.s < prevKey && prevKey != -1) {
            LOG(ERROR) << "PMERGE: KEY: " << t.s << " PREVKEY: " << prevKey;
        }
        prevKey = t.s;
    }

    //Edge case. If it terminates exactly on sizebuffer, then the consumer will not terminate
    if (i == buffersize) {
        //Push the buffer to the consumer
        std::unique_lock<std::mutex> le(*m_exchange);
        exchangeBuffers->push_back(make_pair(buffer, i));
        le.unlock();
        cond_exchange->notify_one();

        //Get a new buffer
        l.lock();
        while (buffers->empty()) {
            cond_buffers->wait(l);
        }
        buffer = buffers->back();
        buffers->pop_back();
        l.unlock();
        i = 0;
    }

    //Push the last buffer to the consumer. Now i < sizebuffer.
    std::unique_lock<std::mutex> le(*m_exchange);
    exchangeBuffers->push_back(make_pair(buffer, i));
    le.unlock();
    cond_exchange->notify_one();
}

template<class K>
void Loader::dumpPermutation_seq(K *start, K *end,
        MultiDiskLZ4Writer *writer,
        int currentPart,
        char sorter) {
    if (sorter == IDX_SPO) {
        while (start < end) {
            Triple t;
            start->toTriple(t);
            t.writeTo(currentPart, writer);
            start++;
        }
    } else {
        long box1, box2;
        while (start < end) {
            Triple t;
            start->toTriple(t);
            switch (sorter) {
                case IDX_SOP:
                    box1 = t.o;
                    t.o = t.p;
                    t.p = box1;
                    break;
                case IDX_OSP:
                    box1 = t.s;
                    box2 = t.p;
                    t.s = t.o;
                    t.p = box1;
                    t.o = box2;
                    break;
                case IDX_OPS:
                    box1 = t.s;
                    t.s = t.o;
                    t.o = box1;
                    break;
                case IDX_POS:
                    box1 = t.s;
                    box2 = t.p;
                    t.s = box2;
                    t.p = t.o;
                    t.o = box1;
                    break;
                case IDX_PSO:
                    box1 = t.p;
                    t.p = t.s;
                    t.s = box1;
                    break;
            }
            t.writeTo(currentPart, writer);
            start++;
        }
    }
    writer->setTerminated(currentPart);
}

template<class K>
void Loader::dumpPermutation(std::vector<K> &input,
        long parallelProcesses,
        long maxReadingThreads,
        long maxValue,
        string out,
        char sorter) {
    tbb::task_scheduler_init init(parallelProcesses);
    LOG(DEBUG) << "Start sorting";
    switch (sorter) {
        case IDX_SPO:
            tbb::parallel_sort(input.begin(), input.begin() + maxValue, K::sLess);
            break;
        case IDX_SOP:
            tbb::parallel_sort(input.begin(), input.begin() + maxValue, K::sLess_sop);
            break;
        case IDX_OSP:
            tbb::parallel_sort(input.begin(), input.begin() + maxValue, K::sLess_osp);
            break;
        case IDX_OPS:
            tbb::parallel_sort(input.begin(), input.begin() + maxValue, K::sLess_ops);
            break;
        case IDX_POS:
            tbb::parallel_sort(input.begin(), input.begin() + maxValue, K::sLess_pos);
            break;
        case IDX_PSO:
            tbb::parallel_sort(input.begin(), input.begin() + maxValue, K::sLess_pso);
            break;
        default:
            throw 10;
    }
    LOG(DEBUG) << "End sorting. Start dumping";

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
    long realSize = maxValue;
    while (realSize > 0) {
        if (K::ismax(input[realSize - 1])) {
            realSize--;
        } else {
            break;
        }
    }
    LOG(DEBUG) << "MaxSize=" << maxValue << " realSize=" << realSize;

    K *rawInput = NULL;
    if (input.size() > 0)
        rawInput = &(input[0]);

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
            threads[i] = std::thread(dumpPermutation_seq<K>,
                    rawInput + currentEnd,
                    rawInput + nextEnd,
                    currentWriter,
                    currentPart,
                    sorter);
        } else {
            currentWriter->setTerminated(currentPart);
        }
        currentEnd = nextEnd;
    }
    for(int i = 0; i < parallelProcesses; ++i) {
        threads[i].join();
    }
    for(int i = 0; i < maxReadingThreads; ++i) {
        delete writers[i];
    }
    delete[] writers;
    delete[] threads;
    LOG(DEBUG) << "Finished dumping";
}

template<class K>
void Loader::sortPermutation_seq(
        const int idReader,
        MultiDiskLZ4Reader *reader,
        long start,
        K *output,
        long maxInserts,
        long *count) {

    long i = 0;
    long currentIdx = start;
    while (!reader->isEOF(idReader) && i < maxInserts) {
        //long t1 = reader->readLong(idReader);
        //long t2 = reader->readLong(idReader);
        //long t3 = reader->readLong(idReader);
        //output[currentIdx].first = t1;
        //output[currentIdx].second = t2;
        //output[currentIdx].third = t3;
        K t;
        t.readFrom(idReader, reader);
        output[currentIdx] = t;

        currentIdx++;
        i++;
        if (i % 100000000 == 0) {
            LOG(DEBUG) << "Processed " << i << " triples";
        }
    }
    *count = i;
    while (i < maxInserts) {
        output[currentIdx] = K::max();
        //output[currentIdx].first = ~0lu;
        //output[currentIdx].second = ~0lu;
        //output[currentIdx].third = ~0lu;
        i++;
        currentIdx++;
    }
    LOG(DEBUG) << "Finished";
}

template<class K>
void Loader::sortPermutation(string inputDir,
        int maxReadingThreads,
        int parallelProcesses,
        bool initialSorting,
        long estimatedSize,
        long elementsMainMem,
        int filesToMerge,
        bool readFirstByte,
        std::vector<std::pair<string, char>> &additionalPermutations) {

    /*** SORT THE ORIGINAL FILES IN BLOCKS OF N RECORDS ***/
    if (initialSorting) {
        vector<string> unsortedFiles = Utils::getFiles(inputDir);
        MultiDiskLZ4Reader **readers = new MultiDiskLZ4Reader*[maxReadingThreads];
        std::vector<std::vector<string>> inputsReaders(parallelProcesses);
        int currentPart = 0;
        for(int i = 0; i < unsortedFiles.size(); ++i) {
            if (fs::exists(fs::path(unsortedFiles[i]))) {
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
                    LOG(DEBUG) << "Part " << j << " is empty";
                } else {
                    LOG(DEBUG) << "Part " << i << " " << j << " " << itr->at(0);
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
        //Should I read a first byte?
        if (readFirstByte) {
            for(int i = 0; i < maxReadingThreads; ++i) {
                for(int j = 0; j < filesPerReader; ++j) {
                    if (!readers[i]->isEOF(j)) {
                        char b = readers[i]->readByte(j);
                        if (b != 1) {
                            //Strange...
                            throw 10;
                        }
                    }
                }
            }
        }
        std::thread *threads = new std::thread[parallelProcesses];

        elementsMainMem = max((long)parallelProcesses,
                min(elementsMainMem, (long)(estimatedSize * 1.2)));
        LOG(DEBUG) << "Creating a vector of " << elementsMainMem << " " << sizeof(L_Triple);
        std::vector<K> triples(elementsMainMem);
        LOG(DEBUG) << "Creating a vector of " << elementsMainMem << ". done";

        K *rawTriples = NULL;
        if (triples.size() > 0) {
            rawTriples = &(triples[0]);
        }
        long maxInserts = max((long)1, (long)(elementsMainMem / parallelProcesses));
        bool isFinished = false;
        int iter = 0;

        LOG(DEBUG) << "Start loop";
        while (!isFinished) {
            //Load in parallel all the triples from disk to the main memory
            std::vector<long> counts(parallelProcesses);
            for (int i = 0; i < parallelProcesses; ++i) {
                MultiDiskLZ4Reader *reader = readers[i % maxReadingThreads];
                int idReader = i / maxReadingThreads;
                threads[i] = std::thread(
                        std::bind(&sortPermutation_seq<K>, idReader, reader,
                            i * maxInserts, rawTriples,
                            maxInserts, &(counts[i])));
            }
            for (int i = 0; i < parallelProcesses; ++i) {
                threads[i].join();
            }

            LOG(DEBUG) << "Fill the empty holes with new data";
            int curPart = 0;
            std::vector<std::pair<int, int>> openedStreams;
            for(int i = 0; i < parallelProcesses; ++i) {
                int idxReader = i % maxReadingThreads;
                int idxPart = i / maxReadingThreads;
                if (!readers[idxReader]->isEOF(idxPart)) {
                    openedStreams.push_back(make_pair(idxReader, idxPart));
                }
            }
            while (curPart < parallelProcesses && !openedStreams.empty()) {
                if (counts[curPart] < maxInserts) {
                    int idxCurPair = 0;
                    while (idxCurPair < openedStreams.size() &&
                            counts[curPart] < maxInserts) {
                        auto pair = openedStreams[idxCurPair];
                        K t;
                        t.readFrom(pair.second, readers[pair.first]);
                        rawTriples[curPart * maxInserts + counts[curPart]] = t;
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
            LOG(DEBUG) << "Finished filling holes";

            //Sort and dump the results to disk
            long maxValue = maxInserts * parallelProcesses;
            string outputFile = inputDir + string("/sorted-") + to_string(iter++);
            dumpPermutation(triples,
                    parallelProcesses,
                    maxReadingThreads,
                    maxValue, outputFile, IDX_SPO);
            for (auto perm : additionalPermutations) {
                outputFile = perm.first + string("/sorted-") + to_string(iter++);
                dumpPermutation(triples,
                        parallelProcesses,
                        maxReadingThreads,
                        maxValue, outputFile, perm.second);
            }

            //Are all files read?
            int i = 0;
            for(; i < parallelProcesses; ++i) {
                if (!readers[i % maxReadingThreads]->isEOF(i / maxReadingThreads)) {
                    break;
                }
            }
            isFinished = i == parallelProcesses;
            if (!isFinished) {
                LOG(DEBUG) << "One round is not enough";
            }
        }

        for(int i = 0; i < maxReadingThreads; ++i) {
            delete readers[i];
        }
        for(auto inputFile : unsortedFiles)
            fs::remove(fs::path(inputFile));
        delete[] readers;
        delete[] threads;
    }

    //Do the merge-sort from the files on disk

}

void Loader::sortAndInsert(ParamSortAndInsert params) {
    int permutation = params.permutation;
    int nindices = params.nindices;
    int parallelProcesses = params.parallelProcesses;
    int maxReadingThreads = params.maxReadingThreads;
    bool inputSorted = params.inputSorted;
    string inputDir = params.inputDir;
    string *POSoutputDir = params.POSoutputDir;
    TreeWriter *treeWriter = params.treeWriter;
    Inserter *ins = params.ins;
    const bool aggregated = params.aggregated;
    const bool canSkipTables = params.canSkipTables;
    const bool storeRaw = params.storeRaw;
    SimpleTripleWriter *sampleWriter = params.sampleWriter;
    double sampleRate = params.sampleRate;
    bool printstats = params.printstats;
    //SinkPtr logPtr = params.logPtr;
    bool removeInput = params.removeInput;
    long estimatedSize = params.estimatedSize;
    bool deletePreviousExt = params.deletePreviousExt;

    SimpleTripleWriter *posWriter = NULL;
    if (POSoutputDir != NULL) {
        posWriter = new SimpleTripleWriter(*POSoutputDir, "inputAggr-" +
                to_string(permutation), true);
    }

    //Sort the triples and store them into files.
    LOG(DEBUG) << "Start sorting...";
    //Calculate the maximum amount of main memory I can use
    long mem = Utils::getSystemMemory() * 0.7 / nindices;
    long nelements = mem / sizeof(L_Triple);
    LOG(DEBUG) << "Triples I can store in main memory: " << nelements <<
        " size triple " << sizeof(L_Triple);
    std::vector<std::pair<string, char>> additionalPermutations; //This parameter is unused here. I use it somewhere else
    if (!aggregated) {
        sortPermutation<L_Triple>(inputDir, maxReadingThreads, parallelProcesses,
                !inputSorted, estimatedSize, nelements, 16, false,
                additionalPermutations);
    } else {
        sortPermutation<L_TripleCount>(inputDir, maxReadingThreads, parallelProcesses,
                !inputSorted, estimatedSize, nelements, 16, true,
                additionalPermutations);
    }
    LOG(DEBUG) << "...completed.";

    LOG(DEBUG) << "Start inserting...";
    long ps, pp, po; //Previous values. Used to remove duplicates.
    ps = pp = po = -1;
    long count = 0;
    long countInput = 0;
    const int randThreshold = sampleRate * 100;
    assert(sampleWriter == NULL || randThreshold > 0);

    auto inputmerge = Utils::getFiles(inputDir, true);
    FileMerger<Triple> merger(inputmerge, true, deletePreviousExt);
    LZ4Writer *plainWriter = NULL;
    if (storeRaw) {
        std::string file = ins->getPathPermutationStorage(permutation) + std::string("raw");
        plainWriter = new LZ4Writer(file);
    }
    bool first = true;

    if (parallelProcesses > 6) {
        LOG(DEBUG) << "Parallel insert";
        std::mutex m_buffers;
        std::condition_variable cond_buffers;
        std::vector<long*> buffers;

        std::mutex m_exchange;
        std::condition_variable cond_exchange;
        std::list<std::pair<long*, int>> exchangeBuffers;

        const int sizebuffer = 4 * 1000000 * 4;
        for (int i = 0; i < 3; ++i) { //Create three buffers
            buffers.push_back(new long[sizebuffer]);
        }

        //Start one thread to merge the files in a single stream. Put the results
        //in a synchronized queue
        std::thread t = std::thread(
                std::bind(Loader::parallelmerge,
                    &merger,
                    sizebuffer,
                    &buffers,
                    &m_buffers,
                    &cond_buffers,
                    &exchangeBuffers,
                    &m_exchange,
                    &cond_exchange));

        //I read the stream and process it
        int countrandom = 0;
        do {
            std::unique_lock<std::mutex> le(m_exchange);
            while (exchangeBuffers.empty()) {
                cond_exchange.wait(le);
            }
            std::pair<long*, int> b = exchangeBuffers.front();
            exchangeBuffers.pop_front();
            le.unlock();

            const int currentsize = b.second;
            //Do the processing
            long *buf = b.first;

            for (int i = 0; i < currentsize; i += 4) {
                countInput++;
                const long s = buf[i];
                const long p = buf[i + 1];
                const long o = buf[i + 2];
                const long countt = buf[i + 3];
                if (count % 5000000000 == 0) {
                    LOG(DEBUG) << "..." << count << "...";
                }

                if (o != po || p != pp || s != ps) {
                    count++;
                    ins->insert(permutation, s, p, o, countt, posWriter,
                            treeWriter,
                            aggregated,
                            canSkipTables);
                    ps = s;
                    pp = p;
                    po = o;

                    if (storeRaw) {
                        plainWriter->writeVLong(s);
                        plainWriter->writeVLong(p);
                        plainWriter->writeVLong(o);
                    }

                    if (sampleWriter != NULL) {
                        if (first || countrandom < randThreshold) {
                            sampleWriter->write(s, p, o);
                            first = false;
                        }
                        countrandom = (countrandom + 1) % 100;
                    }
                }
            }

            //Release the buffer
            std::unique_lock<std::mutex> l(m_buffers);
            buffers.push_back(b.first);
            l.unlock();
            cond_buffers.notify_one();

            //Exit the loop if it was the last buffer
            if (currentsize < sizebuffer) {
                break;
            }
        } while (true);

        //Exit
        t.join();
        while (!buffers.empty()) {
            long *b = buffers.back();
            buffers.pop_back();
            delete[] b;
        }

    } else {
        LOG(DEBUG) << "Sequential insert";
        while (!merger.isEmpty()) {
            Triple t = merger.get();
            countInput++;
            if (count % 1000000000 == 0) {
                LOG(DEBUG) << "..." << count << "...";
            }

            if (t.o != po || t.p != pp || t.s != ps) {
                count++;
                ins->insert(permutation, t.s, t.p, t.o, t.count, posWriter,
                        treeWriter,
                        aggregated,
                        canSkipTables);
                ps = t.s;
                pp = t.p;
                po = t.o;

                if (storeRaw) {
                    plainWriter->writeVLong(t.s);
                    plainWriter->writeVLong(t.p);
                    plainWriter->writeVLong(t.o);
                }

                if (sampleWriter != NULL) {
                    if (first || rand() < randThreshold) {
                        sampleWriter->write(t.s, t.p, t.o);
                        first = false;
                    }
                }
            }
        }
    }

    ins->flush(permutation, posWriter, treeWriter, aggregated, canSkipTables);

    if (plainWriter != NULL) {
        delete plainWriter;
    }

    if (printstats) {
        Statistics *stat = ins->getStats(permutation);
        if (stat != NULL) {
            LOG(DEBUG) << "Perm " << permutation << ": RowLayout" << stat->nListStrategies << " ClusterLayout " << stat->nGroupStrategies << " ColumnLayout " << stat->nList2Strategies;
            LOG(DEBUG) << "Perm " << permutation << ": Exact " << stat->exact << " Approx " << stat->approximate;
            LOG(DEBUG) << "Perm " << permutation << ": FirstElemCompr1 " << stat->nFirstCompr1 << " FirstElemCompr2 " << stat->nFirstCompr2;
            LOG(DEBUG) << "Perm " << permutation << ": SecondElemCompr1 " << stat->nSecondCompr1 << " SecondElemCompr2 " << stat->nSecondCompr2;
            LOG(DEBUG) << "Perm " << permutation << ": Diff " << stat->diff << " Nodiff " << stat->nodiff;
            LOG(DEBUG) << "Perm " << permutation << ": Aggregated " << stat->aggregated << " NotAggr " << stat->notAggregated;
            LOG(DEBUG) << "Perm " << permutation << ": NTables " << ins->getNTablesPerPartition(permutation);
            LOG(DEBUG) << "Perm " << permutation << ": NSkippedTables " << ins->getNSkippedTables(permutation);
        }
    }

    if (POSoutputDir != NULL) {
        delete posWriter;
    }

    //Remove the files
    if (removeInput) {
        LOG(DEBUG) << "Removing " << inputDir;
        fs::remove_all(fs::path(inputDir));
    }
    LOG(DEBUG) << "...completed. Added " << count << " triples out of " << countInput;
}

void Loader::insertDictionary(const int part, DictMgmt *dict, string
        dictFileInput, bool insertDictionary, bool insertInverseDictionary,
        bool storeNumbersCoordinates, nTerm *maxValueCounter) {
    LZ4Writer *tmpWriter = NULL;
    if (storeNumbersCoordinates) {
        tmpWriter = new LZ4Writer(dictFileInput + ".tmp");
    }
    std::vector<string> alldictfiles = Compressor::getAllDictFiles(dictFileInput);
    //Insert the common terms at the end...
    //alldictfiles.insert(alldictfiles.begin(), dictFileInput);

    //Read n. popular terms
    //nTerm key = ((long)1 << 40);
    nTerm key = 0;
    if (fs::exists(dictFileInput)) {
        LZ4Reader in(dictFileInput);
        while (!in.isEof()) {
            in.parseLong();
            int size;
            in.parseString(size);
            key++;
        }
    }

    for (auto dictfile = alldictfiles.begin(); dictfile != alldictfiles.end(); ++dictfile) {
        LZ4Reader in(*dictfile);
        LOG(DEBUG) << "Parsing " << *dictfile;
        while (!in.isEof()) {
            //The key in the files is continuosly resetted. I cannot use it.
            //I use key instead.
            //nTerm key = in.parseLong();.
            in.parseLong();
            int size;
            const char *value = in.parseString(size);
            bool resp = true;
            if (insertDictionary && insertInverseDictionary) {
                if (!storeNumbersCoordinates) {
                    dict->appendPair(value, size, key);
                } else {
                    //In this case I only insert the "dict" entries
                    long coordinates;
                    resp = dict->putDict(value, size, key, coordinates);
                    tmpWriter->writeLong(key);
                    tmpWriter->writeLong(coordinates);
                }
            } else if (insertDictionary) {
                resp = dict->putDict(value, size, key);
            } else {
                resp = dict->putInvDict(value, size, key);
            }

            if (!resp) {
                LOG(ERROR) << "This should not happen. Term " <<
                    string(value, size) << "-" << key << " is already inserted.";
            }
            key++;
        }
    }

    *maxValueCounter = key - 1;
    /*** I can now add the common terms. They are not in lex. ordering so I cannot append ***/
    if (fs::exists(dictFileInput)) {
        LZ4Reader in(dictFileInput);
        LOG(DEBUG) << "Parsing " << dictFileInput;
        key = 0;
        //key = ((long)1 << 40);
        while (!in.isEof()) {
            //The key in the files is continuosly resetted. I cannot use it.
            //I use key instead.
            //nTerm key = in.parseLong();.
            in.parseLong();
            int size;
            const char *value = in.parseString(size);

            bool resp = true;
            if (insertDictionary && insertInverseDictionary) {
                if (!storeNumbersCoordinates) {
                    resp = dict->putPair(value + 2, size - 2, key);
                } else {
                    //In this case I only insert the "dict" entries
                    long coordinates;
                    resp = dict->putDict(value + 2, size - 2, key, coordinates);
                    tmpWriter->writeLong(key);
                    tmpWriter->writeLong(coordinates);
                }
            } else if (insertDictionary) {
                resp = dict->putDict(value + 2, size - 2, key);
            } else {
                resp = dict->putInvDict(value + 2, size - 2, key);
            }

            if (!resp) {
                LOG(ERROR) << "This should not happen. Term " <<
                    string(value + 2, size - 2) << "-" << key << " is already inserted.";
            }
            key++;
        }
    }
    *maxValueCounter = max(key - 1, *maxValueCounter);

    LOG(DEBUG) << "Added " << key << " dictionary terms";

    //Sort the pairs <term,coordinates> by term and add them into the invdict
    //dictionaries
    if (storeNumbersCoordinates) {
        delete tmpWriter;

        //Sort the files
        vector<string> inputFiles;
        inputFiles.push_back(dictFileInput + ".tmp");
        vector<string> files = Sorter::sortFiles<PairLong>(inputFiles,
                dictFileInput + ".tmp");

        if (files.size() > 1) {
            FileMerger<PairLong> merger(files);
            while (!merger.isEmpty()) {
                PairLong el = merger.get();
                dict->putInvDict(el.n1, el.n2);
            }
        } else {
            LZ4Reader reader(files[0]);
            while (!reader.isEof()) {
                PairLong p;
                p.readFrom(&reader);
                dict->putInvDict(p.n1, p.n2);
            }
        }

        //Delete the temporary files
        for (vector<string>::iterator itr = files.begin(); itr != files.end();
                ++itr) {
            fs::remove(*itr);
        }

        //Delete the input file
        fs::remove(dictFileInput + ".tmp");
    }

    for (auto f = alldictfiles.begin(); f != alldictfiles.end(); ++f) {
        fs::remove(*f);
    }
    fs::remove(dictFileInput);
}

void Loader::exportFiles(string tripleDir, string* dictFiles,
        const int ndicts, string outputFileTriple, string outputFileDict) {
    //Export the triples
    LOG(DEBUG) << "Start procedure exporting files";

    Kognac::sortCompressedGraph(tripleDir, outputFileTriple, 2);

    std::ofstream fileDict(outputFileDict, ios_base::binary);
    {
        boost::iostreams::filtering_ostream out;
        out.push(boost::iostreams::gzip_compressor());
        out.push(fileDict);

        for (int i = 0; i < ndicts; ++i) {
            string dictFile = dictFiles[i];
            LOG(DEBUG) << "Exporting dict file " << dictFile;
            LZ4Reader reader(dictFile);
            long counter = 0;
            while (!reader.isEof()) {
                long n = reader.parseLong();
                int size;
                const char* s = reader.parseString(size);
                out << to_string(n) << " " << to_string(size - 2) << " ";
                out.write(s + 2, size - 2);
                out << endl;
                counter++;
            }
            //Exporting non-popular dictionary entries
            std::vector<string> addDictionaryFiles = Compressor::getAllDictFiles(dictFile);
            for (auto df = addDictionaryFiles.begin(); df != addDictionaryFiles.end();
                    df++) {
                LZ4Reader reader(*df);
                while (!reader.isEof()) {
                    //I ignore this number since the counter restarts for each file
                    reader.parseLong();
                    int size;
                    const char* s = reader.parseString(size);
                    out << counter << " " << to_string(size) << " ";
                    out.write(s, size);
                    out << endl;
                    counter++;
                }
            }
        }

    }
    fileDict.close();
}

void Loader::addSchemaTerms(const int dictPartitions, nTerm highestNumber, DictMgmt *dict) {
    if (dictPartitions != 1) {
        LOG(ERROR) << "The addition of schema terms is supported only the dictionary is stored on one partition";
        throw 10;
    }
    vector<string> schemaTerms = Schema::getAllSchemaTerms();
    for (vector<string>::iterator itr = schemaTerms.begin(); itr != schemaTerms.end(); itr++) {
        nTerm key;
        if (!dict->getNumber(itr->c_str(), itr->size(), &key)) {
            //Add it in the dictionary
            LOG(DEBUG) << "Add in the dictionary the entry " << *itr << " with number " << (highestNumber + 1);
            dict->putPair(itr->c_str(), itr->size(), ++highestNumber);
        } else {
            LOG(DEBUG) << "The schema entry " << *itr << " was already in the input with the id " << key;
        }
    }
}

void Loader::monitorPerformance(/*SinkPtr logger,*/ int seconds, std::condition_variable *cv, std::mutex *mtx,  bool *isFinished) {
    //Monitor CPU usage, memory usage and disk I/O

    while (true) {
        std::unique_lock<std::mutex> lck(*mtx);
        cv->wait_for(lck,std::chrono::seconds(seconds));
        if (*isFinished)
            break;
        lck.unlock();

        long mem = TridentUtils::getVmRSS();
        double cpu = TridentUtils::getCPUUsage();
        long diskread = TridentUtils::diskread();
        long diskwrite = TridentUtils::diskwrite();
        long phy_diskread = TridentUtils::phy_diskread();
        long phy_diskwrite = TridentUtils::phy_diskwrite();
        LOG(DEBUG) << "STATS:\tvmrss_kb=" << mem << "\tcpu_perc=" << cpu << "\tdiskread_bytes=" << diskread << "\tdiskwrite_bytes=" << diskwrite << "\tphydiskread_bytes=" << phy_diskread << "\tphydiskwrite_bytes=" << phy_diskwrite;
        //logger->flush();
    }
}

void Loader::load(ParamsLoad p) {
    std::chrono::system_clock::time_point start = std::chrono::system_clock::now();
    LOG(DEBUG) << "Start loading ...";

    //Start a monitoring thread ...
    std::thread monitor;
    std::mutex mtx;
    std::condition_variable cv;
    bool isFinished = false;
    if (/*p.logPtr != NULL &&*/ p.timeoutStats != -1) {
        //Activate it only for Linux systems
#if defined(__linux__) || defined(__linux) || defined(linux) || defined(__gnu_linux__)
        monitor = std::thread(std::bind(Loader::monitorPerformance, p.logPtr, p.timeoutStats, &cv, &mtx, &isFinished));
#endif
    }

    //Check if kbDir exists
    if (fs::exists(fs::path(p.kbDir))) {
        fs::remove_all(fs::path(p.kbDir));
    }
    fs::create_directories(fs::path(p.kbDir));

    if (p.tmpDir != p.kbDir) {
        if (fs::exists(fs::path(p.tmpDir))) {
            fs::remove_all(fs::path(p.tmpDir));
        }
        fs::create_directories(fs::path(p.tmpDir));
    }

    //How many to use dictionaries?
    int ncores = Utils::getNumberPhysicalCores();
    if (p.parallelThreads > ncores) {
        LOG(WARN) << "The parallelThreads parameter is above the number pf physical cores. I set it to " << ncores;
        p.parallelThreads = ncores;
    }

    if (p.maxReadingThreads > p.parallelThreads) {
        LOG(WARN) << "I cannot read with more threads than the available ones. I set it to = " << p.parallelThreads;
        p.maxReadingThreads = p.parallelThreads;
    }

    if (p.dictionaries > p.parallelThreads) {
        LOG(WARN) << "The dictionary partitions cannot be higher than the maximum number of threads. I set it to = " << p.parallelThreads;
        p.dictionaries = p.parallelThreads;
    }

    LOG(DEBUG) << "Set number of dictionaries to " << p.dictionaries << " parallel threads=" << p.parallelThreads << " readingThreads=" << p.maxReadingThreads;

    //Create data structures to compress the input
    int nperms = 1;
    int signaturePerm = 0;
    if (p.nindices == 3) {
        if (p.aggrIndices) {
            nperms = 2;
            Compressor::addPermutation(IDX_SPO, signaturePerm);
            Compressor::addPermutation(IDX_OPS, signaturePerm);
        } else {
            nperms = 3;
            Compressor::addPermutation(IDX_SPO, signaturePerm);
            Compressor::addPermutation(IDX_OPS, signaturePerm);
            Compressor::addPermutation(IDX_POS, signaturePerm);
        }
    } else if (p.nindices == 4) {
        if (p.aggrIndices) {
            nperms = 2;
            Compressor::addPermutation(IDX_SPO, signaturePerm);
            Compressor::addPermutation(IDX_OPS, signaturePerm);
        } else {
            nperms = 4;
            Compressor::addPermutation(IDX_SPO, signaturePerm);
            Compressor::addPermutation(IDX_OPS, signaturePerm);
            Compressor::addPermutation(IDX_POS, signaturePerm);
            Compressor::addPermutation(IDX_PSO, signaturePerm);
        }
    } else if (p.nindices == 6) {
        if (p.aggrIndices) {
            nperms = 4;
            Compressor::addPermutation(IDX_SPO, signaturePerm);
            Compressor::addPermutation(IDX_OPS, signaturePerm);
            Compressor::addPermutation(IDX_SOP, signaturePerm);
            Compressor::addPermutation(IDX_OSP, signaturePerm);
        } else {
            nperms = 6;
            Compressor::addPermutation(IDX_SPO, signaturePerm);
            Compressor::addPermutation(IDX_OPS, signaturePerm);
            Compressor::addPermutation(IDX_POS, signaturePerm);
            Compressor::addPermutation(IDX_SOP, signaturePerm);
            Compressor::addPermutation(IDX_OSP, signaturePerm);
            Compressor::addPermutation(IDX_PSO, signaturePerm);
        }
    }

    long totalCount = 0;
    string *permDirs = new string[nperms];
    for (int i = 0; i < nperms; ++i) {
        permDirs[i] = p.tmpDir + string("/permtmp-") + to_string(i);
        fs::create_directories(fs::path(permDirs[i]));
    }
    string *fileNameDictionaries = new string[p.dictionaries];
    for (int i = 0; i < p.dictionaries; ++i) {
        fileNameDictionaries[i] = p.tmpDir + string("/dict-") + to_string(i);
    }

    if (p.inputformat == "snap") { /*** LOAD SNAP FILES ***/
        if (p.graphTransformation == "") {
            p.graphTransformation = "not_multigraph";
        }
        totalCount = parseSnapFile(p.triplesInputDir,
                p.dictDir,
                permDirs + 3, //Store the output in the SOP directory
                1,
                signaturePerm,
                fileNameDictionaries[0],
                p.maxReadingThreads,
                p.parallelThreads);
    } else { /*** LOAD RDF FILES ***/
        if (!p.inputCompressed) {
            if (p.dictMethod != DICT_HEURISTICS) {
                throw 10;
            }
            Compressor comp(p.triplesInputDir, p.tmpDir);
            //Parse the input
            comp.parse(p.dictionaries, p.sampleMethod, p.sampleArg, (int)(p.sampleRate * 100),
                    p.parallelThreads, p.maxReadingThreads, false, NULL, false,
                    p.graphTransformation != "");
            //Compress it
            LOG(DEBUG) << "For now I create only one permutation";
            int tmpsig = 0;
            Compressor::addPermutation(IDX_SPO, tmpsig);
            comp.compress(p.graphTransformation != "" ? permDirs + 3 : permDirs,
                    1, tmpsig, fileNameDictionaries,
                    p.dictionaries, p.parallelThreads, p.maxReadingThreads,
                    p.graphTransformation != "");
            totalCount = comp.getTotalCount();
            LOG(INFO) << "Compression is finished. Starting the loading ...";
            if (p.onlyCompress) {
                //Convert the triple files and the dictionary files in gzipped files
                string outputTriples = p.kbDir + string("/triples.gz");
                string outputDict = p.kbDir + string("/dict.gz");
                exportFiles(permDirs[0], fileNameDictionaries,
                        p.dictionaries, outputTriples, outputDict);
                for (int i = 0; i < nperms; ++i)
                    fs::remove_all(permDirs[i]);
                for (int i = 0; i < p.dictionaries; ++i) {
                    fs::remove(fileNameDictionaries[i]);
                    std::vector<string> moreDictFiles = Compressor::getAllDictFiles(fileNameDictionaries[i]);
                    for (int j = 0; j < moreDictFiles.size(); ++j) {
                        fs::remove(moreDictFiles[j]);
                    }
                }
                if (p.tmpDir != p.kbDir) {
                    fs::remove_all(fs::path(p.tmpDir));
                }
                return;
            }
        } else {
            totalCount = createPermsAndDictsFromFiles(p.triplesInputDir,
                    p.dictDir,
                    p.graphTransformation != "" ? permDirs + 3 : permDirs, //Use only the first dir
                    1,
                    signaturePerm, //Ignored
                    fileNameDictionaries[0],
                    p.maxReadingThreads,
                    p.parallelThreads);
        }
    }

    KBConfig config;
    config.setParamInt(DICTPARTITIONS, p.dictionaries);
    config.setParamInt(NINDICES, p.nindices);
    config.setParamBool(AGGRINDICES, p.aggrIndices);
    config.setParamBool(USEFIXEDSTRAT, p.enableFixedStrat);
    config.setParamInt(FIXEDSTRAT, p.fixedStrat);
    config.setParamInt(THRESHOLD_SKIP_TABLE, p.thresholdSkipTable);
    LOG(DEBUG) << "Optimizing memory management for " << totalCount << " triples";
    MemoryOptimizer::optimizeForWriting(totalCount, config);
    if (p.dictMethod == DICT_HASH) {
        config.setParamBool(DICTHASH, true);
    }
    std::unique_ptr<KB> kb =
        std::unique_ptr<KB>(new KB(p.kbDir.c_str(), false, false, true, config));
    loadKB(*kb.get(), p,
            totalCount,
            permDirs,
            nperms,
            signaturePerm,
            fileNameDictionaries,
            p.storeDicts,
            p.relsOwnIDs);

    /*** CLEANUP ***/
    delete[] permDirs;
    delete[] fileNameDictionaries;
    if (p.tmpDir != p.kbDir) {
        fs::remove_all(fs::path(p.tmpDir));
    }
    std::unique_lock<std::mutex> lck(mtx);
    isFinished = true;
    cv.notify_all();
    lck.unlock();
    monitor.join();
    std::chrono::duration<double> sec = std::chrono::system_clock::now() - start;
    LOG(INFO) << "Loading is finished: Time (sec) " << sec.count();
}

void Loader::rewriteKG(string inputdir, std::unordered_map<long,long> &map) {
    //For each file in the directory, replace the predicate IDs with new ones
    long counter = 0;
    auto files = Utils::getFiles(inputdir);
    for (auto pathfile : files) {
        {
            LZ4Reader reader(pathfile);
            LZ4Writer writer(pathfile + "-new");
            LOG(DEBUG) << "Converting " << pathfile;
            while (!reader.isEof()) {
                long s = reader.parseLong();
                long p = reader.parseLong();
                long o = reader.parseLong();
                if (map.count(p)) {
                    p = map[p];
                } else {
                    map.insert(std::make_pair(p, counter));
                    p = counter;
                    counter += 1;
                }
                writer.writeLong(s);
                writer.writeLong(p);
                writer.writeLong(o);
            }
        }
        fs::remove(pathfile);
        fs::rename(pathfile + "-new", pathfile);
    }
}

void Loader::loadKB(KB &kb,
        ParamsLoad &p,
        long totalCount,
        string *permDirs,
        int nperms,
        int signaturePerms,
        string *fileNameDictionaries,
        bool storeDicts,
        bool relsOwnIDs) {

    //Init params
    string tmpDir = p.tmpDir;
    const string kbDir = p.kbDir;
    int dictionaries = p.dictionaries;
    string dictMethod = p.dictMethod;
    int nindices = p.nindices;
    bool createIndicesInBlocks = p.createIndicesInBlocks;
    bool aggrIndices = p.aggrIndices;
    bool canSkipTables = p.canSkipTables;
    bool sample = p.sample;
    double sampleRate = p.sampleRate;
    bool storePlainList = p.storePlainList;
    //SinkPtr logPtr = p.logPtr;
    string remoteLocation = p.remoteLocation;
    long limitSpace = p.limitSpace;
    int parallelProcesses = p.parallelThreads;
    int maxReadingThreads = p.maxReadingThreads;
    string graphTransformation = p.graphTransformation;
    //this->logPtr = logPtr;
    //End init params

    std::thread *threads;
    if (storeDicts) {
        LOG(DEBUG) << "Insert the dictionary in the trees";
        threads = new std::thread[dictionaries - 1];
        nTerm *maxValues = new nTerm[dictionaries];
        if (dictMethod != DICT_SMART) {
            for (int i = 1; i < dictionaries; ++i) {
                threads[i - 1] = std::thread(
                        std::bind(&Loader::insertDictionary, i,
                            kb.getDictMgmt(),
                            fileNameDictionaries[i],
                            dictMethod != DICT_HASH,
                            true, false, maxValues + i));
            }
            insertDictionary(0, kb.getDictMgmt(), fileNameDictionaries[0],
                    dictMethod != DICT_HASH, true, false, maxValues);
            for (int i = 1; i < dictionaries; ++i) {
                threads[i - 1].join();
            }
        } else {
            insertDictionary(0, kb.getDictMgmt(), fileNameDictionaries[0], true,
                    true, true, maxValues);
        }
#ifdef REASONING
        addSchemaTerms(dictionaries, maxValues[0], kb.getDictMgmt());
#endif
        delete[] maxValues;
        delete[] threads;
        /*** Close the dictionaries ***/
        LOG(DEBUG) << "Closing dict...";
        kb.closeMainDict();
    }

    LOG(DEBUG) << "Insert the triples in the indices...";
    string *sTreeWriters = new string[nindices];
    TreeWriter **treeWriters = new TreeWriter*[nindices];
    for (int i = 0; i < nindices; ++i) {
        sTreeWriters[i] = tmpDir + string("/tmpTree" ) + to_string(i);
        treeWriters[i] = new TreeWriter(sTreeWriters[i]);
    }

    //Use aggregated indices
    string aggr1Dir = tmpDir + string("/aggr1");
    string aggr2Dir = tmpDir + string("/aggr2");
    if (aggrIndices && nindices > 1) {
        fs::create_directories(fs::path(aggr1Dir));
        if (nindices > 3)
            fs::create_directories(fs::path(aggr2Dir));
    }

    //if sample is requested, create a subdir
    string sampleDir = tmpDir + string("/sampledir");
    SimpleTripleWriter *sampleWriter = NULL;
    if (sample) {
        fs::create_directories(fs::path(sampleDir));
        sampleWriter = new SimpleTripleWriter(sampleDir, "input", false);
    }

    //Create n threads where the triples are sorted and inserted in the knowledge base
    Inserter *ins = kb.insert();
    LOG(DEBUG) << "Start sortAndInsert";

    if (nindices != 6) {
        LOG(ERROR) << "Support only 6 indices (for now)";
        throw 1;
    }

    string outputDirs[6];
    for (int i = 0; i < 6; ++i) {
        outputDirs[i] = kbDir + "/p" + to_string(i);
    }

    if (graphTransformation == "unlabeled") {
        kb.setGraphType(GraphType::DIRECTED);
    }
    if (graphTransformation == "undirected") {
        LOG(DEBUG) << "Transforming the graph in 'undirected'";
        string input = permDirs[3];
        string output = input + "_tmp";
        fs::create_directories(output);
        std::vector<string> files = Utils::getFiles(input);
        for(auto file : files) {
            LOG(DEBUG) << "Transforming file " << file;
            fs::path pfile = fs::path(file);
            LZ4Reader reader(file);
            string fileout = output + string("/") + pfile.filename().string();
            LZ4Writer writer(fileout);
            while (!reader.isEof()) {
                L_Triple t;
                t.readFrom(&reader);
                //Write both versions.
                t.writeTo(&writer);
                if (t.first > t.third) {
                    //swap them
                    long box = t.first;
                    t.first = t.third;
                    t.third = box;
                }
                t.writeTo(&writer);
            }
        }
        fs::remove_all(input);
        fs::rename(fs::path(output), input);
        LOG(DEBUG) << "End transformation";
        kb.setGraphType(GraphType::UNDIRECTED);
    }

    if (graphTransformation != "") {
        //I don't need POS,PSO,SPO,OPS
        if (graphTransformation == "undirected") {
            nindices = 1;
        } else {
            nindices = 2;
        }
        ins->disableColumnStorage();
        ins->setUsageRowForLargeTables();
    } else {
        //If the relations should have their own IDs, I rewrite the compressed
        //graph storing an additional map with the IDs of the relations
        if (relsOwnIDs) {
            if (!storeDicts) {
                LOG(ERROR) << "RelOwnIDs is set to true. Also storeDicts must be set to true";
                throw 10;
            }
            std::unordered_map<long,long> ent2rel;
            //The input is on the form SPO
            rewriteKG(permDirs[0], ent2rel);
            //Store the map in a file
            LZ4Writer writer(kbDir + "/e2r");
            for (auto pair : ent2rel) {
                writer.writeLong(pair.first);
                writer.writeLong(pair.second);
            }
        }
    }

    createIndices(parallelProcesses, maxReadingThreads,
            ins, createIndicesInBlocks,
            aggrIndices,canSkipTables, storePlainList,
            permDirs, outputDirs, aggr1Dir, aggr2Dir, treeWriters, sampleWriter,
            sampleRate,
            remoteLocation,
            limitSpace,
            totalCount,
            nindices);

    if (nindices != 6)
        nindices = 6; //restore

    for (int i = 0; i < nindices; ++i) {
        treeWriters[i]->finish();
    }

    threads = new std::thread[dictionaries + 1];
    LOG(DEBUG) << "Compress the dictionary nodes...";
    for (int i = 0; i < dictionaries && storeDicts; ++i) {
        threads[i + 1] = std::thread(
                std::bind(&NodeManager::compressSpace,
                    kb.getDictPath(i)));
    }

    LOG(DEBUG) << "Start creating the tree...";
    SharedStructs structs;
    structs.bufferToFill = structs.bufferToReturn = &structs.buffer1;
    structs.isFinished = false;
    structs.buffersReady = 0;

    ParamsMergeCoordinates params;
    params.coordinates = sTreeWriters;
    params.ncoordinates = graphTransformation != "" ? 2 : 6;

    params.bufferToFill = structs.bufferToFill;
    params.isFinished = &structs.isFinished;
    params.buffersReady = &structs.buffersReady;
    params.buffer1 = &structs.buffer1;
    params.buffer2 = &structs.buffer2;
    params.cond = &structs.cond;
    params.mut = &structs.mut;
    threads[0] = std::thread(
            std::bind(&Loader::mergeTermCoordinates, params));
    processTermCoordinates(ins, &structs);
    for (int i = 0; i < (storeDicts ? dictionaries : 0) + 1; ++i) {
        threads[i].join();
    }
    for (int i = 0; i < nindices; ++i) {
        fs::remove(fs::path(sTreeWriters[i]));
        delete treeWriters[i];
    }
    delete[] sTreeWriters;
    delete[] treeWriters;

    delete ins;
    delete[] threads;

    if (graphTransformation != "") {
        LOG(DEBUG) << "Load flat representation ...";
        kb.close();
        string flatfile = kbDir + "/tree/flat";
        //Create a tree itr to go through the tree
        std::unique_ptr<Root> root(kb.getRootTree());
        loadFlatTree(kbDir + "/p" + to_string(IDX_SOP),
                kbDir + "/p" +  to_string(IDX_OSP),
                flatfile, root.get(), graphTransformation == "undirected");
    }

    if (sample) {
        LOG(DEBUG) << "Creating a sample dataset";
        delete sampleWriter;
        string sampleKB = kbDir + string("/_sample");

        string *samplePermDirs = new string[nperms];
        for (int i = 0; i < nperms; ++i) {
            samplePermDirs[i] = sampleKB + string("/permtmp-") + to_string(i);
            fs::create_directories(fs::path(samplePermDirs[i]));
        }

        //Create the permutations
        createPermutations(sampleDir, 1, 1, samplePermDirs,
                parallelProcesses, maxReadingThreads);

        //Load a smaller KB
        KBConfig config;
        config.setParamInt(DICTPARTITIONS, dictionaries);
        config.setParamInt(NINDICES, nindices);
        config.setParamBool(AGGRINDICES, aggrIndices);
        config.setParamBool(USEFIXEDSTRAT, false);
        printStats = false;
        MemoryOptimizer::optimizeForWriting((long)(totalCount * sampleRate), config);
        KB kb(sampleKB.c_str(), false, false, false, config);

        ParamsLoad samplep = p;
        samplep.kbDir = sampleKB;
        samplep.tmpDir = sampleKB;
        //samplep.logPtr = NULL;
        samplep.limitSpace = 0;
        samplep.remoteLocation = "";
        samplep.sample = false;
        loadKB(kb,
                samplep,
                totalCount * p.sampleRate,
                samplePermDirs,
                nperms,
                signaturePerms,
                fileNameDictionaries,
                false,
                false);

        fs::remove_all(fs::path(sampleDir));
        delete[] samplePermDirs;
    }

    LOG(DEBUG) << "...completed.";
}

void Loader::generateNewPermutation_seq(MultiDiskLZ4Reader *reader,
        MultiDiskLZ4Writer *writer,
        int idx,
        int pos0,
        int pos1,
        int pos2) {
    long triple[3];
    while (!reader->isEOF(idx)) {
        Triple t;
        t.readFrom(idx, reader);
        triple[0] = t.s;
        triple[1] = t.p;
        triple[2] = t.o;
        writer->writeLong(idx, triple[pos0]);
        writer->writeLong(idx, triple[pos1]);
        writer->writeLong(idx, triple[pos2]);
    }
    writer->setTerminated(idx);
}

void Loader::generateNewPermutation(string outputdir,
        string inputdir,
        char pos0,
        char pos1,
        char pos2,
        int parallelProcesses,
        int maxReadingThreads) {
    //Read all files in the the directory.
    LOG(DEBUG) << "Start process generating new permutation";
    fs::path pInput(inputdir);
    if (fs::is_directory(pInput)) {
        auto files = Utils::getFiles(pInput.string());
        std::vector<std::vector<string>> chunks(parallelProcesses);
        int idx = 0;
        for (auto f : files) {
            chunks[idx].push_back(f);
            idx = (idx + 1) % parallelProcesses;
        }

        //Setup readers
        MultiDiskLZ4Reader **readers = new MultiDiskLZ4Reader*[maxReadingThreads];
        int partsPerFiles = parallelProcesses / maxReadingThreads;
        idx = 0;
        for(int i = 0; i < maxReadingThreads; ++i) {
            readers[i] = new MultiDiskLZ4Reader(partsPerFiles, 3, 4);
            readers[i]->start();
            for(int j = 0; j < partsPerFiles; ++j)
                readers[i]->addInput(j, chunks[idx++]);
        }

        //Setup writers
        MultiDiskLZ4Writer **writers = new MultiDiskLZ4Writer*[maxReadingThreads];
        idx = 0;
        for (int i = 0; i < maxReadingThreads; ++i) {
            std::vector<string> outputchunk;
            for(int j = 0; j < partsPerFiles; ++j) {
                outputchunk.push_back(outputdir + "/input-" + to_string(idx++));
            }
            writers[i] = new MultiDiskLZ4Writer(outputchunk, 3, 4);
        }
        //Start threads
        std::thread *threads = new std::thread[parallelProcesses];
        for(int i = 0; i < parallelProcesses; ++i) {
            int idx1 = i % maxReadingThreads;
            int idx2 = i / maxReadingThreads;
            MultiDiskLZ4Reader *reader = readers[idx1];
            MultiDiskLZ4Writer *writer = writers[idx1];
            threads[i] = std::thread(generateNewPermutation_seq,
                    reader,
                    writer,
                    idx2,
                    pos0,
                    pos1,
                    pos2);
        }
        for(int i = 0; i < parallelProcesses; ++i) {
            threads[i].join();
        }

        for(int i = 0; i < maxReadingThreads; ++i) {
            delete readers[i];
            delete writers[i];
        }
        delete[] readers;
        delete[] writers;
        delete[] threads;
    }
    LOG(DEBUG) << "Finished";
}

void Loader::moveData(string remoteLocation, string inputDir, long limitSpace) {
    //If the space of the device where the inputDir is located is less than limitSpace,
    //then I will move it to remoteLocation
    LOG(DEBUG) << "Check whether I should move " << inputDir << " to " << remoteLocation << " limit " << limitSpace;
    if (remoteLocation != "") {
        LOG(DEBUG) << "Check if the space is less than " << limitSpace;
        fs::space_info info = fs::space(inputDir);
        LOG(DEBUG) << "Space is " << info.available << " bytes";
        if (info.available < limitSpace) {
            //Copy the input dir in a certain location with scp
            string cmd = string("scp -r ") + inputDir + string(" ") + remoteLocation;
            LOG(DEBUG) << "Executing the command " << cmd;
            auto exitcode = system(cmd.c_str());
            if (exitcode == 0) {
                LOG(DEBUG) << "Program exited";
                fs::remove_all(fs::path(inputDir));
            } else {
                LOG(DEBUG) << "Something went wrong in the execution of the program. Do nothing.";
            }
        }
    }
}

/*void Loader::sortChunks(string inputdir,
  int maxReadingThreads,
  int parallelProcesses,
  long estimatedSize,
  std::vector<std::pair<string, char>> &additionalPermutations) {

//calculate the number of elements
long mem = Utils::getSystemMemory() * 0.7;
long nelements = mem / sizeof(L_Triple);
long elementsMainMem = max((long)parallelProcesses,
min(nelements, (long)(estimatedSize * 1.2)));

Loader::sortPermutation<L_Triple>(inputdir,
maxReadingThreads,
parallelProcesses,
true,
estimatedSize,
elementsMainMem,
16,
false,
additionalPermutations);
}*/

void Loader::parallel_createIndices(
        int parallelProcesses,
        int maxReadingThreads,
        Inserter *ins,
        const bool createIndicesInBlocks,
        const bool aggrIndices,
        const bool canSkipTables,
        const bool storePlainList,
        string *permDirs,
        string *outputDirs,
        string aggr1Dir,
        string aggr2Dir,
        TreeWriter **treeWriters,
        SimpleTripleWriter *sampleWriter,
        double sampleRate,
        string remotePath,
        long limitSpace,
        long estimatedSize,
        int nindices) {

    if (aggrIndices && nindices != 6) {
        LOG(ERROR) << "Inconsistency on the input parameters. AggrIndices=true but set less than 6 permutations...";
        throw 10;
    }

    //Sort chunks of the triple in main memory
    std::vector<std::pair<string, char>> outputdirs;
    if (nindices == 1) {
        PermSorter::sortChunks(permDirs[3],
                maxReadingThreads,
                parallelProcesses,
                estimatedSize,
                false,
                outputdirs);
    } else if (nindices == 2) {
        outputdirs.push_back(make_pair(permDirs[4], IDX_OSP));
        PermSorter::sortChunks(permDirs[3],
                maxReadingThreads,
                parallelProcesses,
                estimatedSize,
                false,
                outputdirs);
    } else {
        if (aggrIndices) {
            outputdirs.push_back(make_pair(permDirs[2], IDX_SOP));
            outputdirs.push_back(make_pair(permDirs[1], IDX_OPS));
            outputdirs.push_back(make_pair(permDirs[3], IDX_OSP));
        } else {
            outputdirs.push_back(make_pair(permDirs[3], IDX_SOP));
            outputdirs.push_back(make_pair(permDirs[1], IDX_OPS));
            outputdirs.push_back(make_pair(permDirs[4], IDX_OSP));
            outputdirs.push_back(make_pair(permDirs[2], IDX_POS));
            outputdirs.push_back(make_pair(permDirs[5], IDX_PSO));
        }
        PermSorter::sortChunks(permDirs[0],
                maxReadingThreads,
                parallelProcesses,
                estimatedSize,
                true,
                outputdirs);
    }

    int nperms = aggrIndices ? 4 : 6;
    std::thread ts[3];
    std::thread at1, at2;
    if (!aggrIndices) {
        ParamSortAndInsert params;
        params.nindices = nperms;
        params.parallelProcesses = parallelProcesses;
        params.maxReadingThreads = maxReadingThreads;
        params.ins = ins;
        params.inputSorted = true;
        params.storeRaw = false;
        params.sampleWriter = NULL;
        params.sampleRate = 0.0;
        params.aggregated = false;
        //params.logPtr = NULL;
        params.removeInput = true;
        params.printstats = printStats;
        params.POSoutputDir = NULL;
        params.estimatedSize = estimatedSize;
        params.deletePreviousExt = true;

        params.permutation = 1;
        params.inputDir = permDirs[1];
        params.treeWriter = treeWriters[1];
        params.canSkipTables = false;

        ts[0] = std::thread(
                std::bind(&Loader::sortAndInsert, params));

        params.permutation = 3;
        params.inputDir = permDirs[3];
        params.treeWriter = treeWriters[3];
        params.canSkipTables = canSkipTables;

        ts[1] = std::thread(
                std::bind(&Loader::sortAndInsert, params));

        params.permutation = 4;
        params.inputDir = permDirs[4];
        params.treeWriter = treeWriters[4];
        params.canSkipTables = canSkipTables;

        ts[2] = std::thread(
                std::bind(&Loader::sortAndInsert, params));

        //Start two more threads
        params.permutation = 2;
        params.inputDir = permDirs[2];
        params.treeWriter = treeWriters[2];
        params.canSkipTables = false;

        at1 = std::thread(std::bind(&Loader::sortAndInsert, params));

        params.permutation = 5;
        params.inputDir = permDirs[5];
        params.treeWriter = treeWriters[5];
        params.canSkipTables = canSkipTables;

        at2 = std::thread(std::bind(&Loader::sortAndInsert, params));

    } else {
        ParamSortAndInsert params;
        params.nindices = nperms;
        params.parallelProcesses = parallelProcesses;
        params.maxReadingThreads = maxReadingThreads;
        params.ins = ins;
        params.inputSorted = true;
        params.storeRaw = false;
        params.sampleWriter = NULL;
        params.sampleRate = 0.0;
        params.aggregated = false;
        //params.logPtr = NULL;
        params.removeInput = true;
        params.printstats = printStats;
        params.estimatedSize = estimatedSize;
        params.deletePreviousExt = true;

        params.permutation = 1;
        params.inputDir = permDirs[1];
        params.treeWriter = treeWriters[1];
        params.POSoutputDir = &aggr1Dir;
        params.canSkipTables = false;

        ts[0] = std::thread(
                std::bind(&Loader::sortAndInsert, params));

        params.permutation = 3;
        params.inputDir = permDirs[2];
        params.treeWriter = treeWriters[3];
        params.POSoutputDir = NULL;
        params.canSkipTables = canSkipTables;

        ts[1] = std::thread(
                std::bind(&Loader::sortAndInsert, params));

        params.permutation = 4;
        params.inputDir = permDirs[3];
        params.treeWriter = treeWriters[4];
        params.POSoutputDir = NULL;
        params.canSkipTables = canSkipTables;

        ts[2] = std::thread(
                std::bind(&Loader::sortAndInsert, params));
    }

    ParamSortAndInsert params;
    params.parallelProcesses = parallelProcesses;
    params.maxReadingThreads = maxReadingThreads;
    params.permutation = 0;
    params.nindices = nperms;
    params.inputSorted = true;
    params.inputDir = permDirs[0];
    params.POSoutputDir = aggrIndices ? &aggr2Dir : NULL;
    params.treeWriter = treeWriters[0];
    params.ins = ins;
    params.aggregated = false;
    params.canSkipTables = false;
    params.storeRaw = storePlainList;
    params.sampleWriter = sampleWriter;
    params.sampleRate = sampleRate;
    params.printstats = printStats;
    //params.logPtr = logPtr;
    params.removeInput = true;
    params.estimatedSize = estimatedSize;
    params.deletePreviousExt = true;

    sortAndInsert(params);
    for (int i = 0; i < 3; ++i) {
        ts[i].join();
    }
    at1.join();
    at2.join();

    //Aggregated
    if (aggrIndices) {
        ParamSortAndInsert params;
        params.nindices = 2;
        params.parallelProcesses = parallelProcesses;
        params.maxReadingThreads = maxReadingThreads;
        params.ins = ins;
        params.inputSorted = false;
        params.storeRaw = false;
        params.sampleWriter = NULL;
        params.sampleRate = 0.0;
        //params.logPtr = NULL;
        params.removeInput = true;
        params.aggregated = true;
        params.printstats = printStats;
        params.estimatedSize = estimatedSize;
        params.deletePreviousExt = true;

        params.permutation = 2;
        params.inputDir = aggr1Dir;
        params.treeWriter = treeWriters[2];
        params.POSoutputDir = NULL;
        params.canSkipTables = false;

        std::thread t[2];
        t[0] = std::thread(std::bind(&Loader::sortAndInsert, params));

        params.permutation = 5;
        params.inputDir = aggr2Dir;
        params.treeWriter = treeWriters[5];
        params.POSoutputDir = NULL;
        params.canSkipTables = canSkipTables;

        t[1] = std::thread(std::bind(&Loader::sortAndInsert, params));
        t[0].join();
        t[1].join();
    }
}

void Loader::seq_createIndices(
        int parallelProcesses,
        int maxReadingThreads,
        Inserter *ins,
        const bool createIndicesInBlocks,
        const bool aggrIndices,
        const bool canSkipTables,
        const bool storePlainList,
        string *permDirs,
        string *outputDirs,
        string aggr1Dir,
        string aggr2Dir,
        TreeWriter **treeWriters,
        SimpleTripleWriter *sampleWriter,
        double sampleRate,
        string remotePath,
        long limitSpace,
        long estimatedSize) {

    LOG(DEBUG) << "SortAndIndex one-by-one";

    ParamSortAndInsert params;
    params.parallelProcesses = parallelProcesses;
    params.maxReadingThreads = maxReadingThreads;
    params.permutation = 0;
    params.nindices = 1;
    params.inputSorted = false;
    params.inputDir = permDirs[0];
    params.POSoutputDir = aggrIndices ? &aggr2Dir : NULL;
    params.treeWriter = treeWriters[0];
    params.ins = ins;
    params.aggregated = false;
    params.canSkipTables = false;
    params.storeRaw = storePlainList;
    params.sampleWriter = sampleWriter;
    params.sampleRate = sampleRate;
    params.printstats = printStats;
    //params.logPtr = logPtr;
    params.removeInput = false;
    params.estimatedSize = estimatedSize;
    params.deletePreviousExt = false;

    sortAndInsert(params);

    string lastInput = permDirs[0];
    generateNewPermutation(permDirs[1], lastInput, 2, 1, 0, parallelProcesses,
            maxReadingThreads);
    fs::remove_all(fs::path(lastInput));
    ins->stopInserts(0);
    moveData(remotePath, outputDirs[0], limitSpace);
    LOG(DEBUG) << "Memory used so far: " << Utils::getUsedMemory();

    params.permutation = 1;
    params.nindices = 1;
    params.inputSorted = false;
    params.inputDir = permDirs[1];
    params.POSoutputDir = aggrIndices ? &aggr1Dir : NULL;
    params.treeWriter = treeWriters[1];
    params.ins = ins;
    params.aggregated = false;
    params.canSkipTables = false;
    params.storeRaw = false;
    params.sampleWriter = NULL;
    params.sampleRate = 0.0;
    params.printstats = printStats;
    //params.logPtr = logPtr;
    params.removeInput = false;

    sortAndInsert(params);

    lastInput = permDirs[1];
    generateNewPermutation(aggrIndices ? permDirs[2] : permDirs[3],
            lastInput, 2, 0, 1, parallelProcesses,
            maxReadingThreads);
    fs::remove_all(fs::path(lastInput));
    ins->stopInserts(1);
    moveData(remotePath, outputDirs[1], limitSpace);
    LOG(DEBUG) << "Memory used so far: " << Utils::getUsedMemory();

    params.permutation = 3;
    params.nindices = 1;
    params.inputSorted = false;
    params.inputDir = aggrIndices ? permDirs[2] : permDirs[3];
    params.POSoutputDir = (string*) NULL;
    params.treeWriter = treeWriters[3];
    params.ins = ins;
    params.aggregated = false;
    params.canSkipTables = canSkipTables;
    params.storeRaw = false;
    params.sampleWriter = NULL;
    params.sampleRate = 0.0;
    params.printstats = printStats;
    //params.logPtr = logPtr;
    params.removeInput = false;

    sortAndInsert(params);

    lastInput = aggrIndices ? permDirs[2] : permDirs[3];
    generateNewPermutation(aggrIndices ? permDirs[3] : permDirs[4],
            lastInput, 1, 0, 2, parallelProcesses,
            maxReadingThreads);
    fs::remove_all(fs::path(lastInput));
    ins->stopInserts(3);
    moveData(remotePath, outputDirs[3], limitSpace);
    LOG(DEBUG) << "Memory used so far: " << Utils::getUsedMemory();

    params.permutation = 4;
    params.nindices = 1;
    params.inputSorted = false;
    params.inputDir = aggrIndices ? permDirs[3] : permDirs[4];
    params.POSoutputDir = (string*) NULL;
    params.treeWriter = treeWriters[4];
    params.ins = ins;
    params.aggregated = false;
    params.canSkipTables = canSkipTables;
    params.storeRaw = false;
    params.sampleWriter = NULL;
    params.sampleRate = 0.0;
    params.printstats = printStats;
    //params.logPtr = logPtr;
    params.removeInput = false;

    sortAndInsert(params);

    ins->stopInserts(4);
    moveData(remotePath, outputDirs[4], limitSpace);
    LOG(DEBUG) << "Memory used so far: " << Utils::getUsedMemory();

    lastInput = aggrIndices ? permDirs[3] : permDirs[4];

    if (!aggrIndices) {
        generateNewPermutation(permDirs[2],
                lastInput, 2, 0, 1, parallelProcesses,
                maxReadingThreads);

        ParamSortAndInsert params;
        params.parallelProcesses = parallelProcesses;
        params.maxReadingThreads = maxReadingThreads;
        params.permutation = 2;
        params.nindices = 1;
        params.inputSorted = false;
        params.inputDir = permDirs[2];
        params.POSoutputDir = (string*) NULL;
        params.treeWriter = treeWriters[2];
        params.ins = ins;
        params.aggregated = false;
        params.canSkipTables = false;
        params.storeRaw = false;
        params.sampleWriter = NULL;
        params.sampleRate = 0.0;
        params.printstats = printStats;
        //params.logPtr = logPtr;
        params.removeInput = false;
        params.estimatedSize = estimatedSize;
        params.deletePreviousExt = false;

        sortAndInsert(params);
        LOG(DEBUG) << "Memory used so far: " << Utils::getUsedMemory();
    } else {

        ParamSortAndInsert params;
        params.parallelProcesses = parallelProcesses;
        params.maxReadingThreads = maxReadingThreads;
        params.permutation = 2;
        params.nindices = 1;
        params.inputSorted = false;
        params.inputDir = aggr1Dir;
        params.POSoutputDir = (string*) NULL;
        params.treeWriter = treeWriters[2];
        params.ins = ins;
        params.aggregated = true;
        params.canSkipTables = false;
        params.storeRaw = false;
        params.sampleWriter = NULL;
        params.sampleRate = 0.0;
        params.printstats = printStats;
        //params.logPtr = logPtr;
        params.removeInput = true;
        params.estimatedSize = estimatedSize;
        params.deletePreviousExt = false;

        sortAndInsert(params);
        LOG(DEBUG) << "Memory used so far: " << Utils::getUsedMemory();
    }
    fs::remove_all(fs::path(lastInput));

    if (!aggrIndices) {
        lastInput = permDirs[2];
        generateNewPermutation(permDirs[5],
                lastInput, 0, 2, 1, parallelProcesses,
                maxReadingThreads);
        fs::remove_all(fs::path(lastInput));

        ParamSortAndInsert params;
        params.parallelProcesses = parallelProcesses;
        params.maxReadingThreads = maxReadingThreads;
        params.permutation = 5;
        params.nindices = 1;
        params.inputSorted = false;
        params.inputDir = permDirs[5];
        params.POSoutputDir = (string*) NULL;
        params.treeWriter = treeWriters[5];
        params.ins = ins;
        params.aggregated = false;
        params.canSkipTables = canSkipTables;
        params.storeRaw = false;
        params.sampleWriter = NULL;
        params.sampleRate = 0.0;
        params.printstats = printStats;
        //params.logPtr = logPtr;
        params.removeInput = false;
        params.estimatedSize = estimatedSize;
        params.deletePreviousExt = false;

        sortAndInsert(params);
        LOG(DEBUG) << "Memory used so far: " << Utils::getUsedMemory();

        lastInput = permDirs[5];
        fs::remove_all(fs::path(lastInput));
    } else {
        ParamSortAndInsert params;
        params.parallelProcesses = parallelProcesses;
        params.maxReadingThreads = maxReadingThreads;
        params.permutation = 5;
        params.nindices = 1;
        params.inputSorted = false;
        params.inputDir = aggr2Dir;
        params.POSoutputDir = (string*) NULL;
        params.treeWriter = treeWriters[5];
        params.ins = ins;
        params.aggregated = true;
        params.canSkipTables = canSkipTables;
        params.storeRaw = false;
        params.sampleWriter = NULL;
        params.sampleRate = 0.0;
        params.printstats = printStats;
        //params.logPtr = logPtr;
        params.removeInput = true;
        params.estimatedSize = estimatedSize;
        params.deletePreviousExt = false;

        sortAndInsert(params);
        LOG(DEBUG) << "Memory used so far: " << Utils::getUsedMemory();
    }
}

void Loader::createIndices(
        int parallelProcesses,
        int maxReadingThreads,
        Inserter *ins,
        const bool createIndicesInBlocks,
        const bool aggrIndices,
        const bool canSkipTables,
        const bool storePlainList,
        string *permDirs,
        string *outputDirs,
        string aggr1Dir,
        string aggr2Dir,
        TreeWriter **treeWriters,
        SimpleTripleWriter *sampleWriter,
        double sampleRate,
        string remotePath,
        long limitSpace,
        long estimatedSize,
        int nindices) {
    if (createIndicesInBlocks) {
        seq_createIndices(parallelProcesses, maxReadingThreads,
                ins, createIndicesInBlocks, aggrIndices, canSkipTables,
                storePlainList, permDirs, outputDirs, aggr1Dir, aggr2Dir,
                treeWriters, sampleWriter, sampleRate, remotePath,
                limitSpace, estimatedSize);
    } else {
        parallel_createIndices(parallelProcesses, maxReadingThreads,
                ins, createIndicesInBlocks, aggrIndices, canSkipTables,
                storePlainList, permDirs, outputDirs, aggr1Dir, aggr2Dir,
                treeWriters, sampleWriter, sampleRate, remotePath,
                limitSpace, estimatedSize, nindices);
    }
}

void Loader::createPermutations(string inputDir, int nperms, int signaturePerms,
        string *outputPermFiles, int parallelProcesses, int maxReadingThreads) {
    MultiDiskLZ4Writer ***permWriters = new MultiDiskLZ4Writer**[nperms];
    int partsPerFile = parallelProcesses / maxReadingThreads;
    for (int i = 0; i < nperms; ++i) {
        permWriters[i] = new MultiDiskLZ4Writer*[maxReadingThreads];
        int idx = 0;
        for (int j = 0; j < maxReadingThreads; ++j) {
            std::vector<string> inputfiles;
            for(int m = 0; m < partsPerFile; ++m) {
                string outputfile = outputPermFiles[i] + "/input" + to_string(idx++);
                inputfiles.push_back(outputfile);
            }
            permWriters[i][j] = new MultiDiskLZ4Writer(inputfiles, 3, 4);
        }
    }
    int detailPerms[6];
    Compressor::parsePermutationSignature(signaturePerms, detailPerms);

    //Read all triples
    vector<string> files = Utils::getFiles(inputDir);
    long count = 0;
    int currentPart = 0;
    for (std::vector<string>::iterator itr = files.begin(); itr != files.end();
            ++itr) {
        LZ4Reader reader(*itr);
#if DEBUG
        char quad = reader.parseByte();
        assert(quad == 0);
#else
        reader.parseByte();
#endif
        while (!reader.isEof()) {
            long triple[3];
            triple[0] = reader.parseLong();
            triple[1] = reader.parseLong();
            triple[2] = reader.parseLong();

            const int idxwriter = currentPart / partsPerFile;
            const int offwriter = currentPart % partsPerFile;
            count++;
            for (int i = 0; i < nperms; ++i) {
                switch (detailPerms[i]) {
                    case IDX_SPO:
                        permWriters[i][idxwriter]->writeLong(offwriter, triple[0]);
                        permWriters[i][idxwriter]->writeLong(offwriter, triple[1]);
                        permWriters[i][idxwriter]->writeLong(offwriter, triple[2]);
                        break;
                    case IDX_OPS:
                        permWriters[i][idxwriter]->writeLong(offwriter, triple[2]);
                        permWriters[i][idxwriter]->writeLong(offwriter, triple[1]);
                        permWriters[i][idxwriter]->writeLong(offwriter, triple[0]);
                        break;
                    case IDX_SOP:
                        permWriters[i][idxwriter]->writeLong(offwriter, triple[0]);
                        permWriters[i][idxwriter]->writeLong(offwriter, triple[2]);
                        permWriters[i][idxwriter]->writeLong(offwriter, triple[1]);
                        break;
                    case IDX_OSP:
                        permWriters[i][idxwriter]->writeLong(offwriter, triple[2]);
                        permWriters[i][idxwriter]->writeLong(offwriter, triple[0]);
                        permWriters[i][idxwriter]->writeLong(offwriter, triple[1]);
                        break;
                    case IDX_PSO:
                        permWriters[i][idxwriter]->writeLong(offwriter, triple[1]);
                        permWriters[i][idxwriter]->writeLong(offwriter, triple[0]);
                        permWriters[i][idxwriter]->writeLong(offwriter, triple[2]);
                        break;
                    case IDX_POS:
                        permWriters[i][idxwriter]->writeLong(offwriter, triple[1]);
                        permWriters[i][idxwriter]->writeLong(offwriter, triple[2]);
                        permWriters[i][idxwriter]->writeLong(offwriter, triple[0]);
                        break;
                }
            }
            currentPart = (currentPart + 1) % parallelProcesses;
        }
    }
    for(int p = 0; p < nperms; ++p) {
        for(int i = 0; i < maxReadingThreads; ++i) {
            for(int j = 0; j < partsPerFile; ++j) {
                permWriters[p][i]->setTerminated(j);
            }
        }
    }

    LOG(DEBUG) << "Created permutation for " << count;
    for (int i = 0; i < nperms; ++i) {
        for(int j = 0; j < maxReadingThreads; ++j) {
            delete permWriters[i][j];
        }
        delete[] permWriters[i];
    }
    delete[] permWriters;
}

void Loader::processTermCoordinates(Inserter *ins,
        SharedStructs *structs) {
    BufferCoordinates *buffer;
    while ((buffer = getBunchTermCoordinates(structs)) != NULL) {
        nTerm key;
        while (!buffer->isEmpty()) {
            TermCoordinates *value = buffer->getNext(key);
            ins->insert(key, value);
        }
        releaseBunchTermCoordinates(buffer, structs);
    }
}

void Loader::loadFlatTree(string sop,
        string osp,
        string flatfile,
        Root *root,
        bool undirected) {
    //Write SOP
    long keyToAdd = -1;
    std::vector<string> files = Utils::getFiles(sop);
    if (files.size() == 0) {
        //Nothing to do, exit
        return;
    }
    const int maxPossibleIdx = files.size();
    std::unique_ptr<FlatTreeWriter> ftw =
        std::unique_ptr<FlatTreeWriter>(new FlatTreeWriter(flatfile, undirected));
    TreeItr *itr = root->itr();
    TermCoordinates coord;

    for (int i = 0; i < maxPossibleIdx; ++i) {
        if (i > std::numeric_limits<short>::max()) {
            LOG(DEBUG) << "Too many idx files in sop. Cannot create a flat tree";
            throw 10;
        }
        string fidx = sop + "/" + to_string(i) + ".idx";
        if (fs::exists(fidx)) {
            //Open it, and read the coordinates
            char tmpbuffer[16];
            ifstream ifs;
            ifs.open(fidx);
            ifs.read(tmpbuffer, 8);
            const long nentries = Utils::decode_long(tmpbuffer);

            ifstream rawfile;
            bool rawfileOpened = false;

            for(long entry = 0; entry < nentries; ++entry) {
                ifs.read(tmpbuffer, 11);
                const long key = Utils::decode_longFixedBytes(tmpbuffer + 5, 5);
                const long pos_sop = Utils::decode_longFixedBytes(tmpbuffer, 5);
                char strat_sop = tmpbuffer[10];
                if (StorageStrat::getStorageType(strat_sop) == NEWCOLUMN_ITR) {
                    //Open the file
                    if (!rawfileOpened) {
                        rawfile.open(sop + "/" + to_string(i));
                        rawfileOpened = true;
                    }
                    rawfile.seekg(pos_sop);
                    char tmpbuffer[2];
                    rawfile.read(tmpbuffer, 2);
                    strat_sop = Loader::rewriteNewColumnStrategy(tmpbuffer);
                }
                const short file_sop = i;
                long ntree_sop = -1;
                long ntree_osp = -1;
                while (true) {
                    if(!itr->hasNext()) {
                        LOG(DEBUG) << "Cannot happen. (loadFlatTree)";
                        throw 10;
                    }
                    const long treeKey = itr->next(&coord);
                    if (treeKey > key) {
                        LOG(DEBUG) << "Cannot happen (2). (loadFlatTree)";
                        throw 10;
                    }
                    if (coord.exists(IDX_SOP)) {
                        ntree_sop = coord.getNElements(IDX_SOP);
                    } else {
                        ntree_sop = 0;
                    }
                    if (coord.exists(IDX_OSP)) {
                        ntree_osp = coord.getNElements(IDX_OSP);
                    } else {
                        ntree_osp = 0;
                    }
                    if (ntree_osp == 0 && ntree_sop == 0) {
                        LOG(DEBUG) << "Cannot happen (3). (loadFlatTree)";
                        throw 10;
                    }

                    //First make sure we have a contiguous array (graph can be disconnected)
                    while (++keyToAdd < treeKey) {
                        ftw->write(keyToAdd, 0, 0, 0, 0, 0, 0, 0, 0);
                    }
                    if (keyToAdd != treeKey) {
                        LOG(DEBUG) << "Cannot happen (10). (loadFlatTree)";
                        throw 10;
                    }

                    if (treeKey < key) {
                        //Write the current treekey
                        ftw->write(treeKey, ntree_sop, 0, 0, 0, ntree_osp, 0, 0, 0);
                    } else {
                        break;
                    }
                }
                ftw->write(key, ntree_sop, strat_sop, file_sop, pos_sop, ntree_osp, 0, 0, 0);
            }
            ifs.close();
            if (rawfileOpened) {
                rawfile.close();
            }
        }
    }
    while (itr->hasNext()) {
        const long treeKey = itr->next(&coord);
        if (coord.exists(IDX_SOP)) {
            LOG(DEBUG) << "Cannot happen (4). (loadFlatTree)";
            throw 10;
        }
        long ntree_osp = 0;
        if (coord.exists(IDX_OSP)) {
            ntree_osp = coord.getNElements(IDX_OSP);
        } else {
            LOG(DEBUG) << "Cannot happen (5). (loadFlatTree)";
            throw 10;
        }
        //First make sure we have a contiguous array (graph can be disconnected)
        while (++keyToAdd < treeKey) {
            ftw->write(keyToAdd, 0, 0, 0, 0, 0, 0, 0, 0);
        }
        ftw->write(treeKey, 0, 0, 0, 0, ntree_osp, 0, 0, 0);
    }

    //WRITE OSP
    ftw = std::unique_ptr<FlatTreeWriter>();
    bip::file_mapping mapping(flatfile.c_str(), bip::read_write);
    bip::mapped_region mapped_rgn(mapping, bip::read_write);
    char *rawbuffer = static_cast<char*>(mapped_rgn.get_address());

    for (int i = 0; i < maxPossibleIdx; ++i) {
        if (i > std::numeric_limits<short>::max()) {
            LOG(DEBUG) << "Too many idx files in sop. Cannot create a flat tree";
            throw 10;
        }
        string fidx = osp + "/" + to_string(i) + ".idx";
        if (fs::exists(fidx)) {
            //Open it, and read the coordinates
            char tmpbuffer[16];
            ifstream ifs;
            ifs.open(fidx);
            ifs.read(tmpbuffer, 8);
            const long nentries = Utils::decode_long(tmpbuffer);

            ifstream rawfile;
            bool rawfileOpened = false;

            for(long entry = 0; entry < nentries; ++entry) {
                ifs.read(tmpbuffer, 11);
                const long key = Utils::decode_longFixedBytes(tmpbuffer + 5, 5);
                const long pos_osp = Utils::decode_longFixedBytes(tmpbuffer, 5);
                char strat_osp = tmpbuffer[10];
                const short file_osp = i;

                //overwrite strat and pos
                char *baseblock = rawbuffer + key * 31;
                if (StorageStrat::getStorageType(strat_osp) == NEWCOLUMN_ITR) {
                    //Open the file
                    if (!rawfileOpened) {
                        rawfile.open(osp + "/" + to_string(i));
                        rawfileOpened = true;
                    }
                    rawfile.seekg(pos_osp);
                    char tmpbuffer[2];
                    rawfile.read(tmpbuffer, 2);
                    strat_osp = Loader::rewriteNewColumnStrategy(tmpbuffer);
                }
                baseblock[23] = strat_osp;

                char *cfile_osp = (char *)&file_osp;
                baseblock[24] = cfile_osp[0];
                baseblock[25] = cfile_osp[1];
                //Utils::encode_short(baseblock + 24, file_osp);
                char *cpos_osp = (char *)&pos_osp;
                baseblock[26] = cpos_osp[0];
                baseblock[27] = cpos_osp[1];
                baseblock[28] = cpos_osp[2];
                baseblock[29] = cpos_osp[3];
                baseblock[30] = cpos_osp[4];
                //Utils::encode_longNBytes(baseblock + 26, 5, pos_osp);
            }
            if (rawfileOpened) {
                rawfile.close();
            }
            ifs.close();
        }
    }
    mapped_rgn.flush();
    delete itr;
}

char Loader::rewriteNewColumnStrategy(const char *table) {
    const uint8_t header1 = (uint8_t) table[0];
    const uint8_t bytesPerFirstEntry = (header1 >> 3) & 7;
    const uint8_t header2 = (uint8_t) table[1];
    const uint8_t bytesPerStartingPoint =  header2 & 7;
    const uint8_t bytesPerCount = (header2 >> 3) & 7;
    const uint8_t remBytes = bytesPerCount + bytesPerStartingPoint;
    if (bytesPerFirstEntry < 1 || bytesPerFirstEntry > 5) {
        LOG(ERROR) << "I calculated a maximum of 5 bytes per first entry" << (int) bytesPerFirstEntry;
        throw 10;
    }
    if (remBytes > 16 || remBytes < 2) {
        LOG(ERROR) << "remBytes range wrong " << (int) remBytes;
        throw 10;
    }
    char out = 0;
    if (bytesPerFirstEntry < 3) {
        out = StorageStrat::setStorageType(out, 1);
        if (bytesPerFirstEntry == 2) {
            out |= 1 << 4;
        }
        out |= remBytes - 2;
    } else if (bytesPerFirstEntry < 5) {
        out = StorageStrat::setStorageType(out, 2);
        if (bytesPerFirstEntry == 4) {
            out |= 1 << 4;
        }
        out |= remBytes - 2;
    } else {
        out = StorageStrat::setStorageType(out, NEWCOLUMN_ITR);
        if (bytesPerFirstEntry == 6) {
            out |= 1 << 4;
        }
        out |= remBytes - 2;
    }
    return out;
}

void Loader::mergeTermCoordinates(ParamsMergeCoordinates params) {
    auto coordinates = params.coordinates;
    auto ncoordinates = params.ncoordinates;
    auto bufferToFill = params.bufferToFill;
    auto isFinished = params.isFinished;
    auto buffersReady = params.buffersReady;
    auto buffer1 = params.buffer1;
    auto buffer2 = params.buffer2;
    auto cond = params.cond;
    auto mut = params.mut;

    CoordinatesMerger merger(coordinates, ncoordinates);
    nTerm key;
    TermCoordinates *value = NULL;
    while ((value = merger.get(key)) != NULL) {
        if (bufferToFill->isFull()) {
            std::unique_lock<std::mutex> lock(*mut);
            (*buffersReady)++;
            if (*buffersReady > 0) {
                cond->notify_one();
            }

            while (*buffersReady == 2) {
                cond->wait(lock);
            }

            //Take next buffer
            bufferToFill = (bufferToFill == buffer1) ? buffer2 : buffer1;
        }
        bufferToFill->add(key, value);
    }
    std::unique_lock<std::mutex> lock(*mut);
    if (!bufferToFill->isEmpty()) {
        (*buffersReady)++;
    }
    *isFinished = true;
    cond->notify_one();
    lock.unlock();
}

BufferCoordinates *Loader::getBunchTermCoordinates(SharedStructs *structs) {
    std::unique_lock<std::mutex> lock(structs->mut);
    while (structs->buffersReady == 0) {
        if (structs->isFinished) {
            lock.unlock();
            return NULL;
        }
        structs->cond.wait(lock);
    }
    lock.unlock();

    BufferCoordinates *tmpBuffer = structs->bufferToReturn;
    structs->bufferToReturn = (structs->bufferToReturn == &structs->buffer1) ?
        &structs->buffer2 : &structs->buffer1;
    return tmpBuffer;

}

void Loader::releaseBunchTermCoordinates(BufferCoordinates *cord,
        SharedStructs *structs) {
    cord->clear();
    std::unique_lock<std::mutex> lock(structs->mut);
    if (structs->buffersReady == 2) {
        structs->cond.notify_one();
    }
    structs->buffersReady--;
    lock.unlock();
}

void Loader::testLoadingTree(string tmpDir, Inserter *ins, int nindices) {
    /*string *sTreeWriters = new string[nindices];
      std::thread *threads = new std::thread[1];
      for (int i = 0; i < nindices; ++i) {
      sTreeWriters[i] = tmpDir + string("/tmpTree" ) + to_string(i);
      }


      bufferToFill = bufferToReturn = &buffer1;
      isFinished = false;
      buffersReady = 0;
      threads[0] = std::thread(
      std::bind(&Loader::mergeTermCoordinates, this,
      sTreeWriters, nindices));
      processTermCoordinates(ins);
      for (int i = 0; i < 1; ++i) {
      threads[i].join();
      }

      delete[] sTreeWriters;
      delete[] threads;*/
}

TermCoordinates *CoordinatesMerger::get(nTerm &key) {
    value.clear();

    if (ncoordinates == 1) {
        if (!spoFinished) {
            key = elspo.key;
            value.set(0, elspo.file, elspo.pos, elspo.nElements, elspo.strat);
            spoFinished = !getFirst(&elspo, &spo);
            return &value;
        } else {
            return NULL;
        }
    } else if (ncoordinates == 2) {
        if (!sopFinished && (ospFinished || elsop.key <= elosp.key)) {
            key = elsop.key;
            value.set(3, elsop.file, elsop.pos, elsop.nElements, elsop.strat);
            if (!ospFinished && elosp.key == elsop.key) {
                value.set(4, elosp.file, elosp.pos, elosp.nElements, elosp.strat);
                ospFinished = !getFirst(&elosp, &osp);
            }
            sopFinished = !getFirst(&elsop, &sop);
            return &value;
        } else if (!ospFinished) {
            key = elosp.key;
            value.set(4, elosp.file, elosp.pos, elosp.nElements, elosp.strat);
            ospFinished = !getFirst(&elosp, &osp);
            return &value;
        } else {
            return NULL;
        }
    }

    //n==3 or n==6
    if (!spoFinished && (opsFinished || elspo.key <= elops.key)
            && (posFinished || elspo.key <= elpos.key)) {
        key = elspo.key;
        value.set(0, elspo.file, elspo.pos, elspo.nElements, elspo.strat);
        if (ncoordinates == 6 && !sopFinished && elsop.key == elspo.key) {
            value.set(3, elsop.file, elsop.pos, elsop.nElements, elsop.strat);
            sopFinished = !getFirst(&elsop, &sop);
        }
        if (!opsFinished && elops.key == elspo.key) {
            value.set(1, elops.file, elops.pos, elops.nElements, elops.strat);
            if (ncoordinates == 6 && !ospFinished && elosp.key == elspo.key) {
                value.set(4, elosp.file, elosp.pos, elosp.nElements, elosp.strat);
                ospFinished = !getFirst(&elosp, &osp);
            }
            opsFinished = !getFirst(&elops, &ops);
        }
        if (!posFinished && elpos.key == elspo.key) {
            value.set(2, elpos.file, elpos.pos, elpos.nElements, elpos.strat);
            if ((ncoordinates == 6 || ncoordinates == 4) && !psoFinished
                    && elpso.key == elspo.key) {
                assert(elpos.nElements == elpso.nElements);
                value.set(5, elpso.file, elpso.pos, elpso.nElements, elpso.strat);
                psoFinished = !getFirst(&elpso, &pso);
            }
            posFinished = !getFirst(&elpos, &pos);
        }
        spoFinished = !getFirst(&elspo, &spo);
        return &value;
    } else if (!opsFinished && (posFinished || elops.key <= elpos.key)) {
        key = elops.key;
        value.set(1, elops.file, elops.pos, elops.nElements, elops.strat);
        if (ncoordinates == 6 && !ospFinished && elosp.key == elops.key) {
            value.set(4, elosp.file, elosp.pos, elosp.nElements, elosp.strat);
            ospFinished = !getFirst(&elosp, &osp);
        }
        if (!posFinished && elpos.key == elops.key) {
            value.set(2, elpos.file, elpos.pos, elpos.nElements, elpos.strat);
            if ((ncoordinates == 6 || ncoordinates == 4) && !psoFinished
                    && elpso.key == elops.key) {
                assert(elpos.nElements == elpso.nElements);
                value.set(5, elpso.file, elpso.pos, elpso.nElements, elpso.strat);
                psoFinished = !getFirst(&elpso, &pso);
            }
            posFinished = !getFirst(&elpos, &pos);
        }
        opsFinished = !getFirst(&elops, &ops);
        return &value;
    } else if (!posFinished) {
        key = elpos.key;
        value.set(2, elpos.file, elpos.pos, elpos.nElements, elpos.strat);
        if ((ncoordinates == 6 || ncoordinates == 4) && !psoFinished
                && elpso.key == elpos.key) {
            assert(elpos.nElements == elpso.nElements);
            value.set(5, elpso.file, elpso.pos, elpso.nElements, elpso.strat);
            psoFinished = !getFirst(&elpso, &pso);
        }
        posFinished = !getFirst(&elpos, &pos);
        return &value;
    } else {
        return NULL;
    }
}

bool CoordinatesMerger::getFirst(TreeEl *el, ifstream *buffer) {
    if (buffer->read(supportBuffer, 23)) {
        el->file = Utils::decode_short((const char*) supportBuffer, 16);
        el->key = Utils::decode_long(supportBuffer, 0);
        el->nElements = Utils::decode_long(supportBuffer, 8);
        el->pos = Utils::decode_int(supportBuffer, 18);
        el->strat = supportBuffer[22];
        return true;
    }
    return false;
}

bool L_Triple::sLess_sop(const L_Triple &t1, const L_Triple &t2) {
    if (t1.first < t2.first) {
        return true;
    } else if (t1.first == t2.first) {
        if (t1.third < t2.third) {
            return true;
        } else if (t1.third == t2.third) {
            return t1.second < t2.second;
        }
    }
    return false;
}

bool L_Triple::sLess_ops(const L_Triple &t1, const L_Triple &t2) {
    if (t1.third < t2.third) {
        return true;
    } else if (t1.third == t2.third) {
        if (t1.second < t2.second) {
            return true;
        } else if (t1.second == t2.second) {
            return t1.first < t2.first;
        }
    }
    return false;
}

bool L_Triple::sLess_osp(const L_Triple &t1, const L_Triple &t2) {
    if (t1.third < t2.third) {
        return true;
    } else if (t1.third == t2.third) {
        if (t1.first < t2.first) {
            return true;
        } else if (t1.first == t2.first) {
            return t1.second < t2.second;
        }
    }
    return false;
}

bool L_Triple::sLess_pos(const L_Triple &t1, const L_Triple &t2) {
    if (t1.second < t2.second) {
        return true;
    } else if (t1.second == t2.second) {
        if (t1.third < t2.third) {
            return true;
        } else if (t1.third == t2.third) {
            return t1.first < t2.first;
        }
    }
    return false;
}

bool L_Triple::sLess_pso(const L_Triple &t1, const L_Triple &t2) {
    if (t1.second < t2.second) {
        return true;
    } else if (t1.second == t2.second) {
        if (t1.first < t2.first) {
            return true;
        } else if (t1.first == t2.first) {
            return t1.third < t2.third;
        }
    }
    return false;
}
