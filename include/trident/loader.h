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


#ifndef LOADER_H_
#define LOADER_H_

#include <trident/kb/kb.h>
#include <trident/tree/coordinates.h>
#include <trident/kb/inserter.h>

#include <kognac/filemerger.h>
#include <kognac/sorter.h>
#include <kognac/lz4io.h>
#include <kognac/triple.h>
#include <kognac/multidisklz4reader.h>
#include <kognac/multidisklz4writer.h>
#include <kognac/diskreader.h>

#include <mutex>
#include <condition_variable>
#include <string>
#include <vector>
#include <unordered_map>

using namespace std;

typedef enum _InputFormat { RDF, SNAP } InputFormat;

typedef class PairLong {
    public:
        long n1;
        long n2;

        void readFrom(LZ4Reader *reader) {
            n1 = reader->parseLong();
            n2 = reader->parseLong();
        }

        void writeTo(LZ4Writer *writer) {
            writer->writeLong(n1);
            writer->writeLong(n2);
        }

        static bool less(const PairLong &p1, const PairLong &p2) {
            return p1.n1 < p2.n1;
        }

        bool greater(const PairLong &p) const {
            return !less(*this, p);
        }
} PairLong;

typedef struct TreeEl {
    nTerm key;
    long nElements;
    int pos;
    short file;
    char strat;
} TreeEl;

class TreeWriter: public TreeInserter {
    private:
        ofstream fos;
        char supportBuffer[23];
    public:
        TreeWriter(string path) {
            fos.open(path);
        }

        void addEntry(nTerm key, long nElements, short file, int pos,
                char strategy) {
            Utils::encode_long(supportBuffer, 0, key);
            Utils::encode_long(supportBuffer, 8, nElements);
            Utils::encode_short(supportBuffer, 16, file);
            Utils::encode_int(supportBuffer, 18, pos);
            supportBuffer[22] = strategy;
            fos.write(supportBuffer, 23);
        }

        void finish() {
            fos.close();
        }

        ~TreeWriter() {
        }
        ;
};

class CoordinatesMerger {
    private:
        ifstream spo, ops, pos;
        ifstream sop, osp, pso;
        char supportBuffer[23];

        const int ncoordinates;

        TreeEl elspo, elops, elpos, elsop, elosp, elpso;
        bool spoFinished, opsFinished, posFinished,
             sopFinished, ospFinished, psoFinished;
        TermCoordinates value;

        bool getFirst(TreeEl *el, ifstream *buffer);

    public:
        CoordinatesMerger(string *coordinates, int ncoordinates) :
            ncoordinates(ncoordinates) {

                //Open the three files
                spo.open(coordinates[0]);
                spoFinished = !getFirst(&elspo, &spo);

                if (ncoordinates > 1) {
                    ops.open(coordinates[1]);
                    opsFinished = !getFirst(&elops, &ops);
                    pos.open(coordinates[2]);
                    posFinished = !getFirst(&elpos, &pos);
                }

                if (ncoordinates == 4) {
                    pso.open(coordinates[3]);
                    getFirst(&elpso, &pso);
                } else if (ncoordinates == 6 || ncoordinates == 2) {
                    sop.open(coordinates[3]);
                    osp.open(coordinates[4]);
                    pso.open(coordinates[5]);
                    sopFinished = !getFirst(&elsop, &sop);
                    ospFinished = !getFirst(&elosp, &osp);
                    psoFinished = !getFirst(&elpso, &pso);
                }
            }

        TermCoordinates *get(nTerm &key);

        ~CoordinatesMerger() {
            spo.close();

            if (ncoordinates > 1) {
                ops.close();
                pos.close();
            }

            if (ncoordinates == 4) {
                pso.close();
            } else if (ncoordinates == 6) {
                sop.close();
                osp.close();
                pso.close();
            }
        }
};

class BufferCoordinates {
    nTerm keys[10000];
    TermCoordinates values[10000];
    int size;
    int pos;
    public:
    BufferCoordinates() {
        size = pos = 0;
    }

    void add(nTerm key, TermCoordinates *value) {
        keys[size] = key;
        values[size++].copyFrom(value);
    }

    TermCoordinates *getNext(nTerm &key) {
        key = keys[pos];
        return values + pos++;
    }

    bool isFull() {
        return size == 10000;
    }

    bool isEmpty() {
        return pos == size;
    }

    void clear() {
        pos = size = 0;
    }
};

class SimpleTripleWriter;
struct ParamSortAndInsert {
    ParamSortAndInsert() {}
    int permutation;
    int nindices;
    int parallelProcesses;
    int maxReadingThreads;
    bool inputSorted;
    string inputDir;
    string *POSoutputDir;
    TreeWriter *treeWriter;
    Inserter *ins;
    bool aggregated;
    bool canSkipTables;
    bool storeRaw;
    SimpleTripleWriter *sampleWriter;
    double sampleRate;
    bool printstats;
    //SinkPtr logPtr;
    bool removeInput;
    long estimatedSize;
    bool deletePreviousExt;
};

class L_Triple {
    public:
        uint64_t first, second, third;

        static bool sLess(const L_Triple &t1, const L_Triple &t2) {
            if (t1.first < t2.first) {
                return true;
            } else if (t1.first == t2.first) {
                if (t1.second < t2.second) {
                    return true;
                } else if (t1.second == t2.second) {
                    return t1.third < t2.third;
                }
            }
            return false;
        }

        static bool sLess_sop(const L_Triple &t1, const L_Triple &t2);
        static bool sLess_osp(const L_Triple &t1, const L_Triple &t2);
        static bool sLess_ops(const L_Triple &t1, const L_Triple &t2);
        static bool sLess_pos(const L_Triple &t1, const L_Triple &t2);
        static bool sLess_pso(const L_Triple &t1, const L_Triple &t2);

        virtual void readFrom(int part, MultiDiskLZ4Reader *reader) {
            first = reader->readLong(part);
            second = reader->readLong(part);
            third = reader->readLong(part);
        }

        virtual void readFrom(LZ4Reader *reader) {
            first = reader->parseLong();
            second = reader->parseLong();
            third = reader->parseLong();
        }

        virtual void writeTo(LZ4Writer *writer) {
            writer->writeLong(first);
            writer->writeLong(second);
            writer->writeLong(third);
        }

        virtual void toTriple(Triple &t) {
            t.s = first;
            t.p = second;
            t.o = third;
            t.count = 0;
        }

        static L_Triple max() {
            L_Triple t;
            t.first = ~0lu;
            t.second = ~0lu;
            t.third = ~0lu;
            return t;
        }

        static bool ismax(const L_Triple &t) {
            return t.first == ~0lu;
        }

        virtual ~L_Triple() {}
};

class L_TripleCount : public L_Triple {
    uint64_t count;

    public:

    static bool sLess(const L_TripleCount &t1, const L_TripleCount &t2) {
        return L_Triple::sLess(t1, t2);
    }

    static bool sLess_sop(const L_Triple &t1, const L_Triple &t2) {
        return L_Triple::sLess_sop(t1, t2);
    }

    static bool sLess_osp(const L_Triple &t1, const L_Triple &t2) {
        return L_Triple::sLess_osp(t1, t2);
    }

    static bool sLess_ops(const L_Triple &t1, const L_Triple &t2) {
        return L_Triple::sLess_ops(t1, t2);
    }

    static bool sLess_pos(const L_Triple &t1, const L_Triple &t2) {
        return L_Triple::sLess_pos(t1, t2);
    }

    static bool sLess_pso(const L_Triple &t1, const L_Triple &t2) {
        return L_Triple::sLess_pso(t1, t2);
    }

    void readFrom(int part, MultiDiskLZ4Reader *reader) {
        L_Triple::readFrom(part, reader);
        count = reader->readLong(part);
    }

    void readFrom(LZ4Reader *reader) {
        L_Triple::readFrom(reader);
        count = reader->parseLong();
    }

    void writeTo(LZ4Writer *writer) {
        L_Triple::writeTo(writer);
        writer->writeLong(count);
    }

    void toTriple(Triple &t) {
        t.s = first;
        t.p = second;
        t.o = third;
        t.count = count;
    }

    static L_TripleCount max() {
        L_TripleCount t;
        t.first = ~0lu;
        t.second = ~0lu;
        t.third = ~0lu;
        t.count = 0;
        return t;
    }

    ~L_TripleCount() {}
};

struct ParamsMergeCoordinates {
    string *coordinates;
    int ncoordinates;
    BufferCoordinates *bufferToFill;
    bool *isFinished;
    int *buffersReady;
    BufferCoordinates *buffer1;
    BufferCoordinates *buffer2;
    std::condition_variable *cond;
    std::mutex *mut;
};

struct SharedStructs {
    std::mutex mut;
    int buffersReady;
    bool isFinished;
    std::condition_variable cond;
    BufferCoordinates buffer1;
    BufferCoordinates buffer2;
    BufferCoordinates *bufferToFill;
    BufferCoordinates *bufferToReturn;
};

struct ParamsLoad {
    string inputformat;
    bool onlyCompress;
    bool inputCompressed;
    string triplesInputDir;
    string dictDir;
    string dictDir_rel;
    string tmpDir;
    string kbDir;
    string dictMethod;
    int sampleMethod;
    int sampleArg;
    int parallelThreads;
    int maxReadingThreads;
    int dictionaries;
    int nindices;
    bool createIndicesInBlocks;
    bool aggrIndices;
    bool canSkipTables;
    bool enableFixedStrat;
    int fixedStrat;
    bool storePlainList;
    bool sample;
    double sampleRate;
    int thresholdSkipTable;
    //SinkPtr logPtr;
    string remoteLocation;
    long limitSpace;
    string graphTransformation;
    int timeoutStats;
    bool storeDicts;
    bool relsOwnIDs;
    bool flatTree;
};

class Loader {
    private:
        bool printStats;
        //SinkPtr logPtr;

    public:
        static void generateNewPermutation(string outputdir,
                string inputdir,
                char pos0,
                char pos1,
                char pos2,
                int parallelProcesses,
                int maxReadingThreads);

        template<class K>
            static void sortPermutation(string inputDir,
                    int maxReadingThreads,
                    int parallelProcesses,
                    bool initialSort,
                    long estimatedSize,
                    long elementsMainMem,
                    int filesToMerge,
                    bool readFirstByte,
                    std::vector<std::pair<string, char>> &additionalPermutations);

        template<class K>
            static void sortPermutation_seq(const int idReader,
                    MultiDiskLZ4Reader *reader,
                    long start,
                    K *output,
                    long maxInserts,
                    long *count);

        template<class K>
            static void dumpPermutation(std::vector<K> &input,
                    long parallelProcesses,
                    long maxReadingThreads,
                    long maxValue,
                    string out,
                    char sorter);

        static void sortAndInsert(ParamSortAndInsert params);

        static void insertDictionary(const int part, DictMgmt *dict,
                string dictFileInput,
                bool insertDictionary, bool insertInverseDictionary,
                bool sortNumberCoordinates, nTerm *maxValueCounter);

        static void parallelmerge(FileMerger<Triple> *merger,
                int buffersize,
                std::vector<long*> *buffers,
                std::mutex *m_buffers,
                std::condition_variable *cond_buffers,
                std::list <
                std::pair<long*, int >> *exchangeBuffers,
                std::mutex *m_exchange,
                std::condition_variable *cond_exchange);

        static void processTermCoordinates(Inserter *ins,
                SharedStructs *structs);

        static void mergeTermCoordinates(ParamsMergeCoordinates params);
    private:
        template<class K>
            static void dumpPermutation_seq(K *start, K *end,
                    MultiDiskLZ4Writer *writer,
                    int currentPart,
                    char sorter);

        static BufferCoordinates *getBunchTermCoordinates(SharedStructs *structs);

        static void releaseBunchTermCoordinates(BufferCoordinates *cord,
                SharedStructs *structs);

        void exportFiles(string tripleDir, string* dictFiles, const
                int ndicts, string outputFileTriple, string outputFileDict);

        void addSchemaTerms(const int dictPartitions,
                nTerm highestNumber,
                DictMgmt *dict);

        void seq_createIndices(
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
                long estimatedSize);

        void parallel_createIndices(
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
                int nindices);

        void createIndices(
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
                string remoteLocation,
                long limitSpace,
                long estimatedSize,
                int nindices);

        void loadKB(KB &kb,
                ParamsLoad &p,
                long totalCount,
                string *permDirs,
                int nperms,
                int signaturePerms,
                string *fileNameDictionaries,
                bool storeDicts,
                bool relsOwnIDs);

        void createPermutations(string inputDir,
                int nperms,
                int signaturePerms,
                string *outputPermFiles,
                int parallelProcesses,
                int maxReadingThreads);

        static void moveData(string remoteLocation,
                string inputDir,
                long limitSpace);

        static void createPermsAndDictsFromFiles_seq(DiskReader *reader,
                DiskLZ4Writer *writer, int id, long *output);

        static long createPermsAndDictsFromFiles(
                string inputtriples,
                bool separateDictEntRels,
                string inputdict,
                string inputdictr,
                string *permDirs,
                int nperms,
                int signaturePerm,
                string fileNameDictionaries,
                int maxReadingThreads,
                int parallelProcesses);

        static long parseSnapFile(
                string inputtriples,
                string inputdict,
                string *permDirs,
                int nperms,
                int signaturePerm,
                string fileNameDictionaries,
                int maxReadingThreads,
                int parallelProcesses);

        static void generateNewPermutation_seq(MultiDiskLZ4Reader *reader,
                MultiDiskLZ4Writer *writer,
                int idx,
                int pos0,
                int pos1,
                int pos2);

        static void monitorPerformance(/*SinkPtr logger, */int seconds,
                std::condition_variable *cv, std::mutex *mtx, bool *isFinished);

        static void rewriteKG(string inputdir, std::unordered_map<long,long> &map);

    public:

        Loader() {
            printStats = true;
        }

        void load(ParamsLoad p);

        void testLoadingTree(string inputdir, Inserter *ins, int nindices);
};

#endif /* LOADER_H_ */
