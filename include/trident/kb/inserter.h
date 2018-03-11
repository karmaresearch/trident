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


#ifndef INSERTER_H_
#define INSERTER_H_

#include <trident/kb/dictmgmt.h>

#include <trident/tree/root.h>
#include <trident/kb/consts.h>
#include <trident/binarytables/storagestrat.h>
#include <trident/binarytables/binarytableinserter.h>

#include <kognac/factory.h>
#include <kognac/triple.h>

#include <vector>
#include <iostream>

#define POSAGGRBYTE INT64_C(0x100000000000)

class TreeInserter {

    public:
        virtual void addEntry(nTerm key, int64_t nElements, short file, int pos,
                char stategy) = 0;

        virtual ~TreeInserter() {}
};

class Inserter {
    private:
        static char STRATEGY_FOR_POS;

        Root* tree;
        TableStorage **files;
        const int64_t nTerms;

        const bool useFixedStrategy;
        const char fixedStrategy;

        bool useRowForLargeTables;
        size_t thresholdForColumnStorage;
        const size_t thresholdSkipTable;

        int64_t currentT1[N_PARTITIONS];
        int64_t currentT2[N_PARTITIONS];
        int64_t nElements[N_PARTITIONS];

        int64_t *values1[N_PARTITIONS];
        int64_t *values2[N_PARTITIONS];

        short fileIdx[N_PARTITIONS];
        int startPositions[N_PARTITIONS];
        char strategies[N_PARTITIONS];
        bool onlyReferences[N_PARTITIONS];
        bool skipTable[N_PARTITIONS];

        //Used for the aggregated indices
        int64_t lastFirstTerm[N_PARTITIONS];
        int64_t countLastFirstTerm[N_PARTITIONS];
        int64_t coordinatesLastFirstTerm[N_PARTITIONS];
        int64_t posElements[N_PARTITIONS];

        Statistics stats[N_PARTITIONS];
        int64_t skippedTables[N_PARTITIONS];

        StorageStrat storageStrategy[N_PARTITIONS];
        Factory<RowTableInserter> listFactory[N_PARTITIONS];
        Factory<ClusterTableInserter> comprFactory[N_PARTITIONS];
        Factory<ColumnTableInserter> list2Factory[N_PARTITIONS];
        Factory<NewColumnTableInserter> ncFactory[N_PARTITIONS];
        Factory<NewRowTableInserter> nrFactory[N_PARTITIONS];
        Factory<NewClusterTableInserter> ncluFactory[N_PARTITIONS];
        BinaryTableInserter *currentPairHandler[N_PARTITIONS];

        //Store the number of virtual tables per partition
        int64_t *ntables;
        //Store the number of all first terms in all tables
        int64_t *nFirstElsNTables;

        std::mutex mutex;

        int64_t getCoordinatesForPOS(const int p);
        void writeCurrentEntryIntoTree(int permutation, TripleWriter *posArray,
                TreeInserter *treeInserter,
                const bool aggregated,
                const bool canSkipTables);

        void storeInmemoryValuesIntoFiles(int permutation, int64_t* v1, int64_t* v2,
                int n, TripleWriter *posArray,
                const bool aggregated,
                const bool canSkipTables);


    public:
        Inserter(Root *tree, TableStorage **files, int64_t nTerms,
                bool useFixedStrategy, char fixedStrategy,
                const size_t thresholdSkipTable,
                int64_t *ntables, int64_t *nFirstElsNTables) : nTerms(nTerms),
        useFixedStrategy(useFixedStrategy), fixedStrategy(fixedStrategy),
        useRowForLargeTables(false),
        thresholdForColumnStorage(StorageStrat::getBinaryBreakingPoint()),
        thresholdSkipTable(thresholdSkipTable),
        ntables(ntables), nFirstElsNTables(nFirstElsNTables) {
            assert(thresholdSkipTable < THRESHOLD_KEEP_MEMORY);
            this->tree = tree;
            this->files = files;

            for (int i = 0; i < N_PARTITIONS; ++i) {
                currentT1[i] = -1;
                values1[i] = new int64_t[THRESHOLD_KEEP_MEMORY + 1];
                values2[i] = new int64_t[THRESHOLD_KEEP_MEMORY + 1];
                storageStrategy[i].init(/*NULL, NULL, NULL,*/ NULL, NULL, NULL,
                        &listFactory[i],
                        &comprFactory[i],
                        &list2Factory[i],
                        &ncFactory[i],
                        &nrFactory[i],
                        &ncluFactory[i]);
                currentPairHandler[i] = NULL;

                lastFirstTerm[i] = -1;
                countLastFirstTerm[i] = 0;
                posElements[i] = 0;
                coordinatesLastFirstTerm[i] = 0;
                skippedTables[i] = 0;
            }
            LOG(DEBUGL) << "Threshold for column layout is " << thresholdForColumnStorage;
        }

        void disableColumnStorage() {
            thresholdForColumnStorage = std::numeric_limits<size_t>::max();
        }

        void setUsageRowForLargeTables() {
            useRowForLargeTables = true;
        }

        bool insert(const int permutation, const int64_t t1, const int64_t t2,
                const int64_t t3, const int64_t count,
                TripleWriter *posArray, TreeInserter *treeInserter,
                const bool aggregated, const bool canSkipTables);

        void insert(nTerm key, TermCoordinates *value);

        std::string getPathPermutationStorage(const int perm);

        void flush(int permutation, TripleWriter *posArray,
                TreeInserter *treeInserter,
                const bool aggregated,
                const bool canSkipTables);

        void stopInserts(int permutation);

        Statistics *getStats(int permutation) {
            return &stats[permutation];
        }

        uint64_t getNTablesPerPartition(const int idx) const {
            return ntables[idx];
        }

        uint64_t getNSkippedTables(const int idx) const {
            return skippedTables[idx];
        }

        ~Inserter() {
            for (int i = 0; i < N_PARTITIONS; ++i) {
                delete[] values1[i];
                delete[] values2[i];
            }
        }
};

#endif /* INSERTER_H_ */
