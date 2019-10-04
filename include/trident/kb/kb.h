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


#ifndef KBB_H_
#define KBB_H_

#include <trident/kb/consts.h>
#include <trident/kb/statistics.h>
#include <trident/kb/dictmgmt.h>
#include <trident/kb/kbconfig.h>
#include <trident/kb/cacheidx.h>
#include <trident/kb/diffindex.h>
#include <trident/utils/memorymgr.h>

#include <kognac/factory.h>

#include <string>

class Leaf;
class Querier;
class Inserter;
class TableStorage;
class Root;
class StringBuffer;
struct FileSegment;
class FileDescriptor;

using namespace std;

typedef enum e_GraphType { DEFAULT, DIRECTED, UNDIRECTED } GraphType;

class KB {
    private:
        const string path;

        const bool readOnly;

        Root *tree;
        Stats stats;
        bool isClosed;

        std::shared_ptr<DictMgmt::Dict> maindict;
        DictMgmt *dictManager;
        int dictPartitions;
        bool dictHash;
        std::vector<DictMgmt::Dict> dictUpdates;

        int64_t ntables[N_PARTITIONS];
        int64_t nFirstTables[N_PARTITIONS];

        int64_t totalNumberTriples;
        int64_t totalNumberTerms;
        int64_t nextID;
        GraphType graphType;

        int nindices;
        bool aggrIndices;
        bool incompleteIndices;

        bool useFixedStrategy;
        char storageFixedStrategy;

        size_t thresholdSkipTable;

        bool dictEnabled;
        bool relsIDsSep; //Do relations have their own IDs?

        double sampleRate;

        TableStorage *files[N_PARTITIONS];
        MemoryManager<FileDescriptor> *bytesTracker[N_PARTITIONS];

        CacheIdx *pso;
        CacheIdx *osp;

        KB *sampleKB;
        KBConfig config;

        //The data structures below handle updates
        std::vector<std::unique_ptr<DiffIndex>> diffIndices;
        std::unique_ptr<ROMappedFile> spo_f;
        std::unique_ptr<ROMappedFile> sop_f;
        std::unique_ptr<ROMappedFile> pos_f;
        std::unique_ptr<ROMappedFile> pso_f;
        std::unique_ptr<ROMappedFile> ops_f;
        std::unique_ptr<ROMappedFile> osp_f;

        void loadDict(KBConfig *config);

        void createNewDict(std::string dir);

        void createSingleUpdate(Querier *q, DiffIndex::TypeUpdate type, PairItr *itr,
                std::string dir, std::string diffDir);

    public:
        DDLEXPORT KB(const char *path, bool readOnly, bool reasoning,
                bool dictEnabled, KBConfig &config) : KB(path, readOnly, reasoning,
                    dictEnabled, config, std::vector<string>()) {
                }

        DDLEXPORT KB(const char *path, bool readOnly, bool reasoning,
                bool dictEnabled, KBConfig &config, std::vector<string> locationUpdates);

        DDLEXPORT Querier *query();

        DDLEXPORT Inserter *insert();

        DictMgmt *getDictMgmt() {
            return dictManager;
        }

        DDLEXPORT void mergeUpdates();

        void closeMainDict();

        void close();

        DDLEXPORT Stats getStats();

        DDLEXPORT Stats *getStatsDict();

        string getDictPath(int i);

        string getPath() {
            return path;
        }

        int getNIndices() const {
            return nindices;
        }

        uint64_t getNTerms() const {
            return totalNumberTerms;
        }

        uint64_t getNRels() const {
            return dictManager->getNRels();
        }

        bool areRelIDsSeparated() const {
            return relsIDsSep;
        }

        GraphType getGraphType() const {
            return graphType;
        }

        void setGraphType(GraphType t) {
            this->graphType = t;
        }

        int getNDictionaries() {
            return dictPartitions;
        }

        int getNTablesPerPartition(int idx) {
            return ntables[idx];
        }

        double getSampleRate() {
            return sampleRate;
        }

        int64_t getSize() {
            return totalNumberTriples;
        }

        int64_t getNextID() {
            return nextID;
        }

        Root* getRootTree();

        TreeItr *getItrTerms();

        std::vector<const char*> openAllFiles(int perm);

        void addDiffIndex(string inputdir, const char **globalbuffers, Querier *q);

        DDLEXPORT ~KB();
};

#endif /* KBB_H_ */
