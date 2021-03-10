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


#include <trident/kb/memoryopt.h>
#include <trident/kb/kb.h>
#include <trident/kb/querier.h>
#include <trident/kb/inserter.h>
#include <trident/kb/consts.h>
#include <trident/kb/kbconfig.h>
#include <trident/tree/root.h>
#include <trident/tree/flatroot.h>
#include <trident/tree/stringbuffer.h>
#include <trident/binarytables/tableshandler.h>

#include <string>
#include <iostream>
#include <fstream>
#include <stdlib.h>
#include <cmath>
#include <chrono>

using namespace std;

LIBEXP bool _sort_by_number(const string &s1, const string &s2);

bool _sort_by_number(const string &s1, const string &s2) {
    return atoi(Utils::filename(s1).c_str()) < atoi(Utils::filename(s2).c_str());
}

KB::KB(const char *path,
        bool readOnly,
        bool reasoning,
        bool dictEnabled,
        KBConfig &config,
        std::vector<string> locationUpdates) :
    path(path), readOnly(readOnly), ntables(), nFirstTables(), isClosed(false),
    dictEnabled(dictEnabled), config(config) {

        if (readOnly && !Utils::exists(string(path) + DIR_SEP + "tree")) {
            LOG(ERRORL) << "The input path does not seem to be a valid KB";
            throw 10;
        }

        std::chrono::system_clock::time_point start = std::chrono::system_clock::now();

        //Get statistics and configuration
        string fileConf = path + DIR_SEP + string("kbstats");
        if (Utils::exists(fileConf)) {
            std::ifstream fis;
            fis.open(fileConf);
            char data[8];
            fis.read(data, 8);
            dictPartitions = (int)Utils::decode_long(data, 0);
            fis.read(data, 8);
            totalNumberTerms = Utils::decode_long(data, 0);
            fis.read(data, 8);
            totalNumberTriples = Utils::decode_long(data, 0);
            fis.read(data, 8);
            nextID = Utils::decode_long(data, 0);

            fis.read(data, 4);
            nindices = Utils::decode_int(data, 0);
            stringstream ind;
            bool first = true;
            for (int i = 0; i < N_PARTITIONS; i++) {
                stringstream is;
                is << path << DIR_SEP << "p" << i;
                if (Utils::exists(is.str())) {
                    if (! first) {
                        ind << ";";
                    }
                    first = false;
                    ind << i;
                    present[i] = true;
                } else {
                    present[i] = false;
                }
            }
            indices = ind.str();
            fis.read(data, 1);
            if (data[0]) {
                aggrIndices = true;
            } else {
                aggrIndices = false;
            }
            incompleteIndices = fis.get() != 0;
            dictHash = fis.get() != 0;

            //Read the number of tables per partitions
            for (int i = 0; i < N_PARTITIONS; ++i) {
                fis.read(data, 8);
                ntables[i] = Utils::decode_long(data, 0);
                fis.read(data, 8);
                nFirstTables[i] = Utils::decode_long(data, 0);
            }

            //Get graphType
            fis.read(data, 4);
            graphType = (GraphType) Utils::decode_int(data, 0);

            //Check whether the relations have their own IDs
            fis.read(data, 1);
            relsIDsSep = data[0];

            fis.close();
        } else {
            dictPartitions = config.getParamInt(DICTPARTITIONS);
            dictHash = config.getParamBool(DICTHASH);
            totalNumberTerms = 0;
            totalNumberTriples = 0;
            nextID = 0;
            graphType = GraphType::DEFAULT;
            relsIDsSep = config.getParamBool(RELSOWNIDS);
            nindices = config.getParamInt(NINDICES);
            indices = config.getParam(INDICES);
            if (indices == "") {
                // Fill in some default values.
                if (nindices == 1) {
                    indices = "0";
                } else if (nindices == 3) {
                    indices = "0;1;2";
                } else if (nindices == 4) {
                    indices = "0;1;3;4";
                } else if (nindices == 6) {
                    indices = "0;1;2;3;4;5";
                }
                // Other defaults? TODO
            }
            aggrIndices = config.getParamBool(AGGRINDICES);
            incompleteIndices = config.getParamBool(INCOMPLINDICES);
            useFixedStrategy = config.getParamBool(USEFIXEDSTRAT);
            storageFixedStrategy = (char) config.getParamInt(FIXEDSTRAT);
            thresholdSkipTable = config.getParamInt(THRESHOLD_SKIP_TABLE);
        }

        //Optimize the memory management
        if (reasoning) {
            MemoryOptimizer::optimizeForReasoning(dictPartitions, config);
        } else if (readOnly) {
            MemoryOptimizer::optimizeForReading(dictPartitions, config);
        }

        //Initialize the tree
        string fileTree = path + DIR_SEP + string("tree") + DIR_SEP;
        string flatTree = fileTree + string("flat");
        if (readOnly && Utils::exists(flatTree)) {
            tree = new FlatRoot(flatTree, graphType != GraphType::DEFAULT, graphType == GraphType::UNDIRECTED);
        } else {
            PropertyMap map;
            map.setBool(TEXT_KEYS, false);
            map.setBool(TEXT_VALUES, false);
            map.setBool(COMPRESSED_NODES, false);
            map.setInt(LEAF_SIZE_PREALL_FACTORY,
                    config.getParamInt(TREE_MAXPREALLLEAVESCACHE));
            map.setInt(LEAF_SIZE_FACTORY, config.getParamInt(TREE_MAXLEAVESCACHE));
            map.setInt(MAX_NODES_IN_CACHE, config.getParamInt(TREE_MAXNODESINCACHE));
            map.setInt(NODE_MIN_BYTES, config.getParamInt(TREE_NODEMINBYTES));
            map.setLong(CACHE_MAX_SIZE, config.getParamLong(TREE_MAXSIZECACHETREE));
            map.setInt(FILE_MAX_SIZE, config.getParamInt(TREE_MAXFILESIZE));
            map.setInt(MAX_N_OPENED_FILES, config.getParamInt(TREE_MAXNFILES));
            map.setInt(MAX_EL_PER_NODE, config.getParamInt(TREE_MAXELEMENTSNODE));

            map.setInt(LEAF_MAX_PREALL_INTERNAL_LINES,
                    config.getParamInt(TREE_MAXPREALLINTERNALLINES));
            map.setInt(LEAF_MAX_INTERNAL_LINES,
                    config.getParamInt(TREE_MAXINTERNALLINES));
            map.setInt(LEAF_ARRAYS_FACTORY_SIZE, config.getParamInt(TREE_FACTORYSIZE));
            map.setInt(LEAF_ARRAYS_PREALL_FACTORY_SIZE,
                    config.getParamInt(TREE_ALLOCATEDELEMENTS));

            map.setInt(NODE_KEYS_FACTORY_SIZE,
                    config.getParamInt(TREE_NODE_KEYS_FACTORY_SIZE));
            map.setInt(NODE_KEYS_PREALL_FACTORY_SIZE,
                    config.getParamInt(TREE_NODE_KEYS_PREALL_FACTORY_SIZE));
            tree = new Root(fileTree, NULL, readOnly, map);
        }

        std::chrono::duration<double> sec = std::chrono::system_clock::now()
            - start;
        LOG(DEBUGL) << "Time init tree KB = " << sec.count() * 1000 << " ms and " << Utils::get_max_mem() << " MB occupied";

        //Initialize the dictionaries
        if (dictEnabled) {
            loadDict(&config);
            sec = std::chrono::system_clock::now() - start;
            LOG(DEBUGL) << "Time init dictionaries KB = " <<
                sec.count() * 1000 << " ms and " <<
                Utils::get_max_mem() << " MB occupied";
            dictManager = new DictMgmt(*maindict.get(), string(path) + DIR_SEP + "_diff",
                    dictHash, string(path) + DIR_SEP + "e2r", string(path) + DIR_SEP + "e2s");
        }

        //Initialize the memory tracker for the storage partitions
        bytesTracker[0] = new MemoryManager<FileDescriptor>(
                config.getParamLong(STORAGE_CACHE_SIZE));
        for (int i = 1; i < nindices; ++i) {
            if (!readOnly) {
                bytesTracker[i] = new MemoryManager<FileDescriptor>(
                        config.getParamLong(STORAGE_CACHE_SIZE));
            } else {
                bytesTracker[i] = NULL;
            }
        }
        for (int i = nindices; i < N_PARTITIONS; i++) {
            bytesTracker[i] = NULL;
        }

        std::vector<int> permutations;
        istringstream f(indices);
        string s;
        while (getline(f, s, ';')) {
            permutations.push_back(stoi(s));
        }

        for (int i = 0; i < N_PARTITIONS; i++) {
            files[i] = NULL;
        }

        //Initialize the storage partitions
        for (int i = 0; i < permutations.size(); ++i) {
            stringstream is;
            is << path << DIR_SEP << "p" << permutations[i];
            if (readOnly) {
                //Check if there are actually files in the directory
                if  (!Utils::isEmpty(is.str())) {
                    files[permutations[i]] = new TableStorage(readOnly, is.str(),
                            config.getParamLong(STORAGE_MAX_FILE_SIZE),
                            config.getParamInt(STORAGE_MAX_N_FILES),
                            NULL, stats, permutations[i]);
                }
            } else {
                files[permutations[i]] = new TableStorage(readOnly, is.str(),
                        config.getParamLong(STORAGE_MAX_FILE_SIZE),
                        config.getParamInt(STORAGE_MAX_N_FILES),
                        bytesTracker[i], stats, permutations[i]);
            }
        }

        //Is there some sample data available?
        string sampleDir = path + DIR_SEP + string("_sample");
        if (Utils::exists(sampleDir)) {
            KBConfig sampleConfig;
            sampleKB = new KB(sampleDir.c_str(), true, false, false, sampleConfig);
            sampleRate = (double) sampleKB->getSize() / this->totalNumberTriples;
        } else {
            sampleKB = NULL;
            sampleRate = 0;
        }

        string defaultDiffDir = path + DIR_SEP + string("_diff");
        if (Utils::exists(defaultDiffDir)) {
            std::vector<string> files = Utils::getSubdirs(defaultDiffDir);
            std::vector<string> childrenupdates;
            for (int i = 0; i < files.size(); ++i) {
                string f = files[i];
                string fn = Utils::filename(f);
                if (!fn.empty() && std::find_if(fn.begin(),  fn.end(),
                            [](char c) {
                            return !isdigit(c);
                            }) == fn.end()) {
                    childrenupdates.push_back(f);
                }
            }

            if (!childrenupdates.empty()) {
                std::vector<const char *> globalbuffers;
                globalbuffers.resize(N_PARTITIONS);
                //Instantiate the buffers
                for (int i = 0; i < N_PARTITIONS; ++i)
                    globalbuffers[i] = NULL;
                if (Utils::exists(defaultDiffDir + DIR_SEP + "s" + DIR_SEP + "p0")) {
                    spo_f = std::unique_ptr<ROMappedFile>(
                            new ROMappedFile(defaultDiffDir + DIR_SEP + "s" + DIR_SEP + "p0"));
                    globalbuffers[IDX_SPO] = spo_f->getBuffer();
                }
                if (Utils::exists(defaultDiffDir + DIR_SEP + "s" + DIR_SEP + "p1")) {
                    sop_f = std::unique_ptr<ROMappedFile>(
                            new ROMappedFile(defaultDiffDir + DIR_SEP + "s" + DIR_SEP + "p1"));
                    globalbuffers[IDX_SOP] = sop_f->getBuffer();
                }
                if (Utils::exists(defaultDiffDir + DIR_SEP + "p" + DIR_SEP + "p0")) {
                    pos_f = std::unique_ptr<ROMappedFile>(
                            new ROMappedFile(defaultDiffDir + DIR_SEP + "p" + DIR_SEP + "p0"));
                    globalbuffers[IDX_POS] = pos_f->getBuffer();
                }
                if (Utils::exists(defaultDiffDir + DIR_SEP + "p" + DIR_SEP + "p1")) {
                    pso_f = std::unique_ptr<ROMappedFile>(
                            new ROMappedFile(defaultDiffDir + DIR_SEP + "p" + DIR_SEP + "p1"));
                    globalbuffers[IDX_PSO] = pso_f->getBuffer();
                }
                if (Utils::exists(defaultDiffDir + DIR_SEP + "o" + DIR_SEP + "p0")) {
                    ops_f = std::unique_ptr<ROMappedFile>(
                            new ROMappedFile(defaultDiffDir + DIR_SEP + "o" + DIR_SEP + "p0"));
                    globalbuffers[IDX_OPS] = ops_f->getBuffer();
                }
                if (Utils::exists(defaultDiffDir + DIR_SEP + "o" + DIR_SEP + "p1")) {
                    osp_f = std::unique_ptr<ROMappedFile>(
                            new ROMappedFile(defaultDiffDir + DIR_SEP + "o" + DIR_SEP + "p1"));
                    globalbuffers[IDX_OSP] = osp_f->getBuffer();
                }

                //Sort them by numeric value
                sort(childrenupdates.begin(), childrenupdates.end(), _sort_by_number);
                for (int i = 0; i < childrenupdates.size(); ++i) {
                    std::chrono::system_clock::time_point startDiff = std::chrono::system_clock::now();
                    addDiffIndex(childrenupdates[i], &globalbuffers[0], NULL);
                    sec = std::chrono::system_clock::now() - startDiff;
                    LOG(DEBUGL) << "Time loading diff index " << sec.count() * 1000 << "ms.";
                }
            }
            if (!dictUpdates.empty()) {
                dictManager->addUpdates(dictUpdates);
                //update total number of terms and nextID fields
                for (auto itr = dictUpdates.begin(); itr != dictUpdates.end(); ++itr) {
                    totalNumberTerms += itr->size;
                    nextID = max(nextID, itr->nextid);
                }
            }
            //check also the global dictionary container in dictmanager
            totalNumberTerms += dictManager->getGUDSize();
            nextID = max(nextID, (int64_t) dictManager->getLargestGUDTerm() + 1);
        }

        sec = std::chrono::system_clock::now() - start;
        LOG(DEBUGL) << "Time init KB = " << sec.count() * 1000 << " ms and " << Utils::get_max_mem() << " MB occupied";
    }

Root *KB::getRootTree() {
    string fileTree = path + DIR_SEP + string("tree") + DIR_SEP;
    PropertyMap map;
    map.setBool(TEXT_KEYS, false);
    map.setBool(TEXT_VALUES, false);
    map.setBool(COMPRESSED_NODES, false);
    map.setInt(LEAF_SIZE_PREALL_FACTORY,
            config.getParamInt(TREE_MAXPREALLLEAVESCACHE));
    map.setInt(LEAF_SIZE_FACTORY, config.getParamInt(TREE_MAXLEAVESCACHE));
    map.setInt(MAX_NODES_IN_CACHE, config.getParamInt(TREE_MAXNODESINCACHE));
    map.setInt(NODE_MIN_BYTES, config.getParamInt(TREE_NODEMINBYTES));
    map.setLong(CACHE_MAX_SIZE, config.getParamLong(TREE_MAXSIZECACHETREE));
    map.setInt(FILE_MAX_SIZE, config.getParamInt(TREE_MAXFILESIZE));
    map.setInt(MAX_N_OPENED_FILES, config.getParamInt(TREE_MAXNFILES));
    map.setInt(MAX_EL_PER_NODE, config.getParamInt(TREE_MAXELEMENTSNODE));

    map.setInt(LEAF_MAX_PREALL_INTERNAL_LINES,
            config.getParamInt(TREE_MAXPREALLINTERNALLINES));
    map.setInt(LEAF_MAX_INTERNAL_LINES,
            config.getParamInt(TREE_MAXINTERNALLINES));
    map.setInt(LEAF_ARRAYS_FACTORY_SIZE, config.getParamInt(TREE_FACTORYSIZE));
    map.setInt(LEAF_ARRAYS_PREALL_FACTORY_SIZE,
            config.getParamInt(TREE_ALLOCATEDELEMENTS));

    map.setInt(NODE_KEYS_FACTORY_SIZE,
            config.getParamInt(TREE_NODE_KEYS_FACTORY_SIZE));
    map.setInt(NODE_KEYS_PREALL_FACTORY_SIZE,
            config.getParamInt(TREE_NODE_KEYS_PREALL_FACTORY_SIZE));
    return new Root(fileTree, NULL, true, map);
}

void KB::loadDict(KBConfig *config) {
    maindict = std::shared_ptr<DictMgmt::Dict>(new DictMgmt::Dict());

    PropertyMap map;
    map.setBool(TEXT_KEYS, true);
    map.setBool(TEXT_VALUES, false);
    map.setBool(COMPRESSED_NODES, false);
    map.setInt(LEAF_SIZE_PREALL_FACTORY,
            config->getParamInt(DICT_MAXPREALLLEAVESCACHE));
    map.setInt(LEAF_SIZE_FACTORY, config->getParamInt(DICT_MAXLEAVESCACHE));
    map.setInt(MAX_NODES_IN_CACHE, config->getParamInt(DICT_MAXNODESINCACHE));
    map.setInt(NODE_MIN_BYTES, config->getParamInt(DICT_NODEMINBYTES));
    map.setLong(CACHE_MAX_SIZE, config->getParamLong(DICT_MAXSIZECACHETREE));
    map.setInt(FILE_MAX_SIZE, config->getParamInt(DICT_MAXFILESIZE));
    map.setInt(MAX_N_OPENED_FILES, config->getParamInt(DICT_MAXNFILES));
    map.setInt(MAX_EL_PER_NODE, config->getParamInt(DICT_MAXELEMENTSNODE));
    map.setInt(NODE_KEYS_FACTORY_SIZE,
            config->getParamInt(DICT_NODE_KEYS_FACTORY_SIZE));
    map.setInt(NODE_KEYS_PREALL_FACTORY_SIZE,
            config->getParamInt(DICT_NODE_KEYS_PREALL_FACTORY_SIZE));

    stringstream ss1;
    ss1 << path << DIR_SEP << "dict" << DIR_SEP << 0;
    maindict->sb = std::shared_ptr<StringBuffer>(new StringBuffer(ss1.str(), readOnly,
                config->getParamInt(SB_PREALLBUFFERS),
                config->getParamLong(SB_CACHESIZE),
                maindict->stats.get()));
    maindict->dict = std::shared_ptr<Root>(new Root(ss1.str(), maindict->sb.get(), readOnly, map));

    //Initialize the inverse dictionaries
    map.setBool(TEXT_KEYS, false);
    map.setBool(TEXT_VALUES, true);
    map.setBool(COMPRESSED_NODES, false);
    map.setInt(LEAF_SIZE_PREALL_FACTORY,
            config->getParamInt(INVDICT_MAXPREALLLEAVESCACHE));
    map.setInt(LEAF_SIZE_FACTORY, config->getParamInt(INVDICT_MAXLEAVESCACHE));
    map.setInt(MAX_NODES_IN_CACHE,
            config->getParamInt(INVDICT_MAXNODESINCACHE));
    map.setInt(NODE_MIN_BYTES, config->getParamInt(INVDICT_NODEMINBYTES));
    map.setLong(CACHE_MAX_SIZE, config->getParamLong(INVDICT_MAXSIZECACHETREE));
    map.setInt(FILE_MAX_SIZE, config->getParamInt(INVDICT_MAXFILESIZE));
    map.setInt(MAX_N_OPENED_FILES, config->getParamInt(INVDICT_MAXNFILES));
    map.setInt(MAX_EL_PER_NODE, config->getParamInt(INVDICT_MAXELEMENTSNODE));
    map.setInt(NODE_KEYS_FACTORY_SIZE,
            config->getParamInt(INVDICT_NODE_KEYS_FACTORY_SIZE));
    map.setInt(NODE_KEYS_PREALL_FACTORY_SIZE,
            config->getParamInt(INVDICT_NODE_KEYS_PREALL_FACTORY_SIZE));

    stringstream ss2;
    ss2 << path << DIR_SEP << "invdict" << DIR_SEP << 0;
    maindict->invdict = std::shared_ptr<Root>(new Root(ss2.str(), NULL, readOnly, map));
}

Querier *KB::query() {
    return new Querier(tree, dictManager, files, totalNumberTriples,
            totalNumberTerms, nindices, ntables, nFirstTables,
            sampleKB, diffIndices, present);
}

Inserter *KB::insert() {
    if (readOnly) {
        LOG(ERRORL) << "Insert() is not available if the knowledge base is opened in read_only mode.";
    }

    return new Inserter(tree,
            files,
            totalNumberTerms + (dictEnabled ? dictManager->getNTermsInserted() : 0),
            useFixedStrategy,
            storageFixedStrategy,
            thresholdSkipTable,
            ntables,
            nFirstTables);
}

void KB::closeMainDict() {
    maindict = std::shared_ptr<DictMgmt::Dict>();
    dictManager->clean();
}

string KB::getDictPath(int i) {
    stringstream ss1;
    ss1 << path << DIR_SEP << "dict" << DIR_SEP << i;
    return ss1.str();
}

TreeItr *KB::getItrTerms() {
    return tree->itr();
}

Stats KB::getStats() {
    return stats;
}

Stats *KB::getStatsDict() {
    return maindict->stats.get();
}

void KB::close() {
    if (isClosed)
        return;

    //Update stats about the KB
    if (!readOnly) {
        if (dictEnabled) {
            totalNumberTerms += dictManager->getNTermsInserted();
            nextID = dictManager->getLargestIDInserted() + 1;
        }
        if (this->graphType != GraphType::DEFAULT) {
            totalNumberTriples += files[IDX_SOP]->getNTriplesInserted();
        } else {
            totalNumberTriples += files[IDX_SPO]->getNTriplesInserted();
        }
    }

    if (tree != NULL) {
        delete tree;
        tree = NULL;
    }
    if (dictEnabled) {
        if (dictManager != NULL) {
            dictManager->clean();
            delete dictManager;
            dictManager = NULL;
        }
    }
    for (int i = 0; i < N_PARTITIONS; ++i) {
        if (files[i] != NULL) {
            delete files[i];
            files[i] = NULL;
        }
    }

    // Delete bytesTrackers after deleting all files, because
    // in read-only case, all files share the same bytesTracker. --Ceriel
    for (int i = 0; i < N_PARTITIONS; ++i) {
        if (bytesTracker[i] != NULL) {
            delete bytesTracker[i];
            bytesTracker[i] = NULL;
        }
    }

    diffIndices.clear();
    isClosed = true;
}

KB::~KB() {
    close();

    if (sampleKB != NULL) {
        delete sampleKB;
        sampleKB = NULL;
    }

    if (!readOnly) {
        //Write a file with some statistics
        std::ofstream fos;
        fos.open(this->path + DIR_SEP + string("kbstats"));
        char data[8];
        //Write the number of dictionaries
        Utils::encode_long(data, 0, dictPartitions);
        fos.write(data, 8);

        //Write the total number of terms
        Utils::encode_long(data, 0, totalNumberTerms);
        fos.write(data, 8);

        //Write the total number of triples
        Utils::encode_long(data, 0, totalNumberTriples);
        fos.write(data, 8);

        //Write the highest ID in the KB
        Utils::encode_long(data, 0, nextID);
        fos.write(data, 8);

        //Write number indices
        Utils::encode_int(data, 0, nindices);
        fos.write(data, 4);

        //Write aggregated indices
        data[0] = aggrIndices ? 1 : 0;
        fos.write(data, 1);

        //Write whether the indices are complete or not
        fos.put(incompleteIndices);

        //Dict is hash based?
        fos.put(dictHash);

        //Write the number of virtual tables per partition
        for (int i = 0; i < N_PARTITIONS; ++i) {
            Utils::encode_long(data, 0, ntables[i]);
            fos.write(data, 8);
            Utils::encode_long(data, 0, nFirstTables[i]);
            fos.write(data, 8);
        }

        //Write down the type of the graph
        Utils::encode_int(data, 0, (int)graphType);
        fos.write(data, 4);

        //Write down whether separate IDs were used for the relations
        if (relsIDsSep) {
            data[0] = 1;
        } else {
            data[0] = 0;
        }
        fos.write(data, 1);
        fos.close();
    }
}

void KB::addDiffIndex(string inputdir, const char **globalbuffers, Querier *q) {
    DiffIndex::TypeUpdate type;
    if (Utils::exists(inputdir + DIR_SEP + "ADD")) {
        type = DiffIndex::TypeUpdate::ADDITION_df;
    } else {
        type = DiffIndex::TypeUpdate::DELETE_df;
    }

    if (Utils::exists(inputdir + DIR_SEP + "type1")) {
        diffIndices.push_back(std::unique_ptr<DiffIndex>(
                    new DiffIndex1(inputdir, type)));
    } else {
        diffIndices.push_back(std::unique_ptr<DiffIndex>(
                    new DiffIndex3(inputdir, globalbuffers,
                        config, type)));
    }

    if (Utils::exists(inputdir + DIR_SEP + "dict")) {
        //Load the dictionary
        DictMgmt::Dict ud;
        ud.sb = std::shared_ptr<StringBuffer>(new StringBuffer(inputdir + DIR_SEP + "dict", true, 1, 64 * 1024 * 1024, ud.stats.get()));
        PropertyMap p;
        p.setBool(TEXT_KEYS, true);
        p.setBool(TEXT_VALUES, false);
        ud.dict = std::shared_ptr<Root>(new Root(inputdir + DIR_SEP + "dict" + DIR_SEP + "t2id", ud.sb.get(), true, p));
        p.setBool(TEXT_KEYS, false);
        p.setBool(TEXT_VALUES, true);
        ud.invdict = std::shared_ptr<Root>(new Root(inputdir + DIR_SEP + "dict" + DIR_SEP + "id2t", ud.sb.get(), true, p));

        std::ifstream fis;
        fis.open(inputdir + DIR_SEP + "dict" + DIR_SEP + "stats");
        char data[8];
        fis.read(data, 8);
        ud.size = Utils::decode_long(data);
        fis.read(data, 8);
        ud.nextid = Utils::decode_long(data);

        dictUpdates.push_back(ud);
    }

    if (q) {
        q->initDiffIndex(diffIndices.back().get());
    }
}

std::vector<const char*> KB::openAllFiles(int perm) {
    return files[perm]->loadAllFiles();
}

void KB::createSingleUpdate(DiffIndex::TypeUpdate type, PairItr *itr, std::string dir, std::string diffDir, Querier *q) {

    std::vector<uint64_t> all_s;
    std::vector<uint64_t> all_p;
    std::vector<uint64_t> all_o;

    while (itr->hasNext()) {
        itr->next();
        all_s.push_back(itr->getKey());
        all_p.push_back(itr->getValue1());
        all_o.push_back(itr->getValue2());
    }

    if (all_s.size() == 0) {
        return;
    }

    // Filter: the only deletes that should be left are in the db.
    if (type == DiffIndex::TypeUpdate::DELETE_df) {
        std::vector<uint64_t> del_s;
        std::vector<uint64_t> del_p;
        std::vector<uint64_t> del_o;

        PairItr *kbitr = q->getIterator(IDX_SPO, -1, -1, -1);
        if (kbitr->hasNext()) {
            kbitr->next();
        } else {
            q->releaseItr(kbitr);
            kbitr = NULL;
        }

        size_t i = 0;
        while (i != all_s.size()) {
            //move kbitr to the good position
            int cmpResult;
            while (kbitr && (cmpResult = cmp(kbitr, all_s[i], all_p[i], all_o[i])) < 0) {
                if (kbitr->hasNext()) {
                    kbitr->next();
                } else {
                    q->releaseItr(kbitr);
                    kbitr = NULL;
                }
            }
            if (kbitr) {
                //do the check
                if (cmpResult == 0) {
                    del_s.push_back(all_s[i]);
                    del_p.push_back(all_p[i]);
                    del_o.push_back(all_o[i]);
                    //Must move also the original iterator
                    if (kbitr->hasNext()) {
                        kbitr->next();
                    } else {
                        break;
                    }
                }
                i++;
            } else {
                break;
            }
        }
        if (kbitr)
            q->releaseItr(kbitr);

        if (del_s.size() == 0) {
            return;
        }
        Utils::create_directories(dir);
        DiffIndex3::createDiffIndex(type, dir, diffDir, del_s, del_p, del_o, true, q, true);
    } else {
        Utils::create_directories(dir);
        DiffIndex3::createDiffIndex(type, dir, diffDir, all_s, all_p, all_o, true, q, true);
    }

    //Write the type of file
    string flagup;
    if (type == DiffIndex::TypeUpdate::ADDITION_df) {
        flagup = dir + DIR_SEP + std::string("ADD");
    } else {
        flagup = dir + DIR_SEP +  std::string("DEL");
    }
    ofstream ofs(flagup);
    ofs.close();
}

int KB::cmp(PairItr *itr, uint64_t s, uint64_t p, uint64_t o) {
    if (itr->getKey() < s) {
        return -1;
    } else if (itr->getKey() == s) {
        if (itr->getValue1() < p) {
            return -1;
        } else if (itr->getValue1() == p) {
            if (itr->getValue2() < o) {
                return -1;
            } else if (itr->getValue2() == o) {
                return 0;
            } else {
                return 1;
            }
        } else {
            return 1;
        }
    } else {
        return 1;
    }
}


void KB::mergeUpdates() {

    std::chrono::system_clock::time_point start = std::chrono::system_clock::now();

    std::string diffDir = path + DIR_SEP + std::string("_newdiff");
    std::string oldDiffDir = path + DIR_SEP + std::string("_diff");

    // Count number of ADDITIONs and DELETEs.
    int addCount = 0;
    int rmCount = 0;
    for (size_t i = 0; i < diffIndices.size(); ++i) {
        if (diffIndices[i]->getType() == DiffIndex::TypeUpdate::ADDITION_df) {
            addCount++;
        } else {
            rmCount++;
        }
    }

    if (addCount <= 1 && rmCount <= 1) {
        // No merging needed.
        return;
    }

    Querier *q = query();
    // Create the single updates with respect to a querier that does not have the diffIndices.
    // Create querier with empty diffs
    std::vector<std::unique_ptr<DiffIndex>> diffs;

    Querier *q1 = new Querier(tree, dictManager, files, totalNumberTriples,
        totalNumberTerms, nindices, ntables, nFirstTables,
        sampleKB, diffs, present);

    if (addCount >= 1) {
        PairItr *addItr = q->summaryAddDiff();

        createSingleUpdate(DiffIndex::TypeUpdate::ADDITION_df, addItr, diffDir + DIR_SEP + "0", diffDir, q1);
        q->releaseItr(addItr);
    }
    if (rmCount >= 1) {
        PairItr *rmItr = q->summaryRmDiff();
        createSingleUpdate(DiffIndex::TypeUpdate::DELETE_df, rmItr, diffDir + DIR_SEP + (addCount != 0 ? "1" : "0"), diffDir, q1);
        q->releaseItr(rmItr);
    }

    delete q1;
    delete q;

    if (dictUpdates.size() != 0) {
        createNewDict(diffDir + DIR_SEP + "0");
    }

    std::string old = oldDiffDir + std::string(".old");

    if (Utils::exists(old)) {
        Utils::remove_all(old);
    }

    if (std::rename(oldDiffDir.c_str(), old.c_str()) != 0) {
        LOG(ERRORL) << "Error renaming " << oldDiffDir;
        throw 10;
    }
    if (std::rename(diffDir.c_str(), oldDiffDir.c_str()) != 0) {
        LOG(ERRORL) << "Error renaming " << diffDir;
        // Try to rename the old one back
        std::rename(old.c_str(), oldDiffDir.c_str());
        // Utils::remove_all(diffDir);
        throw 10;
    }
    // Utils::remove_all(old);
    std::chrono::duration<double> sec = std::chrono::system_clock::now() - start;
    LOG(INFOL) << "Total merge time = " << sec.count() * 1000 << " ms.";
}

void KB::createNewDict(std::string dir) {
    std::string dictdir = dir + DIR_SEP + std::string("dict");
    Utils::create_directories(dictdir);

    // Init data structures
    Stats stats;
    std::unique_ptr<StringBuffer> sb = std::unique_ptr<StringBuffer>(
                                           new StringBuffer(dictdir, false, 10, 4096*SB_BLOCK_SIZE,
                                                   &stats));
    PropertyMap conf;
    conf.setBool(TEXT_KEYS, true);
    conf.setBool(TEXT_VALUES, false);
    string t2idir = dictdir + "/t2id";
    Utils::create_directories(t2idir);
    std::unique_ptr<Root> dict = std::unique_ptr<Root>(new Root(t2idir, sb.get(),
                                 false, conf));
    string i2tdir = dictdir + "/id2t";
    Utils::create_directories(i2tdir);
    conf.setBool(TEXT_KEYS, false);
    conf.setBool(TEXT_VALUES, true);
    std::unique_ptr<Root> invdict = std::unique_ptr<Root>(new Root(i2tdir,
                                    sb.get(), false, conf));

    uint64_t maxid = 0;
    uint64_t count = 0;
    LOG(DEBUGL) << "dictUpdates count = " << dictUpdates.size();
    for (int i = 0; i < dictUpdates.size(); ++i) {
        TreeItr *itr = dictUpdates[i].invdict->itr();
        while (itr->hasNext()) {
            int64_t coord;
            nTerm key = itr->next(coord);
            int size;
            char value[MAX_TERM_SIZE+1];
            dictUpdates[i].sb->get(coord, value, size);
            value[size] = 0;
            coord = sb->getSize();
            dict->put((tTerm*) value, size, key);
            LOG(TRACEL) << "Adding " << value << " to dict";
            invdict->put(key, coord);
            if (key > maxid) {
                maxid = key;
            }
            count++;
        }
    }

    ofstream ofs;
    ofs.open(dictdir + "/stats");
    char data[8];
    Utils::encode_long(data, count);
    ofs.write(data, 8);
    Utils::encode_long(data, maxid + 1);
    ofs.write(data, 8);
    ofs.close();
}
