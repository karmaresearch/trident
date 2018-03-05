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

bool _sort_by_number(const string &s1, const string &s2) {
    return atoi(Utils::filename(s1).c_str()) < atoi(Utils::filename(s2).c_str());
}

KB::KB(const char *path,
        bool readOnly,
        bool reasoning,
        bool dictEnabled,
        KBConfig &config,
        std::vector<string> locationUpdates) :
    path(path), readOnly(readOnly), isClosed(false), ntables(), nFirstTables(),
    dictEnabled(dictEnabled), config(config) {

        if (readOnly && !Utils::exists(string(path) + "/tree")) {
            LOG(ERRORL) << "The input path does not seem to be a valid KB";
            throw 10;
        }

        std::chrono::system_clock::time_point start = std::chrono::system_clock::now();

        //Get statistics and configuration
        string fileConf = path + string("/kbstats");
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
        string fileTree = path + string("/tree/");
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
            dictManager = new DictMgmt(*maindict.get(), string(path) + "/_diff",
                    dictHash, string(path) + "/e2r", string(path) + "/e2s");
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

        if (nindices == 3) {
            pso = new CacheIdx();
            osp = new CacheIdx();
        } else {
            pso = NULL;
            osp = NULL;
        }

        //Initialize the storage partitions
        for (int i = 0; i < nindices; ++i) {
            stringstream is;
            is << path << "/p" << i;
            if (readOnly) {
                //Check if there are actually files in the directory
                if  (!Utils::isEmpty(is.str())) {
                    files[i] = new TableStorage(readOnly, is.str(),
                            config.getParamLong(STORAGE_MAX_FILE_SIZE),
                            config.getParamInt(STORAGE_MAX_N_FILES),
                            NULL, stats, i);
                } else {
                    files[i] = NULL;
                }
            } else {
                files[i] = new TableStorage(readOnly, is.str(),
                        config.getParamLong(STORAGE_MAX_FILE_SIZE),
                        config.getParamInt(STORAGE_MAX_N_FILES),
                        bytesTracker[i], stats, i);
            }
        }

        //Is there some sample data available?
        string sampleDir = path + string("/_sample");
        if (Utils::exists(sampleDir)) {
            KBConfig sampleConfig;
            sampleKB = new KB(sampleDir.c_str(), true, false, false, sampleConfig);
            sampleRate = (double) sampleKB->getSize() / this->totalNumberTriples;
        } else {
            sampleKB = NULL;
            sampleRate = 0;
        }

        string defaultDiffDir = path + string("/_diff");
        if (Utils::exists(defaultDiffDir)) {
            std::vector<string> files = Utils::getSubdirs(defaultDiffDir);
            std::vector<string> childrenupdates;
            for (int i = 0; i < files.size(); ++i) {
                string f = files[i];
                string fn = Utils::filename(f);
                if (!fn.empty() && std::find_if(fn.begin(),  fn.end(),
                            [](char c) {
                            return !std::isdigit(c);
                            }) == fn.end()) {
                    childrenupdates.push_back(f);
                }
            }

            if (!childrenupdates.empty()) {
                std::vector<const char *> globalbuffers;
                globalbuffers.resize(6);
                //Instantiate the buffers
                for (int i = 0; i < 6; ++i)
                    globalbuffers[0] = NULL;
                if (Utils::exists(defaultDiffDir + "/s/p0")) {
                    spo_f = std::unique_ptr<ROMappedFile>(
                            new ROMappedFile(defaultDiffDir + "/s/p0"));
                    globalbuffers[IDX_SPO] = spo_f->getBuffer();
                }
                if (Utils::exists(defaultDiffDir + "/s/p1")) {
                    sop_f = std::unique_ptr<ROMappedFile>(
                            new ROMappedFile(defaultDiffDir + "/s/p1"));
                    globalbuffers[IDX_SOP] = sop_f->getBuffer();
                }
                if (Utils::exists(defaultDiffDir + "/p/p0")) {
                    pos_f = std::unique_ptr<ROMappedFile>(
                            new ROMappedFile(defaultDiffDir + "/p/p0"));
                    globalbuffers[IDX_POS] = pos_f->getBuffer();
                }
                if (Utils::exists(defaultDiffDir + "/p/p1")) {
                    pso_f = std::unique_ptr<ROMappedFile>(
                            new ROMappedFile(defaultDiffDir + "/p/p1"));
                    globalbuffers[IDX_PSO] = pso_f->getBuffer();
                }
                if (Utils::exists(defaultDiffDir + "/o/p0")) {
                    ops_f = std::unique_ptr<ROMappedFile>(
                            new ROMappedFile(defaultDiffDir + "/o/p0"));
                    globalbuffers[IDX_OPS] = ops_f->getBuffer();
                }
                if (Utils::exists(defaultDiffDir + "/o/p1")) {
                    osp_f = std::unique_ptr<ROMappedFile>(
                            new ROMappedFile(defaultDiffDir + "/o/p1"));
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
    string fileTree = path + string("/tree/");
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
    ss1 << path << "/dict/" << 0;
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
    ss2 << path << "/invdict/" << 0;
    maindict->invdict = std::shared_ptr<Root>(new Root(ss2.str(), NULL, readOnly, map));
}

Querier *KB::query() {
    return new Querier(tree, dictManager, files, totalNumberTriples,
            totalNumberTerms, nindices, ntables, nFirstTables,
            sampleKB, diffIndices);
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
    ss1 << path << "/dict/" << i;
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
    for (int i = 0; i < nindices; ++i) {
        if (files[i] != NULL) {
            delete files[i];
            files[i] = NULL;
        }
    }

    // Delete bytesTrackers after deleting all files, because
    // in read-only case, all files share the same bytesTracker. --Ceriel
    for (int i = 0; i < nindices; ++i) {
        if (bytesTracker[i] != NULL) {
            delete bytesTracker[i];
            bytesTracker[i] = NULL;
        }
    }

    if (pso != NULL) {
        delete pso;
        pso = NULL;
    }

    if (osp != NULL) {
        delete osp;
        osp = NULL;
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
        fos.open(this->path + string("/kbstats"));
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
    if (Utils::exists(inputdir + "/ADD")) {
        type = DiffIndex::TypeUpdate::ADDITION;
    } else {
        type = DiffIndex::TypeUpdate::DELETE;
    }

    if (Utils::exists(inputdir + "/type1")) {
        diffIndices.push_back(std::unique_ptr<DiffIndex>(
                    new DiffIndex1(inputdir, type)));
    } else {
        diffIndices.push_back(std::unique_ptr<DiffIndex>(
                    new DiffIndex3(inputdir, globalbuffers,
                        config, type)));
    }

    if (Utils::exists(inputdir + "/dict")) {
        //Load the dictionary
        DictMgmt::Dict ud;
        ud.sb = std::shared_ptr<StringBuffer>(new StringBuffer(inputdir + "/dict", true, 1, 1, ud.stats.get()));
        PropertyMap p;
        p.setBool(TEXT_KEYS, true);
        p.setBool(TEXT_VALUES, false);
        ud.dict = std::shared_ptr<Root>(new Root(inputdir + "/dict/t2id", ud.sb.get(), true, p));
        p.setBool(TEXT_KEYS, false);
        p.setBool(TEXT_VALUES, true);
        ud.invdict = std::shared_ptr<Root>(new Root(inputdir + "/dict/id2t", ud.sb.get(), true, p));

        std::ifstream fis;
        fis.open(inputdir + "/dict/stats");
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
