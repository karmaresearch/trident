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
#include <trident/kb/kb.h>
#include <trident/kb/statistics.h>
#include <trident/kb/inserter.h>
#include <trident/kb/updater.h>
#include <trident/kb/kbconfig.h>
#include <trident/kb/querier.h>
#include <trident/mining/miner.h>
#include <trident/tests/common.h>

#ifdef SERVER
#include <trident/server/server.h>
#endif

#include <layers/TridentLayer.hpp>

#include <kognac/utils.h>
#include <kognac/progargs.h>

#ifdef ANALYTICS
#include <snap/analytics.h>
#include <snap/tasks.h>
#endif

#include <zstr/zstr.hpp>

#include <iostream>
#include <cstdlib>
#include <sstream>
#include <fstream>

using namespace std;

//Implemented in main_params.cpp
extern bool initParams(int argc, const char** argv, ProgramArgs &vm);
extern bool checkMachineConstraints();

//Implemented in main_sparql.cpp
extern void execNativeQuery(ProgramArgs &vm, Querier *q, KB &kb, bool silent);
extern void callRDF3X(TridentLayer &db, const string &queryFileName, bool explain,
        bool disableBifocalSampling, bool resultslookup);

//Implemented in main_ml.cpp
extern void launchML(KB &kb, string op, string algo, string paramsLearn,
        string paramsPredict);
extern void subgraphEval(KB &kb, ProgramArgs &vm);
extern void subgraphCreate(KB &kb, ProgramArgs &vm);

//Implemented in kb.cpp
extern bool _sort_by_number(const string &s1, const string &s2);

void lookup(DictMgmt *dict, ProgramArgs &vm) {
    if (vm.count("text")) {
        nTerm value;
        string textTerm = vm["text"].as<string>();
        if (!dict->getNumber((char*) textTerm.c_str(), textTerm.size(), &value)) {
            cout << "Term " << textTerm << " not found" << endl;
        } else {
            cout << value << endl;
        }
    } else {
        nTerm key = vm["number"].as<int64_t>();
        char supportText[4096];
        if (!dict->getText(key, supportText)) {
            cout << "Term " << key << " not found" << endl;
        } else {
            cout << supportText << endl;
        }
    }
}

void dump(KB *kb, string outputdir) {
    if (Utils::exists(outputdir)) {
        LOG(INFOL) << "Removing the output directory ... " << outputdir;
        Utils::remove_all(outputdir);
    }
    Utils::create_directories(outputdir);

    //Create output file
    string filename = outputdir + DIR_SEP + "graph.gz";
    zstr::ofstream out(filename, std::ios_base::out);

    //Get dictionary
    Querier *q = kb->query();
    DictMgmt *dict = q->getDictMgmt();
    std::unique_ptr<char[]> supportBuffer = std::unique_ptr<char[]>(new char[MAX_TERM_SIZE + 2]);

    const bool print_p = kb->getGraphType() == GraphType::DEFAULT;
    PairItr *itr = q->get(IDX_SOP, -1, -1, -1);
    while (itr->hasNext()) {
        int64_t s, p, o;
        itr->next();
        s = itr->getKey();
        o = itr->getValue1();
        bool resp = dict->getText(s, supportBuffer.get());
        if (resp) {
            out << supportBuffer.get() << "\t";
        } else {
            out << "ID:" << s << "\t";
        }
        if (print_p) {
            p = itr->getValue2();
            int size;
            resp = dict->getTextRel(p, supportBuffer.get(), size);
            if (resp) {
                out << supportBuffer.get() << "\t";
            } else {
                out << "ID:" << p << "\t";
            }
        }
        resp = dict->getText(o, supportBuffer.get());
        if (resp) {
            out << supportBuffer.get();
        } else {
            out << "ID:" << o;
        }
        out << std::endl;
    }
    LOG(INFOL) << "Finished dumping";
    q->releaseItr(itr);
    delete q;
}

void testkb(string kbDir, ProgramArgs &vm) {
    if (!Utils::exists(kbDir)) {
        //Load the KB
        Loader loader;
        int sampleMethod = PARSE_COUNTMIN;
        string dictMethod = vm["dictMethod"].as<string>();

        ParamsLoad p;
        p.inputformat = vm["inputformat"].as<string>();
        p.onlyCompress = vm["onlyCompress"].as<bool>();
        p.inputCompressed = false;
        p.triplesInputDir = vm["tripleFiles"].as<string>();
        p.dictDir = "";
        p.dictDir_rel = "";
        p.tmpDir = kbDir;
        p.kbDir = kbDir;
        p.dictMethod = dictMethod;
        p.sampleMethod = sampleMethod;
        p.sampleArg = vm["popArg"].as<int>();
        p.parallelThreads = vm["maxThreads"].as<int>();
        p.maxReadingThreads = vm["readThreads"].as<int>();
        p.dictionaries = vm["ndicts"].as<int>();
        p.nindices = vm["nindices"].as<int>();
        p.createIndicesInBlocks = vm["incrindices"].as<bool>();
        p.aggrIndices = vm["aggrIndices"].as<bool>();
        p.canSkipTables = vm["skipTables"].as<bool>();
        p.enableFixedStrat = vm["enableFixedStrat"].as<bool>();
        p.fixedStrat = vm["fixedStrat"].as<int>();
        p.storePlainList = true;
        p.sample = vm["sample"].as<bool>();
        p.sampleRate = vm["sampleRate"].as<double>();
        p.thresholdSkipTable = vm["thresholdSkipTable"].as<int>();
        //p.logPtr = NULL;
        p.timeoutStats = -1;
        p.remoteLocation = "";
        p.limitSpace = 0;
        p.graphTransformation = vm["gf"].as<string>();
        p.storeDicts = vm["storedicts"].as<bool>();
        p.flatTree = vm["flatTree"].as<bool>();

        loader.load(p);
    }

    KBConfig config;
    std::vector<string> locUpdates;
    KB kb(kbDir.c_str(), true, false, true, config, locUpdates);

    std::vector<int> permutations;
    string testperms = vm["testperms"].as<string>();
    //split the string in tokens
    istringstream f(testperms);
    string s;
    while (getline(f, s, ';')) {
        permutations.push_back(stoi(s));
    }
    TestTrident test(&kb);
    //Check whether there is a _diff dir
    if (Utils::exists(kbDir + DIR_SEP + "_diff")) {
        std::vector<string> ups = Utils::getSubdirs(kbDir + DIR_SEP + "_diff");
        std::vector<string> childrenupdates;
        for (int i = 0; i < ups.size(); ++i) {
            string f = ups[i];
            string fn = Utils::filename(f);
            if (!fn.empty() && std::find_if(fn.begin(),  fn.end(),
                        [](char c) {
                        return !isdigit(c);
                        }) == fn.end()) {
                childrenupdates.push_back(f);
            }
        }
        sort(childrenupdates.begin(), childrenupdates.end(), _sort_by_number);
        locUpdates = childrenupdates;
    }
    test.prepare(kbDir + DIR_SEP + string("p0") + DIR_SEP + "raw", locUpdates);
    test.test_existing(permutations);
    test.test_nonexist(permutations);
    test.test_moveto(permutations);
}

void printInfo(KB &kb) {
    if (kb.areRelIDsSeparated()) {
        cout << "Dictionary: entities and relations have different dictionaries" << endl;
        cout << "N. Entities: " << kb.getNTerms() << endl;
        cout << "N. Rels: " << kb.getNRels() << endl;
    } else {
        cout << "Dictionary: shared between entities and relations" << endl;
        cout << "N. Terms: " << kb.getNTerms() << endl;
    }
    cout << "N. Edges: " << kb.getSize() << endl;
    if (kb.getGraphType() == GraphType::DIRECTED) {
        cout << "Type: Directed Graph without labeled relations" << endl;
    } else if (kb.getGraphType() == GraphType::UNDIRECTED) {
        cout << "Type: Undirected Graph without labeled relations" << endl;
    } else {
        cout << "Type: Directed Graph with labeled relations" << endl;
    }
}

#ifdef SERVER
void startServer(KB &kb, int port, int nthreads) {
    std::unique_ptr<TridentServer> webint;
    webint = std::unique_ptr<TridentServer>(
            new TridentServer(kb, "./../webinterface", nthreads));
    webint->start(port);
    LOG(INFOL) << "Server is launched at 0.0.0.0:" << to_string(port);
    webint->join();
}
#endif

void printStats(KB &kb, Querier *q) {
    LOG(DEBUGL) << "Max mem (MB) " << Utils::get_max_mem();
    LOG(DEBUGL) << "# Read Index Blocks = " << kb.getStats().getNReadIndexBlocks();
    LOG(DEBUGL) << " Read Index Bytes from disk = " << kb.getStats().getNReadIndexBytes();
    Querier::Counters c = q->getCounters();
    LOG(DEBUGL) << "RowLayouts: " << c.statsRow << " ClusterLayouts: " << c.statsCluster << " ColumnLayouts: " << c.statsColumn;
    LOG(DEBUGL) << "AggrIndices: " << c.aggrIndices << " NotAggrIndices: " << c.notAggrIndices << " CacheIndices: " << c.cacheIndices;
    LOG(DEBUGL) << "Permutations: spo " << c.spo << " ops " << c.ops << " pos " << c.pos << " sop " << c.sop << " osp " << c.osp << " pso " << c.pso;
    int64_t nblocks = 0;
    int64_t nbytes = 0;
    for (int i = 0; i < kb.getNDictionaries(); ++i) {
        nblocks = kb.getStatsDict()[i].getNReadIndexBlocks();
        nbytes = kb.getStatsDict()[i].getNReadIndexBytes();
    }
    LOG(DEBUGL) << "# Read Dictionary Blocks = " << nblocks;
    LOG(DEBUGL) << "# Read Dictionary Bytes from disk = " << nbytes;
    LOG(DEBUGL) << "Process IO Read bytes = " << Utils::getIOReadBytes();
    LOG(DEBUGL) << "Process IO Read char = " << Utils::getIOReadChars();
}

#ifdef ANALYTICS
void launchAnalytics(KB &kb, string op, string param1, string param2) {
    if (!AnalyticsTasks::getInstance().isValidTask(op)) {
        LOG(ERRORL) << "Task " << op << " not recognized";
        throw 10;
    }
    AnalyticsTasks::getInstance().load(op, param2);
    Analytics::run(kb, op, param1, param2);
}
#endif

#ifdef ML
void mineFrequentPatterns(string kbdir, int minLen, int maxLen, int64_t minSupport) {
    LOG(INFOL) << "Mining frequent graphs";
    Miner miner(kbdir, 1000);
    miner.mine();
    miner.getFrequentPatterns(minLen, maxLen, minSupport);
}
#endif

int main(int argc, const char** argv) {
    //Check some constraints
    if (!checkMachineConstraints()) {
        return EXIT_FAILURE;
    }

    //Init params
    ProgramArgs vm;
    if (!initParams(argc, argv, vm)) {
        return EXIT_FAILURE;
    }
    //Set logging level
    string ll = vm["logLevel"].as<string>();
    if (ll == "debug") {
        Logger::setMinLevel(DEBUGL);
    } else if (ll == "info") {
        Logger::setMinLevel(INFOL);
    } else if (ll == "warning") {
        Logger::setMinLevel(WARNL);
    } else if (ll == "error") {
        Logger::setMinLevel(ERRORL);
    }
    if (vm["logfile"].as<string>() != "") {
        Logger::logToFile(vm["logfile"].as<string>());
    }

    //Print params
    if (Logger::getMinLevel() <= DEBUGL) {
        string params = "";
        for(int i = 0; i < argc; ++i) {
            params += string(argv[i]) + " ";
        }
        LOG(DEBUGL) << "PARAMS: " << params;
    }

    //Init the knowledge base
    string cmd = string(argv[1]);
    string kbDir = vm["input"].as<string>();

    if (cmd == "query") {
#ifdef SPARQL
        KBConfig config;
        std::vector<string> locUpdates;
        KB kb(kbDir.c_str(), true, false, true, config, locUpdates);
        TridentLayer layer(kb);
        callRDF3X(layer, vm["query"].as<string>(), vm["explain"].as<bool>(),
                vm["disbifsampl"].as<bool>(), vm["decodeoutput"].as<bool>());

        int repeatQuery = vm["repeatQuery"].as<int>();
        ofstream file("/dev/null");
        streambuf* strm_buffer = cout.rdbuf();
        cout.rdbuf(file.rdbuf());
        while (repeatQuery > 0 && !vm["explain"].as<bool>()) {
            callRDF3X(layer, vm["query"].as<string>(), false,
                    vm["disbifsampl"].as<bool>(), vm["decodeoutput"].as<bool>());
            repeatQuery--;
        }
        cout.rdbuf(strm_buffer);
        printStats(kb, layer.getQuerier());
#else
        LOG(ERRORL) << "Trident is not compiled with support to advanced SPARQL querying. Add -DSPARQL=1 to cmake";
        return EXIT_FAILURE;
#endif
    } else if (cmd == "query_native") {
#ifdef SPARQL
        KBConfig config;
        KB kb(kbDir.c_str(), true, false, true, config);
        Querier *q = kb.query();
        execNativeQuery(vm, q, kb, ! vm["decodeoutput"].as<bool>());

        int repeatQuery = vm["repeatQuery"].as<int>();
        ofstream file("/dev/null");
        streambuf* strm_buffer = cout.rdbuf();
        cout.rdbuf(file.rdbuf());
        while (repeatQuery > 0) {
            execNativeQuery(vm, q, kb, ! vm["decodeoutput"].as<bool>());
            repeatQuery--;
        }
        cout.rdbuf(strm_buffer);
        printStats(kb, q);
        delete q;
#else
        LOG(ERRORL) << "Trident is not compiled with support to advanced SPARQL querying. Add -DSPARQL=1 to cmake";
        return EXIT_FAILURE;
#endif
    } else if (cmd == "lookup") {
        KBConfig config;
        KB kb(kbDir.c_str(), true, false, true, config);
        lookup(kb.getDictMgmt(), vm);
    } else if (cmd == "testkb") {
        testkb(kbDir, vm);
    } else if (cmd == "testcq") {
        string inputFile = kbDir + DIR_SEP + string("p0") + DIR_SEP + string("raw");
        _test_createqueries(inputFile, vm["testqueryfile"].as<string>());
    } else if (cmd == "testti") {
        TridentTimings ti(kbDir, vm["testqueryfile"].as<string>());
        ti.launchTests();
    } else if (cmd == "load" || cmd == "compress") {
        Loader loader;
        int sampleMethod;
        if (vm["popMethod"].as<string>() == string("sample")) {
            sampleMethod = PARSE_SAMPLE;
        } else {
            sampleMethod = PARSE_COUNTMIN;
        }
        string dictMethod = vm["dictMethod"].as<string>();
        string tmpDir = kbDir;
        if (!vm["tmpdir"].empty()) {
            tmpDir = vm["tmpdir"].as<string>();
        }

        string inputDir;
        string dictDir = "";
        string dictDir_rel = "";
        bool inputCompressed = false;
        if (!vm["comprinput"].empty()) {
            inputDir = vm["comprinput"].as<string>();
            dictDir = vm["comprdict"].as<string>();
            dictDir_rel = vm["comprdict_rel"].as<string>();
            inputCompressed = true;
        } else {
            inputDir = vm["tripleFiles"].as<string>();
        }

        ParamsLoad p;
        p.inputformat = vm["inputformat"].as<string>();
        p.onlyCompress = vm["onlyCompress"].as<bool>();
        p.inputCompressed = inputCompressed;
        p.triplesInputDir = inputDir;
        p.dictDir = dictDir;
        p.dictDir_rel = dictDir_rel;
        p.tmpDir = tmpDir;
        p.kbDir = kbDir;
        p.dictMethod = dictMethod;
        p.sampleMethod = sampleMethod;
        p.sampleArg = vm["popArg"].as<int>();
        p.parallelThreads = vm["maxThreads"].as<int>();
        p.maxReadingThreads = vm["readThreads"].as<int>();
        p.dictionaries = vm["ndicts"].as<int>();
        p.nindices = vm["nindices"].as<int>();
        p.createIndicesInBlocks = vm["incrindices"].as<bool>();
        p.aggrIndices = vm["aggrIndices"].as<bool>();
        p.canSkipTables = vm["skipTables"].as<bool>();
        p.enableFixedStrat = vm["enableFixedStrat"].as<bool>();
        p.fixedStrat = vm["fixedStrat"].as<int>();
        p.storePlainList = vm["storeplainlist"].as<bool>();
        p.sample = vm["sample"].as<bool>();
        p.sampleRate = vm["sampleRate"].as<double>();
        p.thresholdSkipTable = vm["thresholdSkipTable"].as<int>();
        //p.logPtr = logptr;
        p.timeoutStats = vm["timeoutStats"].as<int>();
        p.remoteLocation = vm["remoteLoc"].as<string>();
        p.limitSpace = vm["limitSpace"].as<int64_t>();
        p.graphTransformation = vm["gf"].as<string>();
        p.storeDicts = vm["storedicts"].as<bool>();
        p.relsOwnIDs = vm["relsOwnIDs"].as<bool>();
        p.flatTree = vm["flatTree"].as<bool>();

        loader.load(p);

    } else if (cmd == "info") {
        KBConfig config;
        KB kb(kbDir.c_str(), true, false, true, config);
        printInfo(kb);
    } else if (cmd == "add") {
        string updatedir = vm["update"].as<string>();
        Updater up;
        up.creatediffupdate(DiffIndex::TypeUpdate::ADDITION_df, kbDir, updatedir);
    } else if (cmd == "rm") {
        string updatedir = vm["update"].as<string>();
        Updater up;
        up.creatediffupdate(DiffIndex::TypeUpdate::DELETE_df, kbDir, updatedir);
    } else if (cmd == "merge") {
        KBConfig config;
        KB kb(kbDir.c_str(), true, false, true, config);
        kb.mergeUpdates();
    } else if (cmd == "analytics") {
#ifdef ANALYTICS
        KBConfig config;
        KB kb(kbDir.c_str(), true, false, true, config);
        launchAnalytics(kb, vm["op"].as<string>(), vm["oparg1"].as<string>(),
                vm["oparg2"].as<string>());
#else
        LOG(ERRORL) << "Trident was not compiled with the analytics enabled. Add -DANALYTICS=1 to cmake";
        return EXIT_FAILURE;
#endif
    } else if (cmd == "mine")  {
#ifdef ML
        int64_t minSupport = vm["minSupport"].as<int64_t>();
        int minLen = vm["minLen"].as<int>();
        int maxLen = vm["maxLen"].as<int>();
        mineFrequentPatterns(kbDir, minLen, maxLen, minSupport);
#else
        LOG(ERRORL) << "Trident was not compiled with the ML parameter enabled. Add -DML=1 to cmake";
        return EXIT_FAILURE;
#endif
    } else if (cmd == "dump") {
        KBConfig config;
        KB kb(kbDir.c_str(), true, false, true, config);
        dump(&kb, vm["output"].as<string>());
    } else if (cmd == "server") {
#ifdef SERVER
        KBConfig config;
        KB kb(kbDir.c_str(), true, false, true, config);
        startServer(kb, vm["port"].as<int>(), vm["webthreads"].as<int>());
#else
        LOG(ERRORL) << "Trident was not compiled with the webserver. Add -DSERVER=1 to cmake";
        return EXIT_FAILURE;
#endif
    } else if (cmd == "learn") {
#ifdef ML
        KBConfig config;
        KB kb(kbDir.c_str(), true, false, true, config);
        launchML(kb, cmd, vm["algo"].as<string>(), vm["args_learn"].as<string>(), "");
#else
        LOG(ERRORL) << "Trident was not compiled with the ML parameter enabled. Add -DML=1 to cmake";
        return EXIT_FAILURE;
#endif
    } else if (cmd == "predict") {
#ifdef ML
        KBConfig config;
        KB kb(kbDir.c_str(), true, false, true, config);
        launchML(kb, cmd, vm["algo"].as<string>(), "", vm["args_predict"].as<string>());
#else
        LOG(ERRORL) << "Trident was not compiled with the ML parameter enabled. Add -DML=1 to cmake";
        return EXIT_FAILURE;
#endif
    } else if (cmd == "subeval") {
#ifdef ML
        KBConfig config;
        KB kb(kbDir.c_str(), true, false, true, config);
        subgraphEval(kb, vm);
#else
        LOG(ERRORL) << "Trident was not compiled with the ML parameter enabled. Add -DML=1 to cmake";
        return EXIT_FAILURE;
#endif
    } else if (cmd == "subcreate") {
#ifdef ML
        KBConfig config;
        KB kb(kbDir.c_str(), true, false, true, config);
        subgraphCreate(kb, vm);
#else
        LOG(ERRORL) << "Trident was not compiled with the ML parameter enabled. Add -DML=1 to cmake";
        return EXIT_FAILURE;
#endif
    }



    //Print other stats
    LOG(INFOL) << "Max memory used: " << Utils::get_max_mem() << " MB";
    return EXIT_SUCCESS;
}
