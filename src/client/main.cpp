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
#include <trident/binarytables/storagestrat.h>
#include <trident/mining/miner.h>
#include <trident/tests/common.h>
#include <trident/server/server.h>
#include <trident/ml/transe.h>
#include <trident/sparql/query.h>
#include <trident/sparql/plan.h>
#include <trident/utils/batch.h>

#include <kognac/utils.h>

#include <snap/tasks.h>

//SNAP dependencies
#include <snap/analytics.h>
//END SNAP dependencies

//RDF3X dependencies
#include <layers/TridentLayer.hpp>
#include <cts/parser/SPARQLLexer.hpp>
#include <cts/parser/SPARQLParser.hpp>
#include <cts/infra/QueryGraph.hpp>
#include <cts/semana/SemanticAnalysis.hpp>
#include <cts/plangen/PlanGen.hpp>
#include <cts/codegen/CodeGen.hpp>
#include <rts/runtime/Runtime.hpp>
#include <rts/runtime/QueryDict.hpp>
#include <rts/operator/Operator.hpp>
#include <rts/operator/PlanPrinter.hpp>
#include <rts/operator/ResultsPrinter.hpp>
//END RDF3x dependencies

#include <boost/chrono.hpp>
#include <boost/log/trivial.hpp>
#include <boost/log/utility/setup/console.hpp>
#include <boost/log/utility/setup/file.hpp>
#include <boost/log/core.hpp>
#include <boost/log/expressions.hpp>
#include <boost/log/support/date_time.hpp>
#include <boost/log/utility/setup/common_attributes.hpp>
#include <boost/filesystem/operations.hpp>
#include <boost/filesystem/path.hpp>
#include <boost/program_options.hpp>

#include <boost/spirit/include/qi.hpp>
#include <boost/spirit/include/karma.hpp>
#include <boost/fusion/adapted/std_pair.hpp>

#include <iostream>
#include <cstdlib>
#include <sstream>
#include <fstream>

using namespace std;
namespace timens = boost::chrono;
namespace logging = boost::log;
namespace fs = boost::filesystem;
namespace po = boost::program_options;

//Defined main_params.cpp
extern bool checkMachineConstraints();
extern bool checkParams(po::variables_map &vm, int argc, const char** argv,
        po::options_description &desc,
        std::map<string,po::options_description> &sections);
extern bool initParams(int argc, const char** argv, po::variables_map &vm);
extern void printHelp(const char *programName, string section,
        po::options_description &desc,
        std::map<string,po::options_description> &sections);

SinkPtr initLogging(
        logging::trivial::severity_level level,
        bool consoleActive,
        bool fileActive,
        string filelog) {

    logging::add_common_attributes();

    if (consoleActive) {
        logging::add_console_log(std::cerr, logging::keywords::format =
                (logging::expressions::stream << "["
                 << logging::expressions::attr <
                 boost::log::attributes::current_thread_id::value_type > (
                     "ThreadID") << " "
                 << logging::expressions::format_date_time <
                 boost::posix_time::ptime > ("TimeStamp",
                     "%m-%d %H:%M:%S") << " - "
                 << logging::trivial::severity << "] "
                 << logging::expressions::smessage));
    }

    SinkPtr sink;
    if (fileActive) {
        sink = logging::add_file_log(filelog, logging::keywords::format =
                (logging::expressions::stream << "["
                 << logging::expressions::attr <
                 boost::log::attributes::current_thread_id::value_type > (
                     "ThreadID") << " "
                 << logging::expressions::format_date_time <
                 boost::posix_time::ptime > ("TimeStamp",
                     "%m-%d %H:%M:%S") << " - "
                 << logging::trivial::severity << "] "
                 << logging::expressions::smessage));
    }

    boost::shared_ptr<logging::core> core = logging::core::get();
    core->set_filter(logging::trivial::severity >= level);
    return sink;
}

void lookup(DictMgmt *dict, po::variables_map &vm) {
    if (vm.count("text")) {
        nTerm value;
        string textTerm = vm["text"].as<string>();
        if (!dict->getNumber((char*) textTerm.c_str(), textTerm.size(), &value)) {
            cout << "Term " << textTerm << " not found" << endl;
        } else {
            cout << value << endl;
        }
    } else {
        nTerm key = vm["number"].as<long>();
        char supportText[4096];
        if (!dict->getText(key, supportText)) {
            cout << "Term " << key << " not found" << endl;
        } else {
            cout << supportText << endl;
        }
    }
}

void dump(KB *kb, string outputdir) {
    if (fs::exists(outputdir)) {
        BOOST_LOG_TRIVIAL(info) << "Removing the output directory ... " << outputdir;
        fs::remove_all(outputdir);
    }
    fs::create_directories(outputdir);

    //Create output file
    string filename = outputdir + "/graph.gz";
    ofstream ntFile;
    ntFile.open(filename, std::ios_base::out);
    boost::iostreams::filtering_stream<boost::iostreams::output> out;
    out.push(boost::iostreams::gzip_compressor());
    out.push(ntFile);

    //Get dictionary
    Querier *q = kb->query();
    DictMgmt *dict = q->getDictMgmt();
    std::unique_ptr<char[]> supportBuffer = std::unique_ptr<char[]>(new char[MAX_TERM_SIZE + 2]);

    const bool print_p = kb->getGraphType() == GraphType::DEFAULT;
    PairItr *itr = q->get(IDX_SOP, -1, -1, -1);
    while (itr->hasNext()) {
        long s, p, o;
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
            resp = dict->getText(p, supportBuffer.get());
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
    BOOST_LOG_TRIVIAL(info) << "Finished dumping";
    q->releaseItr(itr);
    delete q;
}

std::unique_ptr<Query> createQueryFromRF3XQueryGraph(SPARQLParser &parser,
        QueryGraph &graph) {
    std::vector<Pattern*> patterns;
    std::vector<Filter*> filters;
    std::vector<std::string> projections;

    for (QueryGraph::projection_iterator itr = graph.projectionBegin();
            itr != graph.projectionEnd(); ++itr) {
        //Get the text
        projections.push_back(parser.getVariableName(*itr));
    }

    //Create a "Pattern" object for each element in the graph
    QueryGraph::SubQuery &sb = graph.getQuery();
    for (auto el : sb.nodes) {
        Pattern *pattern = new Pattern();
        if (!el.constSubject) {
            pattern->addVar(0, parser.getVariableName(el.subject));
        } else {
            pattern->subject(el.subject);
        }
        if (!el.constPredicate) {
            pattern->addVar(1, parser.getVariableName(el.predicate));
        } else {
            pattern->predicate(el.predicate);
        }
        if (!el.constObject) {
            pattern->addVar(2, parser.getVariableName(el.object));
        } else {
            pattern->object(el.object);
        }

        patterns.push_back(pattern);
        filters.push_back(NULL);
        el.additionalData = (void*) pattern;
    }

    return std::unique_ptr<Query>(new Query(patterns, filters, projections));
}

void parseQuery(bool &success,
        SPARQLParser &parser,
        QueryGraph &queryGraph,
        QueryDict &queryDict,
        TridentLayer &db) {

    //Sometimes the query introduces new constants which need an ID
    try {
        parser.parse();
    } catch (const SPARQLParser::ParserException& e) {
        cerr << "parse error: " << e.message << endl;
        success = false;
        return;
    }

    // And perform the semantic anaylsis
    try {
        SemanticAnalysis semana(db, queryDict);
        semana.transform(parser, queryGraph);
    } catch (const SemanticAnalysis::SemanticException& e) {
        cerr << "semantic error: " << e.message << endl;
        success = false;
        return;
    }
    if (queryGraph.knownEmpty()) {
        cout << "<empty result -- known empty>" << endl;
        success = false;
        return;
    }

    success = true;
    return;
}

void callRDF3X(TridentLayer &db, const string &queryFileName, bool explain,
        bool disableBifocalSampling, bool resultslookup) {
    QueryDict queryDict(db.getNextId());
    QueryGraph queryGraph;
    bool parsingOk;

    // Parse the query
    string queryContent = "";
    if (queryFileName == "") {
        //Read it from STDIN
        cout << "SPARQL Query:" << endl;
        getline(cin, queryContent);
        cout << "QUERY " << queryContent << endl;
    } else {
        std::fstream inFile;
        inFile.open(queryFileName);//open the input file
        std::stringstream strStream;
        strStream << inFile.rdbuf();//read the file
        queryContent = strStream.str();
    }

    SPARQLLexer lexer(queryContent);
    SPARQLParser parser(lexer);

    boost::chrono::system_clock::time_point start = boost::chrono::system_clock::now();
    parseQuery(parsingOk, parser, queryGraph,
            queryDict, db);
    if (!parsingOk) {
        boost::chrono::duration<double> duration = boost::chrono::system_clock::now() - start;
        BOOST_LOG_TRIVIAL(info) << "Runtime queryopti: 0ms.";
        BOOST_LOG_TRIVIAL(info) << "Runtime queryexec: 0ms.";
        BOOST_LOG_TRIVIAL(info) << "Runtime totalexec: " << duration.count() * 1000 << "ms.";
        BOOST_LOG_TRIVIAL(info) << "# rows = 0";
        return;
    }

    // Run the optimizer
    PlanGen plangen;
    Plan* plan = NULL;
    if (!disableBifocalSampling) {
        plan = plangen.translate(db, queryGraph);
    } else {
        db.disableBifocalSampling();
        plan = plangen.translate(db, queryGraph, false);
    }

    if (!plan) {
        cerr << "internal error plan generation failed" << endl;
        return;
    }
    boost::chrono::duration<double> durationO = boost::chrono::system_clock::now() - start;

    // Build a physical plan
    Runtime runtime(db, NULL, &queryDict);
    Operator* operatorTree = CodeGen().translate(runtime, queryGraph, plan, !resultslookup);

    // Execute it
    if (explain) {
        DebugPlanPrinter out(runtime, false);
        operatorTree->print(out);
        delete operatorTree;
    } else {
        boost::chrono::system_clock::time_point startQ = boost::chrono::system_clock::now();
        if (operatorTree->first()) {
            while (operatorTree->next());
        }
        boost::chrono::duration<double> durationQ = boost::chrono::system_clock::now() - startQ;
        boost::chrono::duration<double> duration = boost::chrono::system_clock::now() - start;
        BOOST_LOG_TRIVIAL(info) << "Runtime queryopti: " << durationO.count() * 1000 << "ms.";
        BOOST_LOG_TRIVIAL(info) << "Runtime queryexec: " << durationQ.count() * 1000 << "ms.";
        BOOST_LOG_TRIVIAL(info) << "Runtime totalexec: " << duration.count() * 1000 << "ms.";
        ResultsPrinter *p = (ResultsPrinter*) operatorTree;
        long nElements = p->getPrintedRows();
        BOOST_LOG_TRIVIAL(info) << "# rows = " << nElements;
        delete operatorTree;
    }
}

//Defined in kb.cpp
extern bool _sort_by_number(const string &s1, const string &s2);

void testkb(string kbDir, po::variables_map &vm) {
    if (!fs::exists(fs::path(kbDir))) {
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
        p.logPtr = NULL;
        p.timeoutStats = -1;
        p.remoteLocation = "";
        p.limitSpace = 0;
        p.graphTransformation = vm["gf"].as<string>();
        p.storeDicts = vm["storedicts"].as<bool>();

        loader.load(p);
    }

    KBConfig config;
    //string updatesDir = vm["updates"].as<string>();
    std::vector<string> locUpdates;
    //if (updatesDir.length() > 0)
    //    locUpdates.push_back(updatesDir);
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
    if (fs::exists(kbDir + "/_diff")) {
        std::vector<string> ups = Utils::getSubdirs(kbDir + "/_diff");
        std::vector<string> childrenupdates;
        for (int i = 0; i < ups.size(); ++i) {
            string f = ups[i];
            string fn = fs::path(f).filename().string();
            if (!fn.empty() && std::find_if(fn.begin(),  fn.end(),
                        [](char c) {
                        return !std::isdigit(c);
                        }) == fn.end()) {
                childrenupdates.push_back(f);
            }
        }
        sort(childrenupdates.begin(), childrenupdates.end(), _sort_by_number);
        locUpdates = childrenupdates;
    }
    test.prepare(kbDir + string("/p0/raw"), locUpdates);
    test.test_existing(permutations);
    test.test_nonexist(permutations);
    test.test_moveto(permutations);
}

void execNativeQuery(po::variables_map &vm, Querier *q, KB &kb, bool silent);

void printInfo(KB &kb) {
    cout << "N. Terms: " << kb.getNTerms() << endl;
    cout << "N. Triples: " << kb.getSize() << endl;
    if (kb.getGraphType() == GraphType::DIRECTED) {
        cout << "Type: Directed Graph without labelled predicates" << endl;
    } else if (kb.getGraphType() == GraphType::UNDIRECTED) {
        cout << "Type: Undirected Graph without labelled predicates" << endl;
    } else {
        cout << "Type: Generic Graph" << endl;
    }
}

void startServer(KB &kb, int port) {
    fs::path full_path( fs::initial_path<fs::path>());
    std::unique_ptr<TridentServer> webint;
    webint = std::unique_ptr<TridentServer>(
            new TridentServer(kb, full_path.string() + "/../webinterface"));
    webint->start("0.0.0.0", to_string(port));
    BOOST_LOG_TRIVIAL(info) << "Server is launched at 0.0.0.0:" << to_string(port);
    webint->join();
}

void printStats(KB &kb, Querier *q) {
    BOOST_LOG_TRIVIAL(debug) << "Max mem (MB) " << Utils::get_max_mem();
    BOOST_LOG_TRIVIAL(debug) << "# Read Index Blocks = " << kb.getStats().getNReadIndexBlocks();
    BOOST_LOG_TRIVIAL(debug) << " Read Index Bytes from disk = " << kb.getStats().getNReadIndexBytes();
    Querier::Counters c = q->getCounters();
    BOOST_LOG_TRIVIAL(debug) << "RowLayouts: " << c.statsRow << " ClusterLayouts: " << c.statsCluster << " ColumnLayouts: " << c.statsColumn;
    BOOST_LOG_TRIVIAL(debug) << "AggrIndices: " << c.aggrIndices << " NotAggrIndices: " << c.notAggrIndices << " CacheIndices: " << c.cacheIndices;
    BOOST_LOG_TRIVIAL(debug) << "Permutations: spo " << c.spo << " ops " << c.ops << " pos " << c.pos << " sop " << c.sop << " osp " << c.osp << " pso " << c.pso;
    long nblocks = 0;
    long nbytes = 0;
    for (int i = 0; i < kb.getNDictionaries(); ++i) {
        nblocks = kb.getStatsDict()[i].getNReadIndexBlocks();
        nbytes = kb.getStatsDict()[i].getNReadIndexBytes();
    }
    BOOST_LOG_TRIVIAL(debug) << "# Read Dictionary Blocks = " << nblocks;
    BOOST_LOG_TRIVIAL(debug) << "# Read Dictionary Bytes from disk = " << nbytes;
    BOOST_LOG_TRIVIAL(debug) << "Process IO Read bytes = " << Utils::getIOReadBytes();
    BOOST_LOG_TRIVIAL(debug) << "Process IO Read char = " << Utils::getIOReadChars();
}

void launchAnalytics(KB &kb, string op, string param1, string param2) {
    if (!AnalyticsTasks::getInstance().isValidTask(op)) {
        BOOST_LOG_TRIVIAL(error) << "Task " << op << " not recognized";
        throw 10;
    }
    AnalyticsTasks::getInstance().load(op, param2);
    Analytics::run(kb, op, param1, param2);
}

void launchML(KB &kb, string op, string params) {
    BOOST_LOG_TRIVIAL(info) << "Launching " << op << " " << params << " ...";
    //Parse the params
    std::map<std::string,std::string> mapparams;
    std::string::iterator first = params.begin();
    std::string::iterator last  = params.end();
    const bool result = boost::spirit::qi::phrase_parse(first, last,
            *( *(boost::spirit::qi::char_-"=")  >> boost::spirit::qi::lit("=") >> *(boost::spirit::qi::char_-";") >> -boost::spirit::qi::lit(";") ),
            boost::spirit::ascii::space, mapparams);
    if (!result) {
        BOOST_LOG_TRIVIAL(error) << "Parsing params " << params << " has failed!";
        return;
    }
    BOOST_LOG_TRIVIAL(debug) << "Parsed params: " << boost::spirit::karma::format(*(boost::spirit::karma::string << '=' <<
                boost::spirit::karma::string), mapparams);

    if (op == "transe") {
        if (!kb.areRelIDsSeparated()) {
            BOOST_LOG_TRIVIAL(error) << "The KB is not loaded with separated Rel IDs. TranSE cannot be applied.";
            return;
        }
        uint16_t epochs = 100;
        const uint32_t ne = kb.getNTerms();
        auto dict = kb.getDictMgmt();
        const uint32_t nr = dict->getNRels();
        uint16_t dim = 50;
        uint32_t batchsize = 1000;
        uint16_t nthreads = 1;
        if (mapparams.count("dim")) {
            dim = boost::lexical_cast<uint16_t>(mapparams["dim"]);
        }
        if (mapparams.count("epochs")) {
            epochs = boost::lexical_cast<uint16_t>(mapparams["epochs"]);
        }
        if (mapparams.count("batchsize")) {
            batchsize = boost::lexical_cast<uint32_t>(mapparams["batchsize"]);
        }
        if (mapparams.count("nthreads")) {
            nthreads = boost::lexical_cast<uint16_t>(mapparams["nthreads"]);
        }

        BOOST_LOG_TRIVIAL(debug) << "Launching TranSE with epochs=" << epochs << " dim=" << dim << " ne=" << ne << " nr=" << nr;
        BatchCreator batcher(kb.getPath(), batchsize, nthreads);

        Transe tr(epochs, ne, nr, dim);
        BOOST_LOG_TRIVIAL(info) << "Setting up TranSE ...";
        tr.setup(nthreads);
        BOOST_LOG_TRIVIAL(info) << "Launching the training of TranSE ...";
        tr.train(batcher);
        BOOST_LOG_TRIVIAL(info) << "Done.";

    } else {
        BOOST_LOG_TRIVIAL(error) << "Task " << op << " not recognized";
        return;
    }
}

void mineFrequentPatterns(string kbdir, int minLen, int maxLen, long minSupport) {
    BOOST_LOG_TRIVIAL(info) << "Mining frequent graphs";
    Miner miner(kbdir, 1000);
    miner.mine();
    miner.getFrequentPatterns(minLen, maxLen, minSupport);
}

int main(int argc, const char** argv) {

    //Check some constraints
    if (!checkMachineConstraints()) {
        return EXIT_FAILURE;
    }

    //Init params
    po::variables_map vm;
    if (!initParams(argc, argv, vm)) {
        return EXIT_FAILURE;
    }

    //Init logging system
    logging::trivial::severity_level level =
        vm.count("logLevel") ?
        vm["logLevel"].as<logging::trivial::severity_level>() :
        logging::trivial::warning;
    string filelog = vm["logfile"].as<string>();
    auto logptr = initLogging(level, true, filelog != "", filelog);

    //Print params
    if (level <= logging::trivial::debug) {
        string params = "";
        for(int i = 0; i < argc; ++i) {
            params += string(argv[i]) + " ";
        }
        BOOST_LOG_TRIVIAL(debug) << "PARAMS: " << params;
    }

    //Init the knowledge base
    string cmd = string(argv[1]);
    string kbDir = vm["input"].as<string>();

    if (cmd == "query") {
        KBConfig config;
        std::vector<string> locUpdates;
        //string updatesDir = vm["updates"].as<string>();
        //if (updatesDir.length() > 0)
        //    locUpdates.push_back(updatesDir);
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
    } else if (cmd == "query_native") {
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
    } else if (cmd == "lookup") {
        KBConfig config;
        KB kb(kbDir.c_str(), true, false, true, config);
        lookup(kb.getDictMgmt(), vm);
    } else if (cmd == "testkb") {
        testkb(kbDir, vm);
    } else if (cmd == "testcq") {
        string inputFile = kbDir + string("/p0/raw");
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
        bool inputCompressed = false;
        if (!vm["comprinput"].empty()) {
            inputDir = vm["comprinput"].as<string>();
            dictDir = vm["comprdict"].as<string>();
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
        p.logPtr = logptr;
        p.timeoutStats = vm["timeoutStats"].as<int>();
        p.remoteLocation = vm["remoteLoc"].as<string>();
        p.limitSpace = vm["limitSpace"].as<long>();
        p.graphTransformation = vm["gf"].as<string>();
        p.storeDicts = vm["storedicts"].as<bool>();
        p.relsOwnIDs = vm["relsOwnIDs"].as<bool>();

        loader.load(p);

    } else if (cmd == "info") {
        KBConfig config;
        KB kb(kbDir.c_str(), true, false, true, config);
        printInfo(kb);
    } else if (cmd == "add") {
        string updatedir = vm["update"].as<string>();
        Updater up;
        up.creatediffupdate(DiffIndex::TypeUpdate::ADDITION, kbDir, updatedir);
    } else if (cmd == "rm") {
        string updatedir = vm["update"].as<string>();
        Updater up;
        up.creatediffupdate(DiffIndex::TypeUpdate::DELETE, kbDir, updatedir);
    } else if (cmd == "analytics") {
        KBConfig config;
        KB kb(kbDir.c_str(), true, false, true, config);
        launchAnalytics(kb, vm["op"].as<string>(), vm["oparg1"].as<string>(),
                vm["oparg2"].as<string>());
    } else if (cmd == "mine")  {
        long minSupport = vm["minSupport"].as<long>();
        int minLen = vm["minLen"].as<int>();
        int maxLen = vm["maxLen"].as<int>();
        mineFrequentPatterns(kbDir, minLen, maxLen, minSupport);
    } else if (cmd == "dump") {
        KBConfig config;
        KB kb(kbDir.c_str(), true, false, true, config);
        dump(&kb, vm["output"].as<string>());
    } else if (cmd == "server") {
        KBConfig config;
        KB kb(kbDir.c_str(), true, false, true, config);
        startServer(kb, vm["port"].as<int>());
    } else if (cmd == "ml") {
        KBConfig config;
        KB kb(kbDir.c_str(), true, false, true, config);
        launchML(kb, vm["algo"].as<string>(), vm["args"].as<string>());
    }

    //Print other stats
    BOOST_LOG_TRIVIAL(info) << "Max memory used: " << Utils::get_max_mem() << " MB";
    return EXIT_SUCCESS;
}

void execNativeQuery(po::variables_map &vm, Querier *q, KB &kb, bool silent) {
    long nElements = 0;
    char bufferTerm[MAX_TERM_SIZE];
    DictMgmt *dict = kb.getDictMgmt();
    string queryFileName = vm["query"].as<string>();
    TridentLayer db(kb);

    //Parse the query
    QueryDict queryDict(db.getNextId());
    QueryGraph queryGraph;
    bool parsingOk;

    std::fstream inFile;
    inFile.open(queryFileName);//open the input file
    std::stringstream strStream;
    strStream << inFile.rdbuf();//read the file
    SPARQLLexer lexer(strStream.str());
    SPARQLParser parser(lexer);

    timens::system_clock::time_point start = timens::system_clock::now();

    parseQuery(parsingOk, parser, queryGraph, queryDict, db);
    if (!parsingOk) {
        boost::chrono::duration<double> duration = boost::chrono::system_clock::now() - start;
        BOOST_LOG_TRIVIAL(info) << "Runtime queryopti: 0ms.";
        BOOST_LOG_TRIVIAL(info) << "Runtime queryexec: 0ms.";
        BOOST_LOG_TRIVIAL(info) << "Runtime totalexec: " << duration.count() * 1000 << "ms.";
        return;
    }

    std::unique_ptr<Query> query  = createQueryFromRF3XQueryGraph(parser,
            queryGraph);
    TridentQueryPlan plan(q);
    plan.create(*query.get(), SIMPLE);
    boost::chrono::duration<double> durationO = boost::chrono::system_clock::now() - start;

    //Output plan
#ifdef DEBUG
    BOOST_LOG_TRIVIAL(debug) << "Translated plan:";
    plan.print();
#endif

    {
        q->resetCounters();
        timens::system_clock::time_point startQ = timens::system_clock::now();
        TupleIterator *root = plan.getIterator();
        //Execute the query
        const uint8_t nvars = (uint8_t) root->getTupleSize();
        while (root->hasNext()) {
            root->next();
            if (! silent) {
                for (uint8_t i = 0; i < nvars; ++i) {
                    dict->getText(root->getElementAt(i), bufferTerm);
                    std::cout << bufferTerm << ' ';
                }
                std::cout << '\n';
            }
            nElements++;
        }
        boost::chrono::duration<double> sec = boost::chrono::system_clock::now()
            - startQ;
        boost::chrono::duration<double> secT = boost::chrono::system_clock::now()
            - start;
        //Print stats
        BOOST_LOG_TRIVIAL(info) << "Runtime queryopti: " << durationO.count() * 1000 << "ms.";
        BOOST_LOG_TRIVIAL(info) << "Runtime queryexec: " << sec.count() * 1000 << "ms.";
        BOOST_LOG_TRIVIAL(info) << "Runtime totalexec: " << secT.count() * 1000 << "ms.";
        BOOST_LOG_TRIVIAL(info) << "# rows = " << nElements;
        plan.releaseIterator(root);
    }
    //Print stats dictionary
}
