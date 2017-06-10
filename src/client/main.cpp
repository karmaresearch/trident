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

#include <trident/sparql/query.h>
#include <trident/sparql/plan.h>

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

#include <iostream>
#include <cstdlib>
#include <sstream>
#include <fstream>

using namespace std;
namespace timens = boost::chrono;
namespace logging = boost::log;
namespace fs = boost::filesystem;
namespace po = boost::program_options;

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

void printHelp(const char *programName, string section,
        po::options_description &desc,
        std::map<string,po::options_description> &sections) {

    if (section != "") {
        if (sections.count(section)) {
            cout << sections[section] << endl;
        } else {
            cout << "Command " << section << " not recognized" << endl;
        }
    } else {
        cout << "Usage: " << programName << " <command> [options]" << endl << endl;
        cout << "Possible commands:" << endl;
        cout << "help\t\t produce help message." << endl;
        cout << "query\t\t query the KB using the RDF3X query optimizer and executor." << endl;
        cout << "query_native\t\t query the KB using the experimental engine." << endl;
        cout << "load\t\t load the KB." << endl;
        cout << "add\t\t add triples to an existing KB." << endl;
        cout << "rm\t\t rm triples to an existing KB." << endl;
        cout << "lookup\t\t lookup for values in the dictionary." << endl;
        cout << "analytics\t\t perform analytical operations on the graph." << endl;
        cout << "info\t\tprint some information about the KB." << endl << endl;
        cout << "dump\t\tdump the graph on files." << endl << endl;
        cout << "server\t\tstart a server." << endl << endl;
        cout << desc << endl;
    }
}

inline void printErrorMsg(const char *msg) {
    cout << endl << "*** ERROR: " << msg << "***" << endl << endl
        << "Please type the subcommand \"help\" for instructions (e.g. trident help)."
        << endl;
}

bool checkParams(po::variables_map &vm, int argc, const char** argv,
        po::options_description &desc,
        std::map<string,po::options_description> &sections) {

    string cmd;
    if (argc < 2) {
        printErrorMsg("Command is missing!");
        return false;
    } else {
        cmd = argv[1];
    }

    if (cmd != "help" && cmd != "query" && cmd != "lookup" && cmd != "load"
            && cmd != "testkb" && cmd != "testcq" && cmd != "testti"
            && cmd != "query_native"
            && cmd != "info"
            && cmd != "add"
            && cmd != "rm"
            && cmd != "analytics"
            && cmd != "mine"
            && cmd != "server"
            && cmd != "dump") {
        printErrorMsg(
                (string("The command \"") + cmd + string("\" is unknown.")).c_str());
        return false;
    }

    if (cmd == "help") {
        string section = "";
        if (argc > 2) {
            section = argv[2];
        }
        printHelp(argv[0], section, desc, sections);
        return false;
    } else {
        /*** Check common parameters ***/
        if (!vm.count("input")) {
            printErrorMsg("The parameter -i (the knowledge base) is not set.");
            return false;
        }

        //Check if the directory exists
        string kbDir = vm["input"].as<string>();
        if ((cmd == "query" || cmd == "lookup") && !fs::is_directory(kbDir)) {
            printErrorMsg(
                    (string("The directory ") + kbDir
                     + string(" does not exist.")).c_str());
            return false;
        }

        //Check if the directory is not empty
        if (cmd == "query" || cmd == "lookup" || cmd == "query_native") {
            if (fs::is_empty(kbDir)) {
                printErrorMsg(
                        (string("The directory ") + kbDir + string(" is empty.")).c_str());
                return false;
            }
        }
        /*** Check specific parameters ***/
        if (cmd == "query") {
            string queryFile = vm["query"].as<string>();
            if (queryFile != "" && !fs::exists(queryFile)) {
                printErrorMsg(
                        (string("The file ") + queryFile
                         + string(" doesn't exist.")).c_str());
                return false;
            }
        } else if (cmd == "lookup") {
            if (!vm.count("text") && !vm.count("number")) {
                printErrorMsg(
                        "Neither the -t nor -n parameters are set. At least one of them must be set.");
                return false;
            }
            if (vm.count("text") && vm.count("number")) {
                printErrorMsg(
                        "Both the -t and -n parameters are set, and this is ambiguous. Please choose either one or the other.");
                return false;
            }
        } else if (cmd == "load") {
            if (vm["tripleFiles"].empty() && vm["comprinput"].empty()) {
                printErrorMsg(
                        "The parameter -f (path to the triple files) or --comprinput are not set.");
                return false;
            }

            if (!vm.count("comprinput")) {
                string tripleDir = vm["tripleFiles"].as<string>();
                if (!fs::exists(tripleDir)) {
                    printErrorMsg(
                            (string("The path ") + tripleDir
                             + string(" does not exist.")).c_str());
                    return false;
                }
            }

            string dictMethod = vm["dictMethod"].as<string>();
            if (dictMethod != DICT_HEURISTICS && dictMethod != DICT_HASH && dictMethod != DICT_SMART) {
                printErrorMsg("The parameter dictMethod (-d) can be either 'hash', 'heuristics', or 'smart'");
                return false;
            }

            string sampleMethod = vm["popMethod"].as<string>();
            if (sampleMethod != "sample" && sampleMethod != "hash") {
                printErrorMsg(
                        "The method to identify the popular terms can only be either 'sample' or 'hash'");
                return false;
            }

            if (!vm.count("popArg")) {
                printErrorMsg(
                        "The argument to identify the popular terms is not set (-popArg)");
                return false;
            }

            if (vm["maxThreads"].as<int>() < 1) {
                printErrorMsg(
                        "The number of threads to use must be at least 1");
                return false;
            }

            if (vm["readThreads"].as<int>() < 1) {
                printErrorMsg(
                        "The number of threads to use to read the input must be at least 1");
                return false;
            }

            if (vm["ndicts"].as<int>() < 1) {
                printErrorMsg(
                        "The number of dictionary partitions must be at least 1");
                return false;
            }
            if (!vm["gf"].empty()) {
                string v = vm["gf"].as<string>();
                if (v != "" && v != "unlabeled" && v != "undirected") {
                    printErrorMsg(
                            "The parameter 'gf' accepts only 'unlabeled' or 'undirected'");
                    return false;
                }

            }
        } else if (cmd == "add" || cmd == "rm") {
            if (!vm.count("update")) {
                printErrorMsg(
                        "The path for the update is not set");
                return false;
            }
            string updatedir = vm["update"].as<string>();
            if (!fs::exists(updatedir)) {
                string msgerror = string("The path specified (") + updatedir + string(") does not exist");
                printErrorMsg(msgerror.c_str());
                return false;
            }
        } else if (cmd == "analytics") {
            if (!vm.count("op")) {
                printErrorMsg(
                        "Analytics requires the parameter 'op' to be defined");
                return false;
            }
        }
    }

    return true;
}

bool initParams(int argc, const char** argv, po::variables_map &vm) {
    std::map<string, po::options_description> sections;

    po::options_description query_options("Options for <query>");
    query_options.add_options()("query,q", po::value<string>()->default_value(""),
            "The path of the file with a SPARQL query. If not set then the query is read from STDIN.");
    query_options.add_options()("repeatQuery,r",
            po::value<int>()->default_value(0),
            "Repeat the query <arg> times. If the argument is not specified, then the query will not be repeated.");
    query_options.add_options()("explain,e",
            po::value<bool>()->default_value(false),
            "Explain the query instead of executing it. Default value is false.");
    query_options.add_options()("decodeoutput",
            po::value<bool>()->default_value(true),
            "Retrieve the original values of the results of query. Default is true");
    query_options.add_options()("disbifsampl",
            po::value<bool>()->default_value(false),
            "Disable bifocal sampling (accurate but expensive). Default is false");

    po::options_description load_options("Options for <load>");
    load_options.add_options()("inputformat", po::value<string>()->default_value("rdf"),
            "Input format. Can be either 'rdf' or 'snap'. Default is 'rdf'.");
    load_options.add_options()("comprinput", po::value<string>(),
            "Path to a file that contains a list of compressed triples.");
    load_options.add_options()("comprdict", po::value<string>()->default_value(""),
            "Path to a file that contains the dictionary for the compressed triples.");

    load_options.add_options()("tripleFiles,f", po::value<string>(),
            "Path to the files that contain the compressed triples. This parameter is REQUIRED.");
    load_options.add_options()("tmpdir", po::value<string>(),
            "Path to store the temporary files used during loading. Default is the output directory.");
    load_options.add_options()("dictMethod,d", po::value<string>()->default_value(DICT_HEURISTICS),
            "Method to perform dictionary encoding. It can b: \"hash\", \"heuristics\", or \"smart\". Default is heuristics.");
    load_options.add_options()("popMethod",
            po::value<string>()->default_value("hash"),
            "Method to use to identify the popular terms. Can be either 'sample' or 'hash'. Default is 'hash'");
    load_options.add_options()("maxThreads",
            po::value<int>()->default_value(8),
            "Sets the maximum number of threads to use during the compression. Default is '8'");
    load_options.add_options()("readThreads",
            po::value<int>()->default_value(2),
            "Sets the number of concurrent threads that reads the raw input. Default is '2'");
    load_options.add_options()("ndicts",
            po::value<int>()->default_value(1),
            "Sets the number dictionary partitions. Default is '1'");
    load_options.add_options()("skipTables", po::value<bool>()->default_value(false),
            "Skip storage of some tables. Default is 'false'");
    load_options.add_options()("thresholdSkipTable",
            po::value<int>()->default_value(20),
            "If dynamic strategy is enabled, this param. defines the size above which a table is not stored. Default is '10'");
    load_options.add_options()("timeoutStats",
            po::value<int>()->default_value(-1),
            "If set greater than 0, it starts a new thread to log some resource statistics every n seconds. Works only under Linux. Default is '-1' (disabled)");
    load_options.add_options()("onlyCompress",
            po::value<bool>()->default_value(false),
            "Only compresses the data. Works only with RDF inputs. Default is DISABLED");
    load_options.add_options()("sample",
            po::value<bool>()->default_value(true),
            "Store a little sample of the data, to improve query optimization. Default is ENABLED");
    load_options.add_options()("sampleRate",
            po::value<double>()->default_value(0.01),
            "If the sampling is enabled, this parameter sets the sample rate. Default is 0.01 (i.e 1%)");
    load_options.add_options()("storeplainlist",
            po::value<bool>()->default_value(false),
            "Next to the indices, stores also a dump of all the input in a single file. This improves scan queries. Default is DISABLED");
    load_options.add_options()("storedicts",
            po::value<bool>()->default_value(true),
            "Should I also store the dictionaries? (Maybe I don't need it, since I only want to do graph analytics. Default is ENABLED");
    load_options.add_options()("nindices",
            po::value<int>()->default_value(6),
            "Set the number of indices to use. Can be 1,3,4,6. Default is '6'");
    load_options.add_options()("incrindices", po::value<bool>()->default_value(false),
            "Create the indices a few at the time (saves space). Default is 'false'");
    load_options.add_options()("aggrIndices", po::value<bool>()->default_value(false),
            "Use aggredated indices. Default is 'false'");
    load_options.add_options()("enableFixedStrat", po::value<bool>()->default_value(false),
            "Should we store the tables with a fixed layout?. Default is 'false'");
    string textStrat =  "Fixed strategy to use. Only for advanced users. For for a column-layout " + to_string(StorageStrat::FIXEDSTRAT5) + " for row-layout " + to_string(StorageStrat::FIXEDSTRAT6) + " for a cluster-layout " + to_string(StorageStrat::FIXEDSTRAT7);
    load_options.add_options()("fixedStrat",
            po::value<int>()->default_value(StorageStrat::FIXEDSTRAT5), textStrat.c_str());

    load_options.add_options()("popArg", po::value<int>()->default_value(128),
            "Argument for the method to identify the popular terms. If the method is sample, then it represents the sample percentage (x/10000)."
            "If it it hash, then it indicates the number of popular terms."
            "Default value is 128.");

    load_options.add_options()("remoteLoc", po::value<string>()->default_value(""),
            "");
    load_options.add_options()("limitSpace", po::value<long>()->default_value(0),
            "");
    load_options.add_options()("gf",
            po::value<string>()->default_value(""),
            "Possible graph transformations. 'unlabeled' removes the edge labels (but keeps it directed), 'undirected' makes the graph undirected and without edge labels");
    load_options.add_options()("relsOwnIDs",
            po::value<bool>()->default_value(false),
            "Should I give independent IDs to the terms that appear as predicates? (Useful for ML learning models). Default is DISABLED");

    po::options_description lookup_options("Options for <lookup>");
    lookup_options.add_options()("text,t", po::value<string>(),
            "Textual term to search")("number,n", po::value<long>(),
                "Numeric term to search");

    po::options_description test_options("Options for <tests> (only advanced usage)");
    test_options.add_options()("testqueryfile",
            po::value<string>()->default_value(""),
            "Path file to store/load test queries");
    test_options.add_options()("testperms",
            po::value<string>()->default_value("0;1;2;3;4;5"),
            "Permutations to test.");
    test_options.add_options()("testsystem",
            po::value<int>()->default_value(0),
            "Test system. 0=Trident 1=RDF3X");

    po::options_description update_options("Options for <add> or <rm>");
    update_options.add_options()("update",
            po::value<string>()->default_value(""),
            "Path to the file/dir that contains the triples to update");

    po::options_description server_options("Options for <server>");
    server_options.add_options()("port",
            po::value<int>()->default_value(8080),
            "Port to listen to.");


    po::options_description mine_options("Options for <mine>");
    mine_options.add_options()("minSupport",
            po::value<long>()->default_value(1000),
            "Min support for the patterns to mine.");
    mine_options.add_options()("minLen",
            po::value<int>()->default_value(2),
            "Min lengths of the patterns.");
    mine_options.add_options()("maxLen",
            po::value<int>()->default_value(10),
            "Max lengths of the patterns.");

    po::options_description ana_options("Options for <analytics>");
    ana_options.add_options()("op",
            po::value<string>()->default_value(""),
            "The analytical operation to perform This parameter is REQUIRED.");
    ana_options.add_options()("oparg1",
            po::value<string>()->default_value(""),
            "First argument for the analytical operation. Normally it is the path to output the results of the computation.");
    string helpstring = "List of values to give to additional parameters. The list of details depends on the action. Parameters should be split by ';', e.g., src=0;dst=10 for bfs. Notice that the character ';' should be escaped ('\\;').\n" + AnalyticsTasks::getInstance().getTaskDetails();
    ana_options.add_options()("oparg2",
            po::value<string>()->default_value(""),
            helpstring.c_str());

    po::options_description dump_options("Options for <dump>");
    dump_options.add_options()("output",
            po::value<string>()->default_value(""),
            "Output directory to store the graph");

    po::options_description cmdline_options("Generic options");
    cmdline_options.add(query_options).add(lookup_options).add(load_options).add(test_options).add(update_options).add(ana_options).add(dump_options).add(mine_options).add(server_options);

    sections.insert(make_pair("query",query_options));
    sections.insert(make_pair("lookup",lookup_options));
    sections.insert(make_pair("load",load_options));
    sections.insert(make_pair("test",test_options));
    sections.insert(make_pair("update",update_options));
    sections.insert(make_pair("analytics",ana_options));
    sections.insert(make_pair("dump",dump_options));
    sections.insert(make_pair("mine",mine_options));
    sections.insert(make_pair("server",server_options));

    cmdline_options.add_options()("input,i", po::value<string>(),
            "The path of the KB directory. This parameter is REQUIRED.")(
                "logLevel,l", po::value<logging::trivial::severity_level>(),
                "Set the log level (accepted values: trace, debug, info, warning, error, fatal). Default is warning.");
    cmdline_options.add_options()("logfile",
            po::value<string>()->default_value(""),
            "Set if you want to store the logs in a file");

    po::store(
            po::command_line_parser(argc, argv).options(cmdline_options).run(),
            vm);



    return checkParams(vm, argc, argv, cmdline_options, sections);
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

void mineFrequentPatterns(string kbdir, int minLen, int maxLen, long minSupport) {
    BOOST_LOG_TRIVIAL(info) << "Mining frequent graphs";
    Miner miner(kbdir, 1000);
    miner.mine();
    miner.getFrequentPatterns(minLen, maxLen, minSupport);
}

bool checkMachineConstraints() {
    if (sizeof(long) != 8) {
        BOOST_LOG_TRIVIAL(error) << "Trident expects a 'long' to be 8 bytes";
        return false;
    }
    if (sizeof(int) != 4) {
        BOOST_LOG_TRIVIAL(error) << "Trident expects a 'int' to be 4 bytes";
        return false;
    }
    if (sizeof(short) != 2) {
        BOOST_LOG_TRIVIAL(error) << "Trident expects a 'short' to be 2 bytes";
        return false;
    }
    int n = 1;
    // big endian if true
    if(*(char *)&n != 1) {
        BOOST_LOG_TRIVIAL(error) << "Some features of Trident rely on little endianness. Change machine ...sorry";
        return false;
    }
    return true;
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
