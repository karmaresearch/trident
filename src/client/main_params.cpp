#include <trident/kb/kbconfig.h>
#include <trident/loader.h>
#include <trident/binarytables/storagestrat.h>

#include <kognac/progargs.h>

#ifdef ANALYTICS
#include <snap/tasks.h>
#endif

#ifdef ML
#include <trident/ml/learner.h>
#endif

#include <iostream>

using namespace std;

void printHelp(const char *programName, string section,
        ProgramArgs &vm,
        std::map<string, ProgramArgs::GroupArgs*> &sections) {

    if (section != "") {
        if (sections.count(section)) {
            cout << sections["general"]->tostring() << endl;
            cout << sections[section]->tostring() << endl;
        } else {
            cout << "Command " << section << " not recognized" << endl;
        }
    } else {
        cout << "Usage: " << programName << " <command> [options]" << endl << endl;
        cout << "Possible commands:" << endl;
        cout << "help\t\t\t produce help message." << endl;
        cout << "query\t\t\t query the KB using the RDF3X query optimizer and executor." << endl;
        cout << "query_native\t\t query the KB using the experimental engine." << endl;
        cout << "load\t\t\t load the KB." << endl;
        cout << "add\t\t\t add triples to an existing KB." << endl;
        cout << "rm\t\t\t rm triples to an existing KB." << endl;
        cout << "lookup\t\t\t lookup for values in the dictionary." << endl;
        cout << "info\t\t\t print some information about the KB." << endl;
        cout << "dump\t\t\t dump the graph on files." << endl;

#ifdef ANALYTICS
        cout << "analytics\t\t perform analytical operations on the graph." << endl;
#endif

#ifdef ML
        cout << "learn\t\t\t launch an algorithm to calculate KG embeddings." << endl;
        cout << "predict\t\t\t launch an algorithm to use KG embeddings for link prediction." << endl;
        cout << "subcreate\t\t create embeddings of subgraphs." << endl;
        cout << "subeval\t\t\t use embeddings of subgraphs for link prediction." << endl;
        cout << "subanswers\t\t\t use embeddings of subgraphs for finding all answers of the query." << endl;
#endif

        cout << "server\t\t\t start a server for SPARQL queries." << endl << endl;
        cout << "Type 'help command' to have an overview of the parameters for each command" << endl;
        //cout << vm.tostring() << endl;
    }
}

inline void printErrorMsg(const char *msg) {
    cout << endl << "*** ERROR: " << msg << "***" << endl << endl
        << "Please type the subcommand \"help\" for instructions (e.g. trident help)."
        << endl;
}

bool checkParams(ProgramArgs &vm, int argc, const char** argv,
        ProgramArgs &desc,
        std::map<string, ProgramArgs::GroupArgs*> &sections) {

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
            && cmd != "merge"
#ifdef ANALYTICS
            && cmd != "analytics"
#endif
            && cmd != "mine"
            && cmd != "server"
            && cmd != "dump"
            && cmd != "learn"
            && cmd != "predict"
            && cmd != "subcreate"
            && cmd != "subanswers"
            && cmd != "subeval") {
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
        if ((cmd == "query" || cmd == "lookup") && !Utils::isDirectory(kbDir)) {
            printErrorMsg(
                    (string("The directory ") + kbDir
                     + string(" does not exist.")).c_str());
            return false;
        }

        //Check if the directory is not empty
        if (cmd == "query" || cmd == "lookup" || cmd == "query_native") {
            if (Utils::isEmpty(kbDir)) {
                printErrorMsg(
                        (string("The directory ") + kbDir + string(" is empty.")).c_str());
                return false;
            }
        }
        /*** Check specific parameters ***/
        if (cmd == "query") {
            string queryFile = vm["query"].as<string>();
            if (queryFile != "" && !Utils::exists(queryFile)) {
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
                if (!Utils::exists(tripleDir)) {
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
            if (!Utils::exists(updatedir)) {
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
        } else if (cmd == "learn" || cmd == "predict") {
            if (!vm.count("algo")) {
                printErrorMsg(
                        "ML requires the parameter 'algo' to be defined");
                return false;
            }
        }
    }
    return true;
}

bool checkMachineConstraints() {
    if (sizeof(int) != 4) {
        LOG(ERRORL) << "Trident expects a 'int' to be 4 bytes";
        return false;
    }
    if (sizeof(short) != 2) {
        LOG(ERRORL) << "Trident expects a 'short' to be 2 bytes";
        return false;
    }
    int n = 1;
    // big endian if true
    if(*(char *)&n != 1) {
        LOG(ERRORL) << "Some features of Trident rely on little endianness. Change machine ...sorry";
        return false;
    }
    return true;
}

bool initParams(int argc, const char** argv, ProgramArgs &vm) {
    std::map<string, ProgramArgs::GroupArgs*> sections;

    /***** QUERY *****/
    ProgramArgs::GroupArgs& query_options = *vm.newGroup("Options for <query>");
    query_options.add<string>("q","query", "",
            "The path of the file with a SPARQL query. If not set then the query is read from STDIN.", false);
    query_options.add<int>("r","repeatQuery",
            0, "Repeat the query <arg> times. If the argument is not specified, then the query will not be repeated.", false);
    query_options.add<bool>("e","explain", false,
            "Explain the query instead of executing it. Default value is false.", false);
    query_options.add<bool>("", "decodeoutput", true,
            "Retrieve the original values of the results of query. Default is true", false);
    query_options.add<bool>("", "disbifsampl", false,
            "Disable bifocal sampling (accurate but expensive). Default is false", false);

    /***** LOAD *****/
    ParamsLoad p;

    int nHardwareThreads = std::thread::hardware_concurrency();
    if (nHardwareThreads == 0) {
        nHardwareThreads = 8;
    }
    int defaultConcurrentReaders = 1;

    if (nHardwareThreads < defaultConcurrentReaders) {
        defaultConcurrentReaders = 1;
    }

    ProgramArgs::GroupArgs& load_options = *vm.newGroup("Options for <load>");
    load_options.add<string>("","inputformat", "rdf", "Input format. Can be either 'rdf' or 'snap'. Default is 'rdf'.", false);
    load_options.add<string>("","comprinput", "", "Path to a file that contains a list of compressed triples.", false);
    load_options.add<string>("","comprdict", "", "Path to a file that contains the dictionary for the compressed triples.", false);
    load_options.add<string>("","comprdict_rel", "", "Path to a file that contains the dictionary for the relations used in compressed triples (used only if relsOwnIDs is set to true).", false);

    load_options.add<string>("f","tripleFiles", "", "Path to the files that contain the compressed triples. This parameter is REQUIRED.", false);
    load_options.add<string>("","tmpdir", "", "Path to store the temporary files used during loading. Default is the output directory.", false);
    load_options.add<string>("d","dictMethod", p.dictMethod, "Method to perform dictionary encoding. It can b: \"hash\", \"heuristics\", or \"smart\". Default is heuristics.", false);
    load_options.add<string>("","popMethod", "hash", "Method to use to identify the popular terms. Can be either 'sample' or 'hash'. Default is 'hash'", false);
    load_options.add<int>("","maxThreads", nHardwareThreads,"Sets the maximum number of threads to use during the compression. Default is the number of hardware threads", false);
    load_options.add<int>("","readThreads", p.maxReadingThreads, "Sets the number of concurrent threads that reads the raw input. Default is '1'", false);
    load_options.add<int>("","ndicts", p.dictionaries, "Sets the number dictionary partitions. Default is '1'", false);
    load_options.add<bool>("","skipTables", p.canSkipTables, "Skip storage of some tables. Default is 'false'", false);
    load_options.add<int>("","thresholdSkipTable", p.thresholdSkipTable, "If dynamic strategy is enabled, this param. defines the size above which a table is not stored. Default is '10'", false);
    load_options.add<int>("","timeoutStats", p.timeoutStats, "If set greater than 0, it starts a new thread to log some resource statistics every n seconds. Works only under Linux. Default is '-1' (disabled)", false);
    load_options.add<bool>("","onlyCompress", p.onlyCompress, "Only compresses the data. Works only with RDF inputs. Default is DISABLED", false);
    load_options.add<bool>("","sample", p.sample, "Store a little sample of the data, to improve query optimization. Default is ENABLED", false);
    load_options.add<double>("","sampleRate", p.sampleRate, "If the sampling is enabled, this parameter sets the sample rate. Default is 0.01 (i.e 1%)", false);
    load_options.add<bool>("","storeplainlist", p.storePlainList, "Next to the indices, stores also a dump of all the input in a single file. This improves scan queries. Default is DISABLED", false);
    load_options.add<bool>("","storedicts", p.storeDicts, "Should I also store the dictionaries? (Maybe I don't need it, since I only want to do graph analytics. Default is ENABLED", false);
    load_options.add<int>("","nindices", p.nindices, "Set the number of indices to use. Can be 1,3,4,6. Default is '6'", false);
    load_options.add<bool>("","incrindices", p.createIndicesInBlocks, "Create the indices a few at the time (saves space). Default is 'false'", false);
    load_options.add<bool>("","aggrIndices", p.aggrIndices, "Use aggredated indices. Default is 'false'", false);
    load_options.add<bool>("","enableFixedStrat", p.enableFixedStrat, "Should we store the tables with a fixed layout?. Default is 'false'", false);
    string textStrat = "Fixed strategy to use. Only for advanced users. For for a column-layout " + to_string(FIXEDSTRAT5) + " for row-layout " + to_string(FIXEDSTRAT6) + " for a cluster-layout " + to_string(FIXEDSTRAT7);
    load_options.add<int>("","fixedStrat", p.fixedStrat, textStrat.c_str(), false);
    load_options.add<int>("","popArg", p.sampleArg,
            "Argument for the method to identify the popular terms. If the method is sample, then it represents the sample percentage (x/10000)."
            "If it it hash, then it indicates the number of popular terms."
            "Default value is 128.", false);
    load_options.add<string>("","remoteLoc", p.remoteLocation, "", false);
    load_options.add<int64_t>("","limitSpace", p.limitSpace, "", false);
    load_options.add<string>("","gf", p.graphTransformation, "Possible graph transformations. 'unlabeled' removes the edge labels (but keeps it directed), 'undirected' makes the graph undirected and without edge labels", false);
    load_options.add<bool>("","relsOwnIDs", p.relsOwnIDs, "Should I give independent IDs to the terms that appear as predicates? (Useful for ML learning models). Default is DISABLED", false);
    load_options.add<bool>("","flatTree", p.flatTree, "Create a flat representation of the nodes' tree. This parameter is forced to tree if the graph is unlabeled. Default is DISABLED", false);

    /***** LOOKUP *****/
    ProgramArgs::GroupArgs& lookup_options = *vm.newGroup("Options for <lookup>");
    lookup_options.add<string>("t","text", "", "Textual term to search", false);
    lookup_options.add<int64_t>("n","number", 0, "Numeric term to search", false);

    /***** TEST *****/
    ProgramArgs::GroupArgs& test_options = *vm.newGroup("Options for <tests> (only advanced usage)");
    test_options.add<string>("", "testqueryfile", "", "Path file to store/load test queries", false);
    test_options.add<string>("", "testperms", "0;1;2;3;4;5", "Permutations to test", false);
    test_options.add<int>("", "testsystem", 0, "Test system. 0=Trident 1=RDF3X", false);

    /***** UPDATES *****/
    ProgramArgs::GroupArgs& update_options = *vm.newGroup("Options for <add> or <rm>");
    update_options.add<string>("", "update", "", "Path to the file/dir that contains the triples to update", false);

    /***** SERVER *****/
    ProgramArgs::GroupArgs& server_options = *vm.newGroup("Options for <server>");
    server_options.add<int>("", "port", 8080, "Port to listen to", false);
    server_options.add<int>("", "webthreads", 1, "N. of threads for the webserver", false);

    /***** LEARN/PREDICT *****/
#ifdef ML
    ProgramArgs::GroupArgs& ml_options = *vm.newGroup("Options for <learn> or <predict>");
    ml_options.add<string>("", "algo", "", "The task to perform", false);
    ml_options.add<string>("","args_learn", LearnParams().changeable_tostring().c_str(), "List of arguments for 'learn', separated by ;", false);
    ml_options.add<string>("","args_predict", PredictParams().changeable_tostring().c_str(), "List of arguments for 'predict', separated by ;", false);
#endif

    /***** MINE *****/
    ProgramArgs::GroupArgs& mine_options = *vm.newGroup("Options for <mine>");
    mine_options.add<int64_t>("", "minSupport", 1000, "Min support for the patterns to mine", false);
    mine_options.add<int>("", "minLen", 2, "Min lengths of the patterns", false);
    mine_options.add<int>("", "maxLen", 10, "Max lengths of the patterns", false);

#ifdef ANALYTICS
    /***** ANALYTICS *****/
    ProgramArgs::GroupArgs& ana_options = *vm.newGroup("Options for <analytics>");
    ana_options.add<string>("", "op", "", "The analytical operation to perform", false);
    ana_options.add<string>("", "oparg1", "", "First argument for the analytical operation. Normally it is the path to output the results of the computation", false);
    string helpstring = "List of values to give to additional parameters. The list of details depends on the action. Parameters should be split by ';', e.g., src=0;dst=10 for bfs. Notice that the character ';' should be escaped ('\\;').\n" + AnalyticsTasks::getInstance().getTaskDetails();
    ana_options.add<string>("", "oparg2", "", helpstring.c_str(), false);
#endif

    /***** DUMP *****/
    ProgramArgs::GroupArgs& dump_options = *vm.newGroup("Options for <dump>");
    dump_options.add<string>("", "output", "", "Output directory to store the graph", false);

#ifdef ML
    /***** SUBGRAPHS *****/
    ProgramArgs::GroupArgs& subeval_options = *vm.newGroup("Options for <subcreate> or <subeval> or <subanswers>");
    subeval_options.add<string>("", "embAlgo", "",
            "The algorithm used to create the embeddings of the entities and relations (e.g., transe).", false);
    subeval_options.add<string>("", "subAlgo", "avg",
            "The algorithm to use for creating the subgraph embeddings. Default is 'avg'. (can be 'var')", false);
    subeval_options.add<string>("", "embDir", "",
            "The directory that contains the embeddings of entities and relations.", false);
    subeval_options.add<string>("", "logFile", "",
            "The path of the file to write logs of the test.", false);
    subeval_options.add<string>("", "subFile", "",
            "The path of the file that contains embeddings of the subgraphs.", false);
    subeval_options.add<string>("", "nameTest", "",
            "The path (or name) of the dataset to use to test the performance of the subgraph embeddings.", false);
    subeval_options.add<string>("", "kFile", "",
            "The path of the file that contains suggested K values of top subgraphs for each relation.", false);
    subeval_options.add<string>("", "formatTest", "native",
            "The format used to store the test data. For now it can be 'python' or 'native'. Default is 'native'. If it is native then sgfile can be either 'valid' or 'test'. Otherwise, it is a path of a file.", false);
    subeval_options.add<long>("", "subgraphThreshold", 10,
            "Threshold to consider subgraphs while evaluating. -1 for choosing dynamic K. -2 for choosing per relation K", false);
    subeval_options.add<long>("", "minSubgraphSize", 10,
            "Threshold on number of entities in a subgraph while creating", false);
    subeval_options.add<double>("", "varThreshold", 0.25,
            "Threshold to consider difference between variances", false);
    subeval_options.add<int>("", "secondDist", 3,
            "Type of distance for second level of ranking", false);
    subeval_options.add<string>("", "binEmbDir", "",
            "The directory that contains the binarized embeddings of subgraphs, entities and relations.", false);
#endif

    /***** GENERAL OPTIONS *****/
    ProgramArgs::GroupArgs& cmdline_options = *vm.newGroup("General options");
    cmdline_options.add<string>("i","input", "",
            "The path of the KB directory",true);
    cmdline_options.add<string>("l", "logLevel", "info",
            "Set the log level (accepted values: trace, debug, info, warning, error, fatal). Default is info", false);
    cmdline_options.add<string>("", "logfile","",
            "Set if you want to store the logs in a file", false);

    sections.insert(make_pair("general",&cmdline_options));
    sections.insert(make_pair("query",&query_options));
    sections.insert(make_pair("lookup",&lookup_options));
    sections.insert(make_pair("load",&load_options));
    sections.insert(make_pair("test",&test_options));
    sections.insert(make_pair("add",&update_options));
    sections.insert(make_pair("rm",&update_options));
#ifdef ANALYTICS
    sections.insert(make_pair("analytics",&ana_options));
#endif
    sections.insert(make_pair("dump",&dump_options));
    sections.insert(make_pair("mine",&mine_options));
    sections.insert(make_pair("server",&server_options));
#ifdef ML
    sections.insert(make_pair("learn",&ml_options));
    sections.insert(make_pair("predict",&ml_options));
    sections.insert(make_pair("subeval",&subeval_options));
    sections.insert(make_pair("subcreate",&subeval_options));
#endif

    vm.parse(argc, argv);
    return checkParams(vm, argc, argv, vm, sections);
}
