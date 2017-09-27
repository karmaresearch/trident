#include <trident/kb/kbconfig.h>
#include <trident/binarytables/storagestrat.h>

#include <snap/tasks.h>

#include <boost/log/trivial.hpp>
#include <boost/program_options.hpp>
#include <boost/filesystem/operations.hpp>
#include <boost/filesystem/path.hpp>

#include <iostream>

using namespace std;
namespace logging = boost::log;
namespace po = boost::program_options;
namespace fs = boost::filesystem;

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
        cout << "info\t\t print some information about the KB." << endl << endl;
        cout << "dump\t\t dump the graph on files." << endl << endl;
        cout << "learn\t\t launch an algorithm to calculate KG embeddings." << endl << endl;
        cout << "server\t\t start a server for SPARQL queries." << endl << endl;
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
            && cmd != "dump"
            && cmd != "learn"
            && cmd != "predict"
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

    po::options_description ml_options("Options for <learn> or <predict>");
    ml_options.add_options()("algo",
            po::value<string>()->default_value(""),
            "The task to perform This parameter is REQUIRED.");
    ml_options.add_options()("args",
            po::value<string>()->default_value(""),
            "arguments for the task, separated by ;");


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

    po::options_description subeval_options("Options for <subeval>");
    subeval_options.add_options()("subeval_algo",
            po::value<string>()->default_value(""),
            "The algorithm used to create embeddings. This parameter is REQUIRED.");
    subeval_options.add_options()("embdir",
            po::value<string>()->default_value(""),
            "The directory that contains the embeddings.");
    subeval_options.add_options()("nametest",
            po::value<string>()->default_value(""),
            "The path (or name) of the dataset to use to test the performance.");


    po::options_description cmdline_options("Generic options");
    cmdline_options.add(query_options).add(lookup_options).add(load_options).add(test_options).add(update_options).add(ana_options).add(dump_options).add(mine_options).add(server_options).add(ml_options).add(subeval_options);

    sections.insert(make_pair("query",query_options));
    sections.insert(make_pair("lookup",lookup_options));
    sections.insert(make_pair("load",load_options));
    sections.insert(make_pair("test",test_options));
    sections.insert(make_pair("update",update_options));
    sections.insert(make_pair("analytics",ana_options));
    sections.insert(make_pair("dump",dump_options));
    sections.insert(make_pair("mine",mine_options));
    sections.insert(make_pair("server",server_options));
    sections.insert(make_pair("ml",ml_options));

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

