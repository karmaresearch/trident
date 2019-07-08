#include <trident/kb/kb.h>
#include <trident/ml/trainworkflow.h>
#include <trident/ml/transe.h>
#include <trident/ml/hole.h>
#include <trident/ml/transetester.h>
#include <trident/ml/holetester.h>
#include <trident/ml/distmul.h>
#include <trident/ml/distmultester.h>
#include <trident/ml/subgraphhandler.h>
#include <trident/ml/batch.h>

#include <kognac/progargs.h>

bool _parseParams(std::string raw, std::map<std::string,std::string> &out) {
    //Tokenize depending on ;
    std::istringstream is(raw);
    string part;
    while (getline(is, part, ';')) {
        auto pos = part.find('=');
        if (pos != std::string::npos) {
            auto name = part.substr(0,pos);
            auto value = part.substr(pos+1);
            out.insert(std::make_pair(name, value));
        }
    }
    return true;
}

void launchML(KB &kb, string op, string algo, string paramsLearn,
        string paramsPredict) {
    if (!kb.areRelIDsSeparated()) {
        LOG(ERRORL) << "The KB is not loaded with separated Rel IDs. TranSE cannot be applied.";
        return;
    }

    if (op == "learn") {
        LearnParams p;
        std::map<std::string,std::string> mapparams;
        if (paramsLearn != "") {
            LOG(DEBUGL) << "Parsing params " << paramsLearn << " ...";
            if (!_parseParams(paramsLearn, mapparams)) {
                LOG(ERRORL) << "Parsing params has failed!";
                return;
            }
        }
        if (mapparams.count("dim")) {
            p.dim = TridentUtils::lexical_cast<uint16_t>(mapparams["dim"]);
        }
        if (mapparams.count("epochs")) {
            p.epochs = TridentUtils::lexical_cast<uint16_t>(mapparams["epochs"]);
        }
        if (mapparams.count("batchsize")) {
            p.batchsize = TridentUtils::lexical_cast<uint32_t>(mapparams["batchsize"]);
        }
        if (mapparams.count("nthreads")) {
            p.nthreads = TridentUtils::lexical_cast<uint16_t>(mapparams["nthreads"]);
        }
        if (mapparams.count("nevalthreads")) {
            p.nevalthreads = TridentUtils::lexical_cast<uint16_t>(mapparams["nevalthreads"]);
        }
        if (mapparams.count("nstorethreads")) {
            p.nstorethreads = TridentUtils::lexical_cast<uint16_t>(mapparams["nstorethreads"]);
        }
        if (mapparams.count("margin")) {
            p.margin = TridentUtils::lexical_cast<float>(mapparams["margin"]);
        }
        if (mapparams.count("learningrate")) {
            p.learningrate = TridentUtils::lexical_cast<float>(mapparams["learningrate"]);
        }
        if (mapparams.count("storefolder")) {
            p.storefolder = mapparams["storefolder"];
        }
        if (mapparams.count("storeits")) {
            p.storeits = TridentUtils::lexical_cast<uint32_t>(mapparams["storeits"]);
        }
        if (mapparams.count("evalits")) {
            p.evalits = TridentUtils::lexical_cast<uint32_t>(mapparams["evalits"]);
        }
        if (mapparams.count("valid")) {
            p.valid = TridentUtils::lexical_cast<float>(mapparams["valid"]);
        }
        if (mapparams.count("test")) {
            p.test = TridentUtils::lexical_cast<float>(mapparams["test"]);
        }
        if (mapparams.count("adagrad")) {
            p.adagrad = TridentUtils::lexical_cast<bool>(mapparams["adagrad"]);
        }
        if (mapparams.count("compresstorage")) {
            p.compresstorage = TridentUtils::lexical_cast<bool>(mapparams["compresstorage"]);
        }
        if (mapparams.count("feedbacks")) {
            p.feedbacks = TridentUtils::lexical_cast<bool>(mapparams["feedbacks"]);
        }
        if (mapparams.count("feedbacks_threshold")) {
            p.feedbacks_threshold = TridentUtils::lexical_cast<uint32_t>(mapparams["feedbacks_threshold"]);
        }
        if (mapparams.count("feedbacks_minfulle")) {
            p.feedbacks_minfulle = TridentUtils::lexical_cast<uint32_t>(mapparams["feedbacks_minfulle"]);
        }
        if (mapparams.count("numneg")) {
            p.numneg = TridentUtils::lexical_cast<uint64_t>(mapparams["numneg"]);
        }
        if (mapparams.count("filetrace")) {
            p.filetrace = mapparams["filetrace"];
        }
        if (mapparams.count("regeneratebatch")) {
            p.regeneratebatch = TridentUtils::lexical_cast<bool>(mapparams["regeneratebatch"]);
        }
        p.ne = kb.getNTerms();
        p.nr = kb.getDictMgmt()->getNRels();

        if (algo == "transe") {
            TrainWorkflow<TranseLearner,TranseTester<double>>::launchLearning(kb, p);
        } else if (algo == "hole") {
            TrainWorkflow<HoleLearner,HoleTester<double>>::launchLearning(kb, p);
        } else if (algo == "distmul") {
            TrainWorkflow<DistMulLearner,DistMulTester<double>>::launchLearning(kb, p);
        } else {
            LOG(ERRORL) << "Task not recognized";
        }

    } else { //can only be predict
        PredictParams p;
        //Parse the params
        std::map<std::string,std::string> mapparams;
        if (paramsPredict != "") {
            LOG(DEBUGL) << "Parsing params " << paramsPredict << " ...";
            if (!_parseParams(paramsPredict, mapparams)) {
                LOG(ERRORL) << "Parsing params has failed!";
                return;
            }
        }
        if (mapparams.count("path_modele")) {
            p.path_modele = mapparams["path_modele"];
        }
        if (mapparams.count("path_modelr")) {
            p.path_modelr = mapparams["path_modelr"];
        }
        if (mapparams.count("nametestset")) {
            p.nametestset = mapparams["nametestset"];
        }
        if (mapparams.count("binary")) {
            p.binary = mapparams["binary"];
        }
        if (mapparams.count("nthreads")) {
            p.nthreads = TridentUtils::lexical_cast<uint16_t>(mapparams["nthreads"]);
        }

        Predictor::launchPrediction(kb, algo, p);
    }
}

void subgraphEval(KB &kb, ProgramArgs &vm) {
    SubgraphHandler sh;
    DIST secondDist = (DIST) vm["secondDist"].as<int>();
    sh.evaluate(kb, vm["embAlgo"].as<string>(), vm["embDir"].as<string>(),
            vm["subFile"].as<string>(), vm["subAlgo"].as<string>(),
            vm["nameTest"].as<string>(), vm["formatTest"].as<string>(),
            vm["answerMethod"].as<string>(),
            vm["subgraphThreshold"].as<int64_t>(),
            vm["varThreshold"].as<double>(),
            vm["logFile"].as<string>(),
            secondDist,
            vm["kFile"].as<string>(),
            vm["binEmbDir"].as<string>(),
            vm["calcDisp"].as<bool>(),
            vm["sampleTest"].as<int64_t>());
}

void answer(KB &kb, ProgramArgs &vm) {
    //Params
    std::string embDir = vm["embDir"].as<std::string>();
    std::string subFile = vm["subFile"].as<std::string>();
    std::string subAlgo = vm["subAlgo"].as<std::string>();
    double varThreshold = vm["varThreshold"].as<double>();

    //Load the embeddings
    LOG(INFOL) << "Loading the embeddings ...";
    SubgraphHandler sh;
    sh.loadEmbeddings(embDir);
    LOG(INFOL) << "Loading the subgraphs ...";
    sh.loadSubgraphs(subFile, subAlgo, varThreshold);

    bool headQuery = false;
    uint64_t resource = 0;
    uint64_t relation = 0;

    //Load the query or queries
    if (vm.count("query")) {
        //Parse the query in JSON format
        uint64_t nElements = 0;
        DictMgmt *dict = kb.getDictMgmt();
        string queryFileName = vm["query"].as<string>();
        std::fstream inFile;
        inFile.open(queryFileName);//open the input file
        std::stringstream strStream;
        strStream << inFile.rdbuf();//read the file
        std::string value = strStream.str();
        JSON obj;
        JSON::read(value, obj);

        //Parse the relation
        std::string strVal = obj.getValue("predicate");
        nTerm key = 0;
        bool ok = dict->getNumberRel(strVal.c_str(), strVal.size(), &key);
        relation = key;

        if (obj.existKey("subject")) {
            strVal = obj.getValue("subject");
            headQuery = false;
        } else {
            strVal = obj.getValue("object");
            headQuery = true;
        }

        ok = dict->getNumberRel(strVal.c_str(), strVal.size(), &key);
        relation = key;




    } else {
        //Not implemented
        throw 10;
    }

    //TODO: Rank subgraphs
    std::vector<uint64_t> relevantSubgraphs;


    //TODO: Select the most promising ones

    //TODO: Select promising entities
}

void subgraphCreate(KB &kb, ProgramArgs &vm) {
    SubgraphHandler sh;
    sh.create(kb, vm["subAlgo"].as<string>(), vm["embDir"].as<string>(),
            vm["subFile"].as<string>(), vm["minSubgraphSize"].as<long>(), vm["removeLiterals"].as<bool>());
}
