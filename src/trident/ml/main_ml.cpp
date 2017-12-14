#include <trident/kb/kb.h>
#include <trident/ml/learner.h>
#include <trident/ml/tester.h>
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
    LOG(INFOL) << "Launching " << op << " ...";
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

        Learner::launchLearning(kb, algo, p);
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
        if (mapparams.count("modele")) {
            p.path_modele = mapparams["modele"];
        }
        if (mapparams.count("modelr")) {
            p.path_modelr = mapparams["modelr"];
        }
        if (mapparams.count("nametestset")) {
            p.nametestset = mapparams["nametestset"];
        }
        if (mapparams.count("nthreads")) {
            p.nthreads = TridentUtils::lexical_cast<uint16_t>(mapparams["nthreads"]);
        }

        Predictor::launchPrediction(kb, algo, p);
    }
}

void subgraphEval(KB &kb, ProgramArgs &vm) {
    SubgraphHandler sh;
    sh.evaluate(kb, vm["subeval_algo"].as<string>(), vm["embdir"].as<string>(),
            vm["sgfile"].as<string>(), vm["sgformat"].as<string>(),
            vm["nametest"].as<string>(), "python");
}
