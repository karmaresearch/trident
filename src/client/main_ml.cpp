#include <trident/kb/kb.h>
#include <trident/ml/learner.h>
#include <trident/ml/tester.h>
#include <trident/ml/subgraphhandler.h>
#include <trident/utils/batch.h>

#include <kognac/progargs.h>

#include <boost/spirit/include/qi.hpp>
#include <boost/spirit/include/karma.hpp>
#include <boost/fusion/adapted/std_pair.hpp>

void launchML(KB &kb, string op, string algo, string params) {
    LOG(INFOL) << "Launching " << op << " " << params << " ...";
    //Parse the params
    std::map<std::string,std::string> mapparams;
    std::string::iterator first = params.begin();
    std::string::iterator last  = params.end();
    const bool result = boost::spirit::qi::phrase_parse(first, last,
            *( *(boost::spirit::qi::char_-"=")  >> boost::spirit::qi::lit("=") >> *(boost::spirit::qi::char_-";") >> -boost::spirit::qi::lit(";") ),
            boost::spirit::ascii::space, mapparams);
    if (!result) {
        LOG(ERRORL) << "Parsing params " << params << " has failed!";
        return;
    }

    if (!kb.areRelIDsSeparated()) {
        LOG(ERRORL) << "The KB is not loaded with separated Rel IDs. TranSE cannot be applied.";
        return;
    }

    //Setting up parameters
    uint16_t epochs = 100;
    const uint32_t ne = kb.getNTerms();
    auto dict = kb.getDictMgmt();
    const uint32_t nr = dict->getNRels();
    uint16_t dim = 50;
    uint32_t batchsize = 1000;
    uint16_t nthreads = 1;
    uint16_t nstorethreads = 1;
    float margin = 1.0;
    float learningrate = 0.1;
    string storefolder = "";
    bool adagrad = false;
    bool compresstorage = false;
    uint64_t numneg = 0;
    bool usefeedback = false;
    uint32_t feedback_threshold = 10;
    uint32_t feedback_minfulle = 20;

    uint32_t evalits = 10;
    uint32_t storeits = 10;
    float valid = 0.0;
    float test = 0.0;
    string fileout;

    //params for predict
    string modele;
    string modelr;
    string nametestset;

    if (mapparams.count("dim")) {
        dim = TridentUtils::lexical_cast<uint16_t>(mapparams["dim"]);
    }
    if (mapparams.count("epochs")) {
        epochs = TridentUtils::lexical_cast<uint16_t>(mapparams["epochs"]);
    }
    if (mapparams.count("batchsize")) {
        batchsize = TridentUtils::lexical_cast<uint32_t>(mapparams["batchsize"]);
    }
    if (mapparams.count("nthreads")) {
        nthreads = TridentUtils::lexical_cast<uint16_t>(mapparams["nthreads"]);
    }
    if (mapparams.count("nstorethreads")) {
        nstorethreads = TridentUtils::lexical_cast<uint16_t>(mapparams["nstorethreads"]);
    }
    if (mapparams.count("margin")) {
        margin = TridentUtils::lexical_cast<float>(mapparams["margin"]);
    }
    if (mapparams.count("learningrate")) {
        learningrate = TridentUtils::lexical_cast<float>(mapparams["learningrate"]);
    }
    if (mapparams.count("storefolder")) {
        storefolder = mapparams["storefolder"];
    }
    if (mapparams.count("storeits")) {
        storeits = TridentUtils::lexical_cast<uint32_t>(mapparams["storeits"]);
    }
    if (mapparams.count("evalits")) {
        evalits = TridentUtils::lexical_cast<uint32_t>(mapparams["evalits"]);
    }
    if (mapparams.count("validperc")) {
        valid = TridentUtils::lexical_cast<float>(mapparams["validperc"]);
    }
    if (mapparams.count("testperc")) {
        test = TridentUtils::lexical_cast<float>(mapparams["testperc"]);
    }
    if (mapparams.count("adagrad")) {
        adagrad = TridentUtils::lexical_cast<bool>(mapparams["adagrad"]);
    }
    if (mapparams.count("compress")) {
        compresstorage = TridentUtils::lexical_cast<bool>(mapparams["compress"]);
    }
    if (mapparams.count("feedback")) {
        usefeedback = TridentUtils::lexical_cast<bool>(mapparams["feedback"]);
    }
    if (mapparams.count("feedback_threshold")) {
        feedback_threshold = TridentUtils::lexical_cast<uint32_t>(mapparams["feedback_threshold"]);
    }
    if (mapparams.count("feedback_minfullep")) {
        feedback_minfulle = TridentUtils::lexical_cast<uint32_t>(mapparams["feedback_minfulle"]);
    }
    if (mapparams.count("numneg")) {
        numneg = TridentUtils::lexical_cast<uint64_t>(mapparams["numneg"]);
    }
    if (mapparams.count("debugout")) {
        fileout = mapparams["debugout"];
    }
    if (mapparams.count("modele")) {
        modele = mapparams["modele"];
    }
    if (mapparams.count("modelr")) {
        modelr = mapparams["modelr"];
    }
    if (mapparams.count("nametestset")) {
        nametestset = mapparams["nametestset"];
    }
    if (op == "learn") {
        LearnParams p;
        p.epochs = epochs;
        p.ne = ne;
        p.nr = nr;
        p.dim = dim;
        p.margin = margin;
        p.learningrate = learningrate;
        p.batchsize = batchsize;
        p.adagrad = adagrad;

        p.nthreads = nthreads;
        p.nstorethreads = nstorethreads;
        p.evalits = evalits;
        p.storeits = storeits;
        p.storefolder = storefolder;
        p.compressstorage = compresstorage;
        p.filetrace = fileout;
        p.valid = valid;
        p.test = test;
        p.numneg = numneg;
        p.feedback = usefeedback;
        p.feedback_threshold = feedback_threshold;
        p.feedback_minFullEpochs = feedback_minfulle;
        Learner::launchLearning(kb, algo, p);
    } else { //can only be predict
        PredictParams p;
        p.path_modele = modele;
        p.path_modelr = modelr;
        p.nametestset = nametestset;
        p.nthreads = nthreads;
        Predictor::launchPrediction(kb, algo, p);
    }
}

void subgraphEval(KB &kb, ProgramArgs &vm) {
    SubgraphHandler sh;
    sh.evaluate(kb, vm["subeval_algo"].as<string>(), vm["embdir"].as<string>(),
            vm["sgfile"].as<string>(), vm["sgformat"].as<string>(),
            vm["nametest"].as<string>(), "python");
}

