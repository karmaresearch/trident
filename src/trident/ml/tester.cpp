#include <trident/ml/tester.h>
#include <trident/ml/transetester.h>
#include <trident/ml/transebinarytester.h>
#include <trident/ml/holetester.h>
#include <trident/ml/batch.h>

PredictParams::PredictParams() {
    nametestset = "";
    nthreads = 1;
    path_modele = "";
    path_modelr = "";
    binary = "false";
}

string PredictParams::changeable_tostring() {
    string out = "nametestset=" + nametestset;
    out += ";nthreads=" + to_string(nthreads);
    out += ";path_modele=" + path_modele;
    out += ";path_modelr=" + path_modelr;
    out += ";binary=" + binary;
    return out;
}

void Predictor::launchPrediction(KB &kb, string algo, PredictParams &p) {
    //Load model
    std::shared_ptr<Embeddings<double>> E;
    std::shared_ptr<Embeddings<double>> R;
    if (p.binary == "true") {
        E = Embeddings<double>::loadBinary(p.path_modele);
        R = Embeddings<double>::loadBinary(p.path_modelr);
    } else {
        E = Embeddings<double>::load(p.path_modele);
        R = Embeddings<double>::load(p.path_modelr);
    }

    //Load test files
    std::vector<uint64_t> testset;
    string pathtest;
    if (p.nametestset == "valid") {
        pathtest = BatchCreator::getValidPath(kb.getPath());
    } else {
        pathtest = BatchCreator::getTestPath(kb.getPath());
    }
    BatchCreator::loadTriples(pathtest, testset);

    if (algo == "transe") {
        if (p.binary == "true") {
            TranseBinaryTester<double> tester(E, R, kb.query());
            auto result = tester.test(p.nametestset, testset, p.nthreads, 0);
        } else {
            TranseTester<double> tester(E,R, kb.query());
            auto result = tester.test(p.nametestset, testset, p.nthreads, 0);
        }
    } else if (algo == "hole") {
        HoleTester<double> tester(E,R, kb.query());
        auto result = tester.test(p.nametestset, testset, p.nthreads, 0);
    } else {
        LOG(ERRORL) << "Not yet supported";
    }

}
