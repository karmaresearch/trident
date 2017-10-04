#include <trident/ml/tester.h>
#include <trident/ml/transetester.h>
#include <trident/utils/batch.h>

void Predictor::launchPrediction(KB &kb, string algo, PredictParams &p) {
    //Load model
    std::shared_ptr<Embeddings<double>> E;
    E = Embeddings<double>::load(p.path_modele);
    std::shared_ptr<Embeddings<double>> R;
    R = Embeddings<double>::load(p.path_modelr);

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
        TranseTester<double> tester(E,R);
        auto result = tester.test(p.nametestset, testset, p.nthreads, 0);
    } else {
        LOG(ERRORL) << "Not yet supported";
    }

}
