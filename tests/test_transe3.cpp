#include <boost/chrono.hpp>

#include <sstream>
#include <fstream>
#include <iostream>
#include <vector>
#include <cstdlib>

#include <trident/ml/transe.h>
#include <trident/ml/transetester.h>
#include <trident/ml/embeddings.h>

#include <kognac/utils.h>

#define DIMS 50

namespace timens = boost::chrono;
using namespace std;

int main(int argc, const char** argv) {
    char buffer[24];
    ifstream ifs;
    ifs.open(argv[1]);
    //Load the entity and relation embeddings
    ifs.read(buffer, 4);
    int nents = *(int*)buffer;
    std::vector<double> embe_old;
    for(int j = 0; j < nents * DIMS; ++j) {
        ifs.read(buffer, 8);
        double v = *(double*)(buffer);
        embe_old.push_back(v);
    }
    ifs.read(buffer, 4);
    int nrels = *(int*)buffer;
    std::vector<double> embr_old;
    for(int j = 0; j < nrels * DIMS; ++j) {
        ifs.read(buffer, 8);
        double v = *(double*)(buffer);
        embr_old.push_back(v);
    }
    ifs.close();
    //Load embeddings
    std::shared_ptr<Embeddings<double>> E = std::shared_ptr<Embeddings<double>>(new Embeddings<double>(embe_old.size() / DIMS, DIMS, embe_old));
    std::shared_ptr<Embeddings<double>> R = std::shared_ptr<Embeddings<double>>(new Embeddings<double>(embr_old.size() / DIMS, DIMS, embr_old));
    std::unique_ptr<double> pe2;
    std::unique_ptr<double> pr2;

    //Test!
    std::vector<uint64_t> testset;
    BatchCreator::loadTriples(argv[2], testset);
    TranseTester<double> tester(E, R);
    auto result = tester.test("valid", testset, 1, 0);

    return 0;
}
