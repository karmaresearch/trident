#include <trident/ml/faisswrapper.h>
#include <trident/ml/subgraphhandler.h>
#include <trident/kb/querier.h>
#include <trident/ml/tester.h>
#include <trident/ml/transetester.h>
#include <trident/ml/holetester.h>
#include <trident/ml/batch.h>
#include <cmath>
#include <kognac/utils.h>
#include <unordered_map>
#include <set>
#include <fstream>

#include <faiss/IndexPQ.h>
#include <faiss/IndexIVFPQ.h>
#include <faiss/IndexFlat.h>
#include <faiss/IndexIVFFlat.h>
#include <faiss/index_io.h>

void FaissWrapper::loadEmbeddings(string embdir) {
    this->E = Embeddings<double>::load(embdir + "/E");
    this->R = Embeddings<double>::load(embdir + "/R");
}

void FaissWrapper::loadSubgraphs(string subgraphsFile, string subformat) {
    if (!Utils::exists(subgraphsFile)) {
        LOG(ERRORL) << "The file " << subgraphsFile << " does not exist";
        throw 10;
    }
    if (subformat == "avg") {
        subgraphs = std::shared_ptr<Subgraphs<double>>(new AvgSubgraphs<double>());
    } else if (subformat == "var" ||
            subformat == "avgvar" ||
            subformat == "avg+var" ||
            subformat == "kl") {
        subgraphs = std::shared_ptr<Subgraphs<double>>(new VarSubgraphs<double>());
    } else {
        LOG(ERRORL) << "Subgraph format not implemented!";
        throw 10;
    }
    subgraphs->loadFromFile(subgraphsFile);
}

void FaissWrapper::add(double *dest, double *v1, double *v2, uint16_t dim) {
    for(uint16_t i = 0; i < dim; ++i) {
        dest[i] = v1[i] + v2[i];
    }
}

void FaissWrapper::sub(double *dest, double *v1, double *v2, uint16_t dim) {
    for(uint16_t i = 0; i < dim; ++i) {
        dest[i] = v1[i] - v2[i];
    }
}

void FaissWrapper::create(string embDir, string subfile) {
    loadEmbeddings(embDir);
    int d = E->getDim();
    size_t nb = E->getN();
    size_t nr = R->getN();
    size_t nt = nb;
    int ncentroids = int(4 * sqrt(nb));  // total # of centroids
    int bytesPerCode = 4; // d must be multiple of this
    int bitsPerSubcode = 8;
    if (d % 10 == 0) {
        bytesPerCode = d /10;
    }
    faiss::IndexFlatL2 quantizer(d);
    faiss::IndexIVFPQ index(&quantizer, d, ncentroids, bytesPerCode, bitsPerSubcode);

    index.verbose = true;
    float float_emb[nt * d];
    for (long i2 = 0; i2 < nt; ++i2) {
        double *emb = E->get(i2);
        double *remb = NULL;
        for (uint16_t j2 = 0; j2 < d; ++j2) {
            float_emb[d * i2 + j2] = (float)emb[j2];
        }
        float_emb[d * i2] += i2 / 1000.;
    }
    index.train(nt, float_emb);
    index.add(nt, float_emb);
    index.nprobe = 16;
    faiss::write_index(&index, subfile.c_str());
}

void FaissWrapper::nearestNeighbours(
    unique_ptr<Querier>& q,
    int iq,
    string embAlgo,
    string subAlgo,
    int64_t threshold,
    DIST secondDist,
    ANSWER_METHOD ansMethod,
    Subgraphs<double>::TYPE type,
    uint64_t ent,
    uint64_t rel,
    SubgraphHandler &sh,
    vector<int64_t>& actualAnswers,
    int& k
    )
{
    uint16_t dim = E->getDim();
    std::unique_ptr<double> resultAnn = std::unique_ptr<double>(new double[dim]);
    std::chrono::system_clock::time_point start;
    std::chrono::duration<double> duration;
    // FAISS libraries only support float
    float queriesAnn[dim];
    if (type == Subgraphs<double>::TYPE::SP) {
        add(resultAnn.get(), E->get(ent), R->get(rel), dim);
    } else {
        sub(resultAnn.get(), E->get(ent), R->get(rel), dim);
    }
    for (uint16_t j = 0; j < dim; ++j) {
        queriesAnn[j] = (float) resultAnn.get()[j];
    }
    queriesAnn[0] += iq / 1000.;

    // Select sungraphs for this query
    vector<uint64_t> relevantSubgraphs;
    vector<double> relevantSubgraphsDistances;
    sh.selectRelevantSubGraphs(L1, q.get(), embAlgo, type,
                            rel, ent, relevantSubgraphs, relevantSubgraphsDistances,
                            threshold, subAlgo, secondDist);

    // Find union/intersection of these subgraphs
    sh.getAllPossibleAnswers(q.get(), relevantSubgraphs, actualAnswers, ansMethod);
    
    // Set out param k = size of number of entities in these subgraphs
    k = actualAnswers.size();
    
    // Reset the output vector for possible heads
    vector<int64_t>().swap(actualAnswers);

    // Query the Approx nearest neighbours for this query
    long long nns[k];
    float dis[k];
    start = std::chrono::system_clock::now();
    annIndex->search(1, queriesAnn, k, dis, nns);
    duration = std::chrono::system_clock::now() - start;
    for (int jj = 0; jj < k; ++jj) {
        actualAnswers.push_back(nns[jj]);
    }
}

void FaissWrapper::evaluate(
    KB& kb,
    string embAlgo,
    string embDir,
    string subFile,
    string subAlgo,
    string nameTest, 
    int64_t threshold,
    string subgraphFilter,
    DIST secondDist,
    string kFile
    )
{
    DictMgmt *dict = kb.getDictMgmt();
    std::unique_ptr<Querier> q(kb.query());

    //Load the embeddings
    LOG(INFOL) << "Loading the embeddings ...";
    loadEmbeddings(embDir);
    //Load the subgraphs
    LOG(INFOL) << "Loading the subgraphs ...";
    loadSubgraphs(subFile, subAlgo);

    //Load the test file
    std::vector<uint64_t> testTriples;
    std::vector<uint64_t> validTriples;
    unordered_map<uint64_t, pair<int, int>> relKMap;
    LOG(INFOL) << "Loading the queries ...";
    //The test set can be extracted from the database
    string pathtest;
    if (nameTest == "valid") {
        pathtest = BatchCreator::getValidPath(kb.getPath());
    } else if (nameTest == "test") {
        pathtest = BatchCreator::getTestPath(kb.getPath());
    } else {
        pathtest = nameTest;
    }
    LOG(INFOL) << "path test = " << pathtest;
    BatchCreator::loadTriples(pathtest, testTriples);

    SubgraphHandler sh; // Initialize with embedding loading, subgraph loading
    sh.loadEmbeddings(embDir);
    sh.loadSubgraphs(subFile, subAlgo);

    ANSWER_METHOD ansMethod = UNION;
    if (subgraphFilter == "intersection") {
        ansMethod = INTERSECTION;
    } else if(subgraphFilter == "interunion") {
        ansMethod = INTERUNION;
    }

    annIndex = faiss::read_index(kFile.c_str());
    assert(annIndex != NULL);
    uint16_t dim = E->getDim();
    int nq = testTriples.size()/3;
    uint64_t hitsHead = 0;
    uint64_t hitsTail = 0;
    int kHead = -1;
    int kTail = -1;
    for (int i = 0; i < testTriples.size(); i += 3) {
        uint64_t h = testTriples[i];
        uint64_t r = testTriples[i+1];
        uint64_t t = testTriples[i+2];

        int iq = i/3;
        // Head queries
        vector<int64_t> actualAnswersH;
        nearestNeighbours(
            q,
            iq,
            embAlgo,
            subAlgo,
            threshold,
            secondDist,
            ansMethod,
            Subgraphs<double>::TYPE::PO,
            t,
            r,
            sh,
            actualAnswersH,
            kHead);
        for (int j = 0; j < kHead; ++j) {
            if (h == actualAnswersH[j]) {
                hitsHead++;
                break;
            }
        }

        // Tail queries
        vector<int64_t> actualAnswersT;
        nearestNeighbours(
            q,
            iq,
            embAlgo,
            subAlgo,
            threshold,
            secondDist,
            ansMethod,
            Subgraphs<double>::TYPE::SP,
            h,
            r,
            sh,
            actualAnswersT,
            kTail);
        for (int j = 0; j < kTail; ++j) {
            if (t == actualAnswersT[j]) {
                hitsTail++;
                break;
            }
        }
    }
    float hitRateH = ((float)hitsHead / (float)(testTriples.size()/3))*100;
    float hitRateT = ((float)hitsTail / (float)(testTriples.size()/3))*100;
    LOG(INFOL) << "Hit@" << kHead << " (H): " << hitRateH;
    LOG(INFOL) << "Hit@" << kTail << " (T): " << hitRateT;
    return;
}
