#include <trident/ml/subgraphhandler.h>
#include <trident/kb/querier.h>
#include <trident/utils/batch.h>

void SubgraphHandler::loadEmbeddings(string embdir) {
    this->E = Embeddings<double>::load(embdir + "/E");
    this->R = Embeddings<double>::load(embdir + "/R");
}

void SubgraphHandler::loadSubgraphs(string subgraphsFile, string subformat) {
    if (subformat == "cikm") {
        subgraphs = std::shared_ptr<Subgraphs<double>>(new CIKMSubgraphs<double>());
        subgraphs->loadFromFile(subgraphsFile);
    } else {
        BOOST_LOG_TRIVIAL(error) << "Subgraph format not implemented!";
        throw 10;
    }
}

void SubgraphHandler::selectRelevantSubGraphs(DIST dist,
        Querier *q,
        string algo,
        TYPEQUERY t, uint64_t rel, uint64_t ent,
        std::vector<uint64_t> &output) {
    //Get embedding for ent
    double *embe = E->get(ent);
    uint16_t dime = E->getDim();
    //Get embedding for rel
    double *embr = R->get(ent);

    //Combine them (depends on the algo)
    uint16_t dim = 0;
    std::unique_ptr<double> emb = NULL;
    if (algo == "transe") {
        dim = dime;
        emb = std::unique_ptr<double>(new double[dim]);
        if (t == PO) {
            sub(emb.get(), embe, embr, dim);
        } else {
            add(emb.get(), embe, embr, dim);
        }
    } else {
        BOOST_LOG_TRIVIAL(error) << "Algo not recognized";
        throw 10;
    }

    //Get all the distances
    std::vector<double> distances;
    subgraphs->getDistanceToAllSubgraphs(dist, q, distances, emb.get(), dim);
    //TODO: pick the top and copy them to output
}

bool SubgraphHandler::isAnswerInSubGraphs(uint64_t a,
        const std::vector<uint64_t> &subgraphs, Querier *q) {
    //TODO
    return false;
}

void SubgraphHandler::evaluate(KB &kb,
        string algo,
        string embdir,
        string subfile,
        string subformat,
        string nametest,
        string format) {
    std::unique_ptr<Querier> q(kb.query());
    //Load the embeddings
    loadEmbeddings(embdir);
    //Load the subgraphs
    loadSubgraphs(subfile, subformat);
    //Load the test file
    std::vector<uint64_t> testTriples;
    if (format == "python") {
        //The file is a uncompressed file with all the test triples serialized after
        //each other
        if (!fs::exists(nametest)) {
            BOOST_LOG_TRIVIAL(error) << "Test file " << nametest << " not found";
            throw 10;
        }
        std::ifstream ifs;
        ifs.open(nametest, std::ifstream::in);
        const uint16_t sizeline = 8 * 3;
        std::unique_ptr<char> buffer = std::unique_ptr<char>(new char[sizeline]); //one line
        while (true) {
            ifs.read(buffer.get(), sizeline);
            if (ifs.eof()) {
                break;
            }
            testTriples.push_back(*(uint64_t*)buffer.get());
            testTriples.push_back(*(uint64_t*)(buffer.get()+8));
            testTriples.push_back(*(uint64_t*)(buffer.get()+16));
        }
    } else {
        //The test set can be extracted from the database
        throw 10; //not implemented yet
        string pathtest;
        if (nametest == "valid") {
            pathtest = BatchCreator::getValidPath(kb.getPath());
        } else {
            pathtest = BatchCreator::getTestPath(kb.getPath());
        }
        BatchCreator::loadTriples(pathtest, testTriples);
    }

    /*** TEST ***/
    //Select most promising subgraphs to do the search
    std::vector<uint64_t> relevantSubgraphs;
    for(uint64_t i = 0; i < testTriples.size(); i+=3) {
        uint64_t h, t, r;
        h = testTriples[i];
        t = testTriples[i + 1];
        r = testTriples[i + 2];
        selectRelevantSubGraphs(L1, q.get(), algo, PO, r, t, relevantSubgraphs);
        bool foundH = isAnswerInSubGraphs(h, relevantSubgraphs, q.get());
        selectRelevantSubGraphs(L1, q.get(), algo, SP, r, h, relevantSubgraphs);
        bool foundT = isAnswerInSubGraphs(t, relevantSubgraphs, q.get());
        //TODO: Now I have the list of relevant subgraphs. It the answer in one of these?
    }
}

void SubgraphHandler::add(double *dest, double *v1, double *v2, uint16_t dim) {
    for(uint16_t i = 0; i < dim; ++i) {
        dest[i] = v1[i] + v2[i];
    }
}

void SubgraphHandler::sub(double *dest, double *v1, double *v2, uint16_t dim) {
    for(uint16_t i = 0; i < dim; ++i) {
        dest[i] = v1[i] - v2[i];
    }
}
