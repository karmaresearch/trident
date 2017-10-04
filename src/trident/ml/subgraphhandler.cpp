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
        LOG(ERRORL) << "Subgraph format not implemented!";
        throw 10;
    }
}

void SubgraphHandler::selectRelevantSubGraphs(DIST dist,
        Querier *q,
        string algo,
        TYPEQUERY t, uint64_t rel, uint64_t ent,
        std::vector<uint64_t> &output,
        uint32_t topk) {
    //Get embedding for ent
    double *embe = E->get(ent);
    uint16_t dime = E->getDim();
    //Get embedding for rel
    double *embr = R->get(rel);

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
        LOG(ERRORL) << "Algo not recognized";
        throw 10;
    }

    //Get all the distances
    std::vector<std::pair<double, uint64_t>> distances;
    subgraphs->getDistanceToAllSubgraphs(dist, q, distances, emb.get(), dim);
    //Sort them by distance
    std::sort(distances.begin(), distances.end(), [](const std::pair<double, uint64_t> &a,
                const std::pair<double, uint64_t> &b) -> bool
            {
            return a.first < b.first;
            });
    output.clear();
    //Return the top k
    for(uint32_t i = 0; i < topk; ++i) {
        output.push_back(distances[i].second);
    }
}

long SubgraphHandler::isAnswerInSubGraphs(uint64_t a,
        const std::vector<uint64_t> &subgs, Querier *q) {
    long out = 0;
    for(auto subgraphid : subgs) {
        Subgraphs<double>::Metadata &meta = subgraphs->getMeta(subgraphid);
        if (meta.t == PO) {
            if (q->exists(a, meta.rel, meta.ent))
                return out;
        } else {
            if (q->exists(meta.ent, meta.rel, a))
                return out;
        }
        out++;
    }
    return -1;
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
        if (!Utils::exists(nametest)) {
            LOG(ERRORL) << "Test file " << nametest << " not found";
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
    //Stats
    uint64_t counth = 0;
    uint64_t sumh = 0;
    uint64_t countt = 0;
    uint64_t sumt = 0;
    //auto dict = q->getDictMgmt();
    //char buffer[MAX_TERM_SIZE];

    //Select most promising subgraphs to do the search
    std::vector<uint64_t> relevantSubgraphs;
    for(uint64_t i = 0; i < testTriples.size(); i+=3) {
        uint64_t h, t, r;
        h = testTriples[i];
        t = testTriples[i + 1];
        r = testTriples[i + 2];

        /*dict->getText(h, buffer);
        string sh = string(buffer);
        dict->getText(t, buffer);
        string st = string(buffer);
        int size;
        dict->getTextRel(r, buffer, size);
        string sr = string(buffer, size);
        LOG(INFOL) << "Testing " << h << " " << t << " " << r << " " << sh << " " << st << " " << sr;*/

        selectRelevantSubGraphs(L1, q.get(), algo, PO, r, t, relevantSubgraphs, 10);
        long foundH = isAnswerInSubGraphs(h, relevantSubgraphs, q.get());
        selectRelevantSubGraphs(L1, q.get(), algo, SP, r, h, relevantSubgraphs, 10);
        long foundT = isAnswerInSubGraphs(t, relevantSubgraphs, q.get());
        //Now I have the list of relevant subgraphs. It the answer in one of these?
        if (foundH >= 0) {
            counth++;
            sumh += foundH + 1;
        }
        if (foundT >= 0) {
            countt++;
            sumt += foundT + 1;
        }
    }
    LOG(INFOL) << "Found (H): " << counth << " (T): " << countt << " of " << testTriples.size();
    LOG(INFOL) << "Avg (H): " << sumh << "/" << counth << " (T): " << (double)sumt / countt;
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
