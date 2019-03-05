#include <trident/ml/subgraphhandler.h>
#include <trident/kb/querier.h>
#include <trident/ml/batch.h>

#include <kognac/utils.h>

void SubgraphHandler::loadEmbeddings(string embdir) {
    this->E = Embeddings<double>::load(embdir + "/E");
    this->R = Embeddings<double>::load(embdir + "/R");
}

void SubgraphHandler::loadSubgraphs(string subgraphsFile, string subformat) {
    if (!Utils::exists(subgraphsFile)) {
        LOG(ERRORL) << "The file " << subgraphsFile << " does not exist";
        throw 10;
    }
    if (subformat == "avg") {
        subgraphs = std::shared_ptr<Subgraphs<double>>(new AvgSubgraphs<double>());
        subgraphs->loadFromFile(subgraphsFile);
    } else {
        LOG(ERRORL) << "Subgraph format not implemented!";
        throw 10;
    }
}

void SubgraphHandler::selectRelevantSubGraphs(DIST dist,
        Querier *q,
        string embeddingAlgo,
        Subgraphs<double>::TYPE t, uint64_t rel, uint64_t ent,
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
    if (embeddingAlgo == "transe") {
        dim = dime;
        emb = std::unique_ptr<double>(new double[dim]);
        if (t == Subgraphs<double>::TYPE::PO) {
            sub(emb.get(), embe, embr, dim);
        } else {
            add(emb.get(), embe, embr, dim);
        }
    } else {
        LOG(ERRORL) << "Algo not recognized";
        throw 10;
    }

    //Get all the distances. Notice that the same graph as the query is excluded
    std::vector<std::pair<double, uint64_t>> distances;
    subgraphs->getDistanceToAllSubgraphs(dist, q, distances, emb.get(), dim,
            t, rel, ent);
    //Sort them by distance
    std::sort(distances.begin(), distances.end(), [](const std::pair<double, uint64_t> &a,
                const std::pair<double, uint64_t> &b) -> bool
            {
            return a.first < b.first;
            });
    output.clear();

    assert(distances.size() == subgraphs->getNSubgraphs() - 1);
    //Return the top k
    for(uint32_t i = 0; i < distances.size() && i < topk; ++i) {
        output.push_back(distances[i].second);
    }
}

int64_t SubgraphHandler::isAnswerInSubGraphs(uint64_t a,
        const std::vector<uint64_t> &subgs, Querier *q) {
    DictMgmt *dict = q->getDictMgmt();
    char buffer[MAX_TERM_SIZE];
    int size;

    int64_t out = 0;
    for(auto subgraphid : subgs) {
        Subgraphs<double>::Metadata &meta = subgraphs->getMeta(subgraphid);

        dict->getText(meta.ent, buffer);
        string sent = string(buffer);
        dict->getTextRel(meta.rel, buffer, size);
        string srel = string(buffer, size);

        if (meta.t == Subgraphs<double>::TYPE::PO) {
            LOG(DEBUGL) << " GroupPO " << out << ": " << srel << " " << sent;
            if (q->exists(a, meta.rel, meta.ent)) {
                LOG(DEBUGL) << " ***FOUND***";
                return out;
            }
        } else {
            LOG(DEBUGL) << " GroupSP " << out << ": " << srel << " " << sent;
            if (q->exists(meta.ent, meta.rel, a)) {
                LOG(DEBUGL) << " ***FOUND***";
                return out;
            }
        }
        out++;
    }
    return -1;
}

int64_t SubgraphHandler::numberInstancesInSubgraphs(
        Querier *q,
        const std::vector<uint64_t> &subgs) {
    int64_t out = 0;
    for(auto subgraphid : subgs) {
        Subgraphs<double>::Metadata &meta = subgraphs->getMeta(subgraphid);
        out += meta.size;
    }
    return out;
}

void SubgraphHandler::create(KB &kb,
        string subgraphType,
        string embdir,
        string subfile,
	uint64_t minCard) {
    std::unique_ptr<Querier> q(kb.query());
    //Load the embeddings
    loadEmbeddings(embdir);
    if (subgraphType == "avg") {
        subgraphs = std::shared_ptr<Subgraphs<double>>(new AvgSubgraphs<double>(minCard));
        subgraphs->calculateEmbeddings(q.get(), E, R);
        subgraphs->storeToFile(subfile);
    } else {
        LOG(ERRORL) << "Subgraph type not recognized!";
        throw 10;
    }
}

void SubgraphHandler::evaluate(KB &kb,
        string embAlgo,
        string embDir,
        string subFile,
        string subType,
        string nameTest,
        string formatTest,
        uint64_t threshold,
        string writeLogs) {
    DictMgmt *dict = kb.getDictMgmt();
    std::unique_ptr<Querier> q(kb.query());

    //Load the embeddings
    LOG(INFOL) << "Loading the embeddings ...";
    loadEmbeddings(embDir);
    //Load the subgraphs
    LOG(INFOL) << "Loading the subgraphs ...";
    loadSubgraphs(subFile, subType);
    //Load the test file
    std::vector<uint64_t> testTriples;
    LOG(INFOL) << "Loading the queries ...";
    if (formatTest == "python") {
        //The file is a uncompressed file with all the test triples serialized after
        //each other
        if (!Utils::exists(nameTest)) {
            LOG(ERRORL) << "Test file " << nameTest << " not found";
            throw 10;
        }
        std::ifstream ifs;
        ifs.open(nameTest, std::ifstream::in);
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
        string pathtest;
        if (nameTest == "valid") {
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
    uint64_t cons_comparisons_h = 0;
    uint64_t cons_comparisons_t = 0;

    char buffer[MAX_TERM_SIZE];
    const uint64_t nents = E->getN();

    std::unique_ptr<ofstream> logWriter;
    if (writeLogs != "") {
        logWriter = std::unique_ptr<ofstream>(new ofstream());
        logWriter->open(writeLogs, std::ios_base::out);
        *logWriter.get() << "Query\tTestHead\tTestTail\tComparisonHead\tComparisonTail" << std::endl;
    }

    //Select most promising subgraphs to do the search
    LOG(INFOL) << "Test the queries ...";
    std::vector<uint64_t> relevantSubgraphsH;
    std::vector<uint64_t> relevantSubgraphsT;
    uint64_t offset = testTriples.size() / 100;
    if (offset == 0) offset = 1;
    for(uint64_t i = 0; i < testTriples.size(); i+=3) {
        uint64_t h, t, r;
        h = testTriples[i];
        r = testTriples[i + 1];
        t = testTriples[i + 2];
        if (i % offset == 0) {
            LOG(INFOL) << "***Tested " << i / 3 << " of "
                << testTriples.size() / 3 << " queries";
            if (i > 0) {
                LOG(INFOL) << "***Found (H): " << counth << " (T): " << countt <<
                    " of " << i / 3;
                LOG(INFOL) << "***Avg (H): " << (double) sumh / counth << " (T): "
                    << (double)sumt / countt;
                LOG(INFOL) << "***Comp. reduction (H) " << cons_comparisons_h <<
                    " instead of " << nents * counth;
                LOG(INFOL) << "***Comp. reduction (T) " << cons_comparisons_t <<
                    " instead of " << nents * countt;
            }
        }

        dict->getText(h, buffer);
        string sh = string(buffer);
        dict->getText(t, buffer);
        string st = string(buffer);
        int size;
        dict->getTextRel(r, buffer, size);
        string sr = string(buffer, size);

        LOG(DEBUGL) << "Query: ? " << sr << " " << st;
        selectRelevantSubGraphs(L1, q.get(), embAlgo, Subgraphs<double>::TYPE::PO,
                r, t, relevantSubgraphsH, threshold);
        int64_t foundH = isAnswerInSubGraphs(h, relevantSubgraphsH, q.get());
        int64_t totalSizeH = numberInstancesInSubgraphs(q.get(), relevantSubgraphsH);

        LOG(DEBUGL) << "Query: " << sh << " " << sr << " ?";
        selectRelevantSubGraphs(L1, q.get(), embAlgo, Subgraphs<double>::TYPE::SP,
                r, h, relevantSubgraphsT, threshold);
        int64_t foundT = isAnswerInSubGraphs(t, relevantSubgraphsT, q.get());
        int64_t totalSizeT = numberInstancesInSubgraphs(q.get(), relevantSubgraphsT);
        //Now I have the list of relevant subgraphs. It the answer in one of these?
        if (foundH >= 0) {
            counth++;
            sumh += foundH + 1;
            cons_comparisons_h += totalSizeH;
        }
        if (foundT >= 0) {
            countt++;
            sumt += foundT + 1;
            cons_comparisons_t += totalSizeT;
        }

        if (logWriter) {
            string line = to_string(h) + " " + to_string(r) + " " + to_string(t) + "\t";
            line += to_string(foundH) + "\t" + to_string(foundT) + "\t" +
                to_string(totalSizeH) + "\t" + to_string(totalSizeT);
            if (foundH >= 0) {
                auto &metadata = subgraphs->getMeta(relevantSubgraphsH[foundH]);
                line += "\t" + to_string(metadata.t) + "\t" + to_string(metadata.rel);
                line += "\t" + to_string(metadata.ent);
            } else {
                line += "\tN.A.\tN.A.\tN.A.";
            }
            if (foundT >= 0) {
                auto &metadata = subgraphs->getMeta(relevantSubgraphsT[foundH]);
                line += "\t" + to_string(metadata.t) + "\t" + to_string(metadata.rel);
                line += "\t" + to_string(metadata.ent);
            } else {
                line += "\tN.A.\tN.A.\tN.A.";
            }
            *logWriter.get() << line << endl;
        }
    }
    LOG(INFOL) << "Found (H): " << counth << " (T): " << countt << " of " << testTriples.size() / 3;
    LOG(INFOL) << "Avg (H): " << (double) sumh / counth << " (T): " << (double)sumt / countt;
    LOG(INFOL) << "Comp. reduction (H) " << cons_comparisons_h << " instead of " << nents * counth;
    LOG(INFOL) << "Comp. reduction (T) " << cons_comparisons_t << " instead of " << nents * countt;

    if (logWriter) {
        logWriter->close();
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
