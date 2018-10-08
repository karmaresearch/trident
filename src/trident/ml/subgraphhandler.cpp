#include <trident/ml/subgraphhandler.h>
#include <trident/kb/querier.h>
#include <trident/ml/tester.h>
#include <trident/ml/transetester.h>
#include <trident/ml/holetester.h>
#include <trident/ml/batch.h>
#include <cmath>
#include <kognac/utils.h>
#include <unordered_map>

void SubgraphHandler::loadEmbeddings(string embdir) {
    this->E = Embeddings<double>::load(embdir + "/E");
    this->R = Embeddings<double>::load(embdir + "/R");
}

void SubgraphHandler::loadSubgraphs(string subgraphsFile, string subformat, double varThreshold) {
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
        subgraphs = std::shared_ptr<Subgraphs<double>>(new VarSubgraphs<double>(varThreshold));
    } else {
        LOG(ERRORL) << "Subgraph format not implemented!";
        throw 10;
    }
    subgraphs->loadFromFile(subgraphsFile);
}

void SubgraphHandler::selectRelevantSubGraphs(DIST dist,
        Querier *q,
        string embeddingAlgo,
        Subgraphs<double>::TYPE t, uint64_t rel, uint64_t ent,
        std::vector<uint64_t> &output,
        uint32_t topk,
        string &subgraphType,
        DIST secondDist) {
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
    } else if (embeddingAlgo == "hole") {
        // TODO: Combine based on ccorr
        // temporary code same as transe
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
            t, rel, ent, E);

    //Sort them by distance
    std::sort(distances.begin(), distances.end(), [](const std::pair<double, uint64_t> &a,
                const std::pair<double, uint64_t> &b) -> bool
            {
            return a.first < b.first;
            });
    output.clear();

    assert(distances.size() <= subgraphs->getNSubgraphs());

    vector<std::pair<double, uint64_t>> variances;
    unordered_map <uint64_t, double> varmap;
    vector<std::pair<double, uint64_t>> varOutput;
    vector<std::pair<double, uint64_t>> divergences;
    if (subgraphType == "var") {
        subgraphs->getDistanceToAllSubgraphs(secondDist, q, variances, emb.get(), dim, t, rel, ent, E);
        for (auto v : variances) {
            varmap[v.second] = v.first;
        }

        for(uint32_t i = 0; i < distances.size() && i < topk; ++i) {
            varOutput.push_back(make_pair(varmap[distances[i].second],distances[i].second ));
        }

        std::sort(varOutput.begin(), varOutput.end(), [] (const std::pair<double, uint64_t> &a,
                    const std::pair<double, uint64_t> &b) -> bool
                {
                return a.first < b.first;
                });
    } else if (subgraphType == "kl") {
        subgraphs->getDistanceToAllSubgraphs(KL, q, divergences, emb.get(), dim, t, rel, ent, E);
        //Sort them by distance
        std::sort(divergences.begin(), divergences.end(), [](const std::pair<double, uint64_t> &a,
                    const std::pair<double, uint64_t> &b) -> bool
                {
                return a.first < b.first;
                });
    }

    //Return the top k
    if (subgraphType == "var") {
        for(uint32_t i = 0; i < varOutput.size(); ++i) {
            output.push_back(varOutput[i].second);
        }
    } else if (subgraphType == "kl") {
        for (uint32_t i = 0; i < divergences.size() && i < topk; ++i) {
            output.push_back(divergences[i].second);
        }
    } else {
        for(uint32_t i = 0; i < distances.size() && i < topk; ++i) {
            output.push_back(distances[i].second);
        }
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
            if (q->exists(a, meta.rel, meta.ent)) {
                return out;
            }
        } else {
            if (q->exists(meta.ent, meta.rel, a)) {
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
        uint64_t minSubgraphSize) {
    std::unique_ptr<Querier> q(kb.query());
    //Load the embeddings
    loadEmbeddings(embdir);
    LOG(INFOL) << "Creating subgraph embeddings using " << subgraphType << " minSize=" << minSubgraphSize;
    if (subgraphType == "avg") {
        subgraphs = std::shared_ptr<Subgraphs<double>>(
                new AvgSubgraphs<double>(0, minSubgraphSize));
    } else if (subgraphType == "var") {
        subgraphs = std::shared_ptr<Subgraphs<double>>(
                new VarSubgraphs<double>(0, minSubgraphSize));
    } else {
        LOG(ERRORL) << "Subgraph type not recognized!";
        throw 10;
    }
    subgraphs->calculateEmbeddings(q.get(), E, R);
    subgraphs->storeToFile(subfile);
}

template<>
void SubgraphHandler::getDisplacement<TranseTester<double>>(
        TranseTester<double> &tester,
        double *test,
        uint64_t h,
        uint64_t t,
        uint64_t r,
        uint64_t nents,
        uint16_t dime,
        uint16_t dimr,
        Querier *q,
        //OUTPUT
        uint64_t &displacementO,
        uint64_t &displacementS,
        std::vector<double> &scores,
        std::vector<std::size_t> &indices,
        std::vector<std::size_t> &indices2
        ) {
    // Test objects
    tester.predictO(h, dime, r, dimr, test);
    for(uint64_t idx = 0; idx < nents; ++idx) {
        scores[idx] = tester.closeness(test, idx, dime);
    }
    const uint64_t posO = tester.getPos(nents, scores, indices, indices2, t) + 1;

    auto itr = q->getPermuted(IDX_SPO, h, r, -1, true);
    uint64_t countResultsO = 0;
    while(itr->hasNext()) {
        itr->next();
        countResultsO++;
    }
    LOG(DEBUGL) << "object results = " << countResultsO;
    if (posO > countResultsO){
        displacementO = (posO - countResultsO);
    }

    // Test subjects
    tester.predictS(test, r, dimr, t, dime);
    for(uint64_t idx = 0; idx < nents; ++idx) {
        scores[idx] = tester.closeness(test, idx, dime);
    }
    const uint64_t posS = tester.getPos(nents, scores, indices, indices2, h) + 1;

    // Order POS in IDX_POS also determines order of parameters we pass to getPermuted()
    itr = q->getPermuted(IDX_POS, r, t, -1, true);
    uint64_t countResultsS = 0;
    while (itr->hasNext()) {
        itr->next();
        countResultsS++;
    }
    LOG(DEBUGL) << "subject results = " << countResultsS;
    if (posS > countResultsS){
        displacementS = (posS - countResultsS);
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
        double varThreshold,
        string writeLogs,
        DIST secondDist) {
    DictMgmt *dict = kb.getDictMgmt();
    std::unique_ptr<Querier> q(kb.query());

    //Load the embeddings
    LOG(INFOL) << "Loading the embeddings ...";
    loadEmbeddings(embDir);
    //Load the subgraphs
    LOG(INFOL) << "Loading the subgraphs ...";
    loadSubgraphs(subFile, subType, varThreshold);
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
    std::vector<double> scores;
    const uint64_t nents = E->getN();
    scores.resize(nents);

    TranseTester<double> tester(E, R, kb.query());
    const uint16_t dime = E->getDim();
    const uint16_t dimr = R->getDim();
    std::vector<double> testArray(dime);
    double *test = testArray.data();
    std::vector<std::size_t> indices(nents);
    std::iota(indices.begin(), indices.end(), 0u);
    std::vector<std::size_t> indices2(nents);
    std::iota(indices2.begin(), indices2.end(), 0u);
    uint64_t sumDisplacementOPos = 0;
    uint64_t sumDisplacementONeg = 0;
    uint64_t sumDisplacementSPos = 0;
    uint64_t sumDisplacementSNeg = 0;

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

        //Calculate the displacements
        uint64_t displacementO = 0;
        uint64_t displacementS = 0;
        getDisplacement<TranseTester<double>>(tester, test, h, t, r, nents,
                dime, dimr, q.get(), displacementO, displacementS, scores,
                indices, indices2);

        dict->getText(h, buffer);
        string sh = string(buffer);
        dict->getText(t, buffer);
        string st = string(buffer);
        int size;
        dict->getTextRel(r, buffer, size);
        string sr = string(buffer, size);

        LOG(DEBUGL) << "Query: ? " << sr << " " << st;
        DIST distType = L1;
        selectRelevantSubGraphs(distType, q.get(), embAlgo, Subgraphs<double>::TYPE::PO,
                r, t, relevantSubgraphsH, threshold, subType, secondDist);

        int64_t foundH = isAnswerInSubGraphs(h, relevantSubgraphsH, q.get());
        int64_t totalSizeH = numberInstancesInSubgraphs(q.get(), relevantSubgraphsH);

        LOG(DEBUGL) << "Query: " << sh << " " << sr << " ?";
        selectRelevantSubGraphs(distType, q.get(), embAlgo, Subgraphs<double>::TYPE::SP,
                r, h, relevantSubgraphsT, threshold, subType, secondDist);
        int64_t foundT = isAnswerInSubGraphs(t, relevantSubgraphsT, q.get());
        int64_t totalSizeT = numberInstancesInSubgraphs(q.get(), relevantSubgraphsT);
        //Now I have the list of relevant subgraphs. Is the answer in one of these?
        if (foundH >= 0) {
            counth++;
            sumh += foundH + 1;
            cons_comparisons_h += totalSizeH;
            sumDisplacementSPos += displacementS;
        } else {
            sumDisplacementSNeg += displacementS;
        }

        if (foundT >= 0) {
            countt++;
            sumt += foundT + 1;
            cons_comparisons_t += totalSizeT;
            sumDisplacementOPos += displacementO;
        } else {
            sumDisplacementONeg += displacementO;
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
                auto &metadata = subgraphs->getMeta(relevantSubgraphsT[foundT]);
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
    LOG(INFOL) << "Comp. reduction (H) " << cons_comparisons_h;
    LOG(INFOL) << "instead          of " << nents * counth;
    LOG(INFOL) << "Comp. reduction (T) " << cons_comparisons_t;
    LOG(INFOL) << "instead          of " << nents * countt;
    LOG(INFOL) << "Disp(O+): " << sumDisplacementOPos / testTriples.size();
    LOG(INFOL) << "Disp(O-): " << sumDisplacementONeg / testTriples.size();
    LOG(INFOL) << "Disp(S+): " << sumDisplacementSPos / testTriples.size();
    LOG(INFOL) << "Disp(S-): " << sumDisplacementSNeg / testTriples.size();
    LOG(INFOL) << "# entities : " << nents;

    double percentReductionH = ((double)((nents*counth) - cons_comparisons_h)/ (double)(nents * counth)) * 100;
    double percentReductionT = ((double)((nents*countt) - cons_comparisons_t)/ (double)(nents * countt)) * 100;

    float reductionInverseH = (float)1 / (float)percentReductionH;
    float reductionInverseT = (float)1 /  (float)percentReductionT;
    float hitRateH = ((float)counth / (float)(testTriples.size()/3))*100;
    float hitRateT = ((float)countt / (float)(testTriples.size()/3))*100;
    float f1H = (2 * reductionInverseH * hitRateH) / (reductionInverseH + hitRateH);
    float f1T = (2 * reductionInverseT * hitRateT) / (reductionInverseT + hitRateT);
    LOG(INFOL) << "f1H = " << f1H << " , f1T = " << f1T;
    std::cout << threshold <<"," << counth << ","<<countt << "," << std::fixed << std::setprecision(2)<< (double)sumh/counth <<"," \
        << (double)sumt/countt <<"," << percentReductionH <<"," << percentReductionT << "," \
        << sumDisplacementOPos / testTriples.size() << "," \
        << sumDisplacementONeg / testTriples.size() << "," \
        << sumDisplacementSPos / testTriples.size() << "," \
        << sumDisplacementSNeg / testTriples.size() << "," \
        << f1H << "," << f1T << \
        std::endl;

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
