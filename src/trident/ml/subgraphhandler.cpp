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


void SubgraphHandler::loadEmbeddings(string embdir) {
    this->E = Embeddings<double>::load(embdir + "/E");
    this->R = Embeddings<double>::load(embdir + "/R");
}

void SubgraphHandler::processBinarizedEmbeddingsDirectory(string binEmbDir,
        vector<double>& subCompressedEmbeddings,
        vector<double>& entCompressedEmbeddings,
        vector<double>& relCompressedEmbeddings) {
    auto npos = string::npos;
    bool flag = false;
    if (binEmbDir[binEmbDir.length()-1] == '/') {
        npos = binEmbDir.length() - 2;
        flag = true;
    }
    auto lastIndexofSlash = binEmbDir.find_last_of("/", npos);
    string dbName = "";
    if (string::npos != lastIndexofSlash) {
        if (!flag) {
            dbName += "/";
        }
        dbName += binEmbDir.substr(lastIndexofSlash+1, npos-lastIndexofSlash);
    }
    string subFile = binEmbDir + dbName + "-sub-var-comp-size-64.bin";
    string entFile = binEmbDir + dbName + "-ent--comp-size-64.bin";
    string relFile = binEmbDir + dbName + "-rel--comp-size-64.bin";

    LOG(INFOL) << subFile;
    LOG(INFOL) << entFile;
    LOG(INFOL) << relFile;
    loadBinarizedEmbeddings(subFile, subCompressedEmbeddings);
    loadBinarizedEmbeddings(entFile, entCompressedEmbeddings);
    loadBinarizedEmbeddings(relFile, relCompressedEmbeddings);
}

void SubgraphHandler::loadBinarizedEmbeddings(string subFile, vector<double>& rawCompressedEmbeddings) {
    const uint16_t sizeline = 25;
    std::unique_ptr<char> buffer = std::unique_ptr<char>(new char[sizeline]);
    ifstream ifs;
    ifs.open(subFile, std::ifstream::in);
    ifs.read(buffer.get(), 8);
    uint64_t nSubgraphs = *(uint64_t*)buffer.get();
    LOG(INFOL) << "# subgraphs : " << nSubgraphs;

    for (int i = 0; i < nSubgraphs; ++i) {
        ifs.read(buffer.get(), 25);
        //int type = (int)buffer.get()[0];
        //uint64_t ent = *(uint64_t*) (buffer.get() + 1);
        //uint64_t rel = *(uint64_t*) (buffer.get() + 9);
        //uint64_t siz = *(uint64_t*) (buffer.get() + 17);
        //LOG(INFOL) << type << ") " << ent << " , " << rel << " : " << siz;
    }

    memset(buffer.get(), 0, 25);
    ifs.read(buffer.get(), 18);
    uint16_t dims = Utils::decode_short(buffer.get());
    uint64_t mincard = Utils::decode_long(buffer.get() , 2);
    uint64_t nextBytes = Utils::decode_long(buffer.get(), 10);
    LOG(INFOL) << dims << " , " << mincard << "," << nextBytes;

    const uint16_t sizeOriginalEmbeddings = dims * 8;
    std::unique_ptr<char> buffer2 = std::unique_ptr<char>(new char[sizeOriginalEmbeddings]);
    for (int i = 0; i < nSubgraphs; ++i) {
        ifs.read(buffer2.get(), dims*8);
    }

    memset(buffer.get(), 0, 25);
    ifs.read(buffer.get(), 2);
    uint16_t compSize = Utils::decode_short(buffer.get());
    LOG(INFOL) << compSize;

    if (64 == compSize) {
        compSize = 1;
    }
    const uint16_t sizeCompressedEmbeddings = compSize * 8;
    std::unique_ptr<char> buffer3 = std::unique_ptr<char>(new char[sizeCompressedEmbeddings]);
    for (int i = 0; i < nSubgraphs; ++i) {
        ifs.read(buffer3.get(), compSize*8);
        rawCompressedEmbeddings.push_back(*(uint64_t*)buffer3.get());
    }

    LOG(INFOL) << "total compressed embeddings : " << rawCompressedEmbeddings.size();
    ifs.close();
}

void SubgraphHandler::loadSubgraphs(string subgraphsFile, string subformat) {
    if (!Utils::exists(subgraphsFile)) {
        LOG(ERRORL) << "The file " << subgraphsFile << " does not exist";
        throw 10;
    }
    if (subformat == "avg" || subformat == "annavg") {
        subgraphs = std::shared_ptr<Subgraphs<double>>(new AvgSubgraphs<double>());
    } else if (subformat == "var" ||
            subformat == "avgvar" ||
            subformat == "annvar" ||
            subformat == "avg+var" ||
            subformat == "kl") {
        subgraphs = std::shared_ptr<Subgraphs<double>>(new VarSubgraphs<double>());
    } else {
        LOG(ERRORL) << "Subgraph format not implemented!";
        throw 10;
    }
    subgraphs->loadFromFile(subgraphsFile);
}

void SubgraphHandler::getAnswerAccuracy(vector<uint64_t>& expectedEntities,
        vector<int64_t>& actualEntities,
        double& precision,
        double& recall) {
    uint64_t correctGuesses = 0;
    uint64_t guesses = actualEntities.size();
    if (0 == guesses) {
        return;
    }
    sort(expectedEntities.begin(), expectedEntities.end());
    for (auto ae : actualEntities) {
        if (std::binary_search(expectedEntities.begin(), expectedEntities.end(),ae)) {
            correctGuesses += 1;
        }
    }
    uint64_t total = expectedEntities.size();
    if (0 != total) {
        recall = double(correctGuesses /double(total)) * 100;
    }
    precision = double(correctGuesses / double(guesses)) * 100;
}

void SubgraphHandler::getModelAccuracy(vector<vector<uint64_t>>& expectedEntities,
        vector<vector<int64_t>>& actualEntities,
        double& precision,
        double& recall) {
    uint64_t totalCorrectGuesses = 0;
    uint64_t totalActualGuesses = 0;
    uint64_t totalExpectedAnswers = 0;
    assert(expectedEntities.size() == actualEntities.size());

    for (int i = 0; i < expectedEntities.size(); ++i){
        uint64_t correctGuesses = 0;
        uint64_t guesses = actualEntities[i].size();
        if (0 == guesses) {
            continue;
        }
        sort(expectedEntities[i].begin(), expectedEntities[i].end());
        for (auto ae : actualEntities[i]) {
            if (std::binary_search(expectedEntities[i].begin(), expectedEntities[i].end(),ae)) {
                correctGuesses += 1;
            }
        }
        uint64_t total = expectedEntities[i].size();
        totalCorrectGuesses += correctGuesses;
        totalActualGuesses += guesses;
        totalExpectedAnswers += total;
    }
    if (0 != totalExpectedAnswers) {
        recall = double(totalCorrectGuesses /double(totalExpectedAnswers)) * 100;
    }
    precision = double(totalCorrectGuesses / double(totalActualGuesses)) * 100;
    LOG(INFOL) << "correct guesses : " << totalCorrectGuesses;
    LOG(INFOL) << "total guesses   : " << totalActualGuesses;
    LOG(INFOL) << "expected answers: " << totalExpectedAnswers;
}

void SubgraphHandler::getExpectedAnswersFromTest(vector<uint64_t>& testTriples,
        Subgraphs<double>::TYPE type,
        uint64_t rel,
        uint64_t ent,
        vector<uint64_t> &output){
    for(uint64_t i = 0; i < testTriples.size(); i+=3) {
        uint64_t h, t, r;
        h = testTriples[i];
        r = testTriples[i + 1];
        t = testTriples[i + 2];
        if (type == Subgraphs<double>::TYPE::PO) {
            if (r == rel && t == ent) {
                output.push_back(h);
            }
        } else {
            if (r == rel && h == ent) {
                output.push_back(t);
            }
        }
    }
}

inline vector<int64_t> intersection (const std::vector<std::vector<int64_t>> &vecs) {

    auto last_intersection = vecs[0];
    std::vector<int64_t> curr_intersection;

    for (std::size_t i = 1; i < vecs.size(); ++i) {
        std::set_intersection(last_intersection.begin(), last_intersection.end(),
                vecs[i].begin(), vecs[i].end(),
                std::back_inserter(curr_intersection));
        std::swap(last_intersection, curr_intersection);
        curr_intersection.clear();
    }
    return last_intersection;
}

inline vector<int64_t> intersectionNPercent(const vector<vector<int64_t>> &entityIds, int percent=50) {
    vector<int64_t> output;
    size_t N = entityIds.size();
    size_t required = (int) (N * ((float)percent/100.0));
    for (int i = 0; i < entityIds.size(); ++i) {
        for (int j = 0; j < entityIds[i].size(); ++j) {
            int cntFound = 0;
            for (int k = 0; k < entityIds.size(); ++k) {
                bool found = std::binary_search(entityIds[k].begin(), entityIds[k].end(), entityIds[i][j]);
                if (found) {
                    cntFound++;
                }
            }
            if (cntFound == required) {
                output.push_back(entityIds[i][j]);
            }
        }
    }
    return output;
}

template<class K >
K random_unique(K begin, K end, size_t num_random) {
    size_t left = std::distance(begin, end);
    srand(time(0));
    while (num_random--) {
        K r = begin;
        std::advance(r, rand()%left);
        std::swap(*begin, *r);
        ++begin;
        --left;
    }
    return begin;
}

vector<uint64_t> sampleTriples(vector<uint64_t> & triples, int nChosen=1000) {
    uint64_t totalTriples = triples.size() / 3;
    LOG(DEBUGL) << "total triples : " << totalTriples;
    vector<uint64_t> tripleIndices(totalTriples);
    for (uint64_t i = 0; i < totalTriples; ++i) {
        tripleIndices[i] = i;
    }
    random_unique(tripleIndices.begin(), tripleIndices.end(), nChosen);

    assert(tripleIndices.size() == nChosen);
    vector<uint64_t> chosenTriples(nChosen * 3);
    for (uint64_t i = 0, j = 0; i < nChosen*3 && j < tripleIndices.size(); i+=3, j+=1) {
        chosenTriples[i]   = triples[tripleIndices[j]*3];
        chosenTriples[i+1] = triples[tripleIndices[j]*3 + 1];
        chosenTriples[i+2] = triples[tripleIndices[j]*3 + 2];
    }
    return chosenTriples;
}

vector<uint64_t> SubgraphHandler::sampleSubgraphs(vector<uint64_t>& allsub, int percent) {
    vector<uint64_t> chosenSubgraphs;
    uint64_t totalEntities = 0;
    uint64_t maxEntities = (uint64_t)(E->getN() *(double)percent/100);
    for (auto id: allsub) {
        totalEntities += subgraphs->getMeta(id).size;
        if (totalEntities >= maxEntities) {
            break;
        }
        chosenSubgraphs.push_back(id);
    }
    return chosenSubgraphs;
}

double SubgraphHandler::calculateScore(uint64_t ent,
        vector<uint64_t>& subgs,
        Querier* q) {
    int i = 0;
    size_t N = subgs.size();
    double score = 0.0;
    for(auto subgraphid : subgs) {
        Subgraphs<double>::Metadata &meta = subgraphs->getMeta(subgraphid);

        if (meta.t == Subgraphs<double>::TYPE::PO) {
            if (q->exists(ent, meta.rel, meta.ent)) {
                score += (N-i);
            }
        } else {
            if (q->exists(meta.ent, meta.rel, ent)) {
                score += (N-i);
            }
        }
        i++;
    }
    // TODO: whether to multiply with distances ?
    // Perhaps not, because
    // subgraphs are sorted in the ascending order of these distances
    // and the distances are just scores which will be lowest for the first subgraph
    // and highest for the last subgraph
    return score;
}

void SubgraphHandler::getAllPossibleAnswers(Querier *q,
        vector<uint64_t> &relevantSubgraphs,
        vector<int64_t> &output,
        ANSWER_METHOD answerMethod
        ) {
    uint64_t totalAnswers = 0;
    vector<vector<int64_t>> entityIds;
    unordered_map <uint64_t, double> scoreMap;
    set<int64_t> unionAnswers;
    int i = 0;
    for(auto subgraphid : relevantSubgraphs) {
        Subgraphs<double>::Metadata &meta = subgraphs->getMeta(subgraphid);
        int queryType = -1;
        if (meta.t == Subgraphs<double>::TYPE::PO) {
            queryType = IDX_POS;
        } else {
            queryType = IDX_PSO;
        }
        auto itr = q->getPermuted(queryType, meta.rel, meta.ent, -1, true);
        uint64_t countResultsS = 0;
        if (answerMethod == INTERSECTION || answerMethod == INTERUNION) {
            entityIds.push_back(vector<int64_t>());
        }
        //LOG(DEBUGL) << "Subgraph ID : " << subgraphid << " " << meta.rel  << " , " << meta.ent;
        while(itr->hasNext()) {
            //int64_t p = itr->getKey();
            //int64_t e1 = itr->getValue1();
            int64_t e2 = itr->getValue2();
            //LOG(DEBUGL) << countResultsS << ")" << e1 << ": " << p << " , " << e2;
            if (answerMethod == INTERSECTION || answerMethod == INTERUNION) {
                entityIds[i].push_back(e2);
            } else {
                unionAnswers.insert(e2);
                //output.push_back(e2);
            }
            itr->next();
            countResultsS++;
            totalAnswers++;
        }
        if (answerMethod == INTERSECTION) {
            sort(entityIds[i].begin(), entityIds[i].end());
        } else if (answerMethod == INTERUNION) {
            for (auto ent : entityIds[i]) {
                if (scoreMap.find(ent) == scoreMap.end()) {
                    scoreMap[ent] = calculateScore(ent, relevantSubgraphs, q);
                    //LOG(INFOL) << "Score : " << scoreMap[ent];
                }
            }
        }
        i++;
    }

    if (answerMethod == INTERSECTION) {
        output = intersectionNPercent(entityIds);
    } else if (answerMethod == INTERUNION) {
        for (auto scores : scoreMap) {
            auto N = relevantSubgraphs.size();
            /**
             * If there are 5 subgraphs (N = 5)
             * then the score of the entity should be more than 2
             * If entity appears in top 1 subgraph, then its score is 5
             * if it appears in 2nd ranked subgraph, then its score is 4
             * and so on
             * In this way, if the entity appears in more than one low ranked
             * subgraphs, we still include it in potential answers.
             * */
            double desiredScore = N * 0.4;
            if (scores.second > desiredScore) {
                //LOG(INFOL) << "Score = " << scores.second << " desired = " << desiredScore;
                output.push_back(scores.first);
            }
        }
    } else {
        std::copy(unionAnswers.begin(), unionAnswers.end(), back_inserter(output));
    }
    //LOG(DEBUGL) << "Total possible answers : " << totalAnswers;
}


void SubgraphHandler::selectRelevantBinarySubgraphs(
        Subgraphs<double>::TYPE t,
        uint64_t rel,
        uint64_t ent,
        uint32_t topK,
        vector<double>& subCompressedEmbeddings,
        vector<double>& entCompressedEmbeddings,
        vector<double>& relCompressedEmbeddings,
        vector<uint64_t>& output) {

    uint64_t result;
    double embE = entCompressedEmbeddings[ent];
    double embR = relCompressedEmbeddings[rel];
    if (t == Subgraphs<double>::TYPE::PO) {
        result = ~((uint64_t) embE | (uint64_t) embR);
    } else {
        result = (uint64_t) embE | (uint64_t) embR;
    }

    vector<pair<uint64_t, uint64_t>> distances;
    for (int i = 0 ; i < subCompressedEmbeddings.size(); ++i) {
        uint64_t distance = result ^ (uint64_t)(subCompressedEmbeddings[i]);
        distances.push_back(make_pair(distance, i));
    }
    std::sort(distances.begin(), distances.end(), [](const std::pair<uint64_t, uint64_t> &a,
                const std::pair<uint64_t, uint64_t> &b) -> bool
            {
            return a.first < b.first;
            });
    output.clear();
    for (uint32_t i = 0; i < distances.size() && i < topK; ++i) {
        output.push_back(distances[i].second);
    }
}

void SubgraphHandler::selectRelevantSubGraphs(DIST dist,
        Querier *q,
        string embeddingAlgo,
        Subgraphs<double>::TYPE t, uint64_t rel, uint64_t ent,
        std::vector<uint64_t> &output,
        vector<double> &outputDistances,
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
            outputDistances.push_back(varOutput[i].first);
        }
    } else if (subgraphType == "kl") {
        for (uint32_t i = 0; i < divergences.size() && i < topk; ++i) {
            output.push_back(divergences[i].second);
            outputDistances.push_back(divergences[i].first);
        }
    } else {
        for(uint32_t i = 0; i < distances.size() && i < topk; ++i) {
            output.push_back(distances[i].second);
            outputDistances.push_back(distances[i].first);
        }
    }
}

int64_t SubgraphHandler::isTripleInSubGraphs(
        uint64_t h,
        uint64_t t,
        const std::vector<uint64_t> &subgs,
        Querier *q) {
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
            if (q->exists(h, meta.rel, meta.ent)) {
                //LOG(DEBUGL) << sent << " :<-->: " << srel;
                return out;
            }
        } else {
            if (q->exists(meta.ent, meta.rel, t)) {
                //LOG(DEBUGL) << sent << " :<-->: " << srel;
                return out;
            }
        }
        out++;
    }
    return -1;
}

int64_t SubgraphHandler::isAnswerInSubGraphs(
        uint64_t a,
        const std::vector<uint64_t> &subgs,
        Querier *q) {
    //DictMgmt *dict = q->getDictMgmt();
    //char buffer[MAX_TERM_SIZE];
    //int size;

    int64_t out = 0;
    for(auto subgraphid : subgs) {
        Subgraphs<double>::Metadata &meta = subgraphs->getMeta(subgraphid);

        //dict->getText(meta.ent, buffer);
        //string sent = string(buffer);
        //dict->getTextRel(meta.rel, buffer, size);
        //string srel = string(buffer, size);

        if (meta.t == Subgraphs<double>::TYPE::PO) {
            if (q->exists(a, meta.rel, meta.ent)) {
                //LOG(INFOL) << sent << " :<-->: " << srel;
                return out;
            }
        } else {
            if (q->exists(meta.ent, meta.rel, a)) {
                //LOG(INFOL) << sent << " :<-->: " << srel;
                return out;
            }
        }
        out++;
    }
    return -1;
}

vector<uint64_t> SubgraphHandler::areTriplesInSubGraphs(
        vector<uint64_t>& testTriples,
        const std::vector<uint64_t> &subgs,
        Querier *q) {
    DictMgmt *dict = q->getDictMgmt();
    char buffer[MAX_TERM_SIZE];
    int size;
    vector<uint64_t> output;
    vector<uint64_t> unfoundTriples;
    vector<uint64_t> workingTestTriples = testTriples;
    int64_t out = 0;
    uint64_t countLiteralSubgraphs = 0;
    for(auto subgraphid : subgs) {
        Subgraphs<double>::Metadata &meta = subgraphs->getMeta(subgraphid);

        dict->getText(meta.ent, buffer);
        string sent = string(buffer);
        dict->getTextRel(meta.rel, buffer, size);
        string srel = string(buffer, size);
        if (sent.find("\"") != std::string::npos) {
            ++countLiteralSubgraphs;
            LOG(DEBUGL) << countLiteralSubgraphs << " literal subgraphs...";
            continue;
        }
        //LOG(DEBUGL) << "Subgraph of [" << sent << "] , [" << srel << "]";
        for (uint64_t i = 0; i < workingTestTriples.size(); i+=3) {
            uint64_t h, t, r;
            h = workingTestTriples[i];
            r = workingTestTriples[i + 1];
            t = workingTestTriples[i + 2];
            //dict->getText(h, buffer);
            //string sh = string(buffer);
            //dict->getText(t, buffer);
            //string st = string(buffer);
            //dict->getTextRel(r, buffer, size);
            //string sr = string(buffer, size);

            //LOG(DEBUGL) << "Triple: " << sh << " "  << sr << " " << st;
            bool found = false;
            if (meta.t == Subgraphs<double>::TYPE::PO) {
                if (q->exists(h, meta.rel, meta.ent)) {
                    //LOG(DEBUGL) << sh << " :<-" << srel<< "->: "  << sent << " (" << out << ")";
                    found = true;
                    output.push_back(h);
                    output.push_back(r);
                    output.push_back(t);
                }
            } else {
                if (q->exists(meta.ent, meta.rel, t)) {
                    //LOG(DEBUGL) << sent << " :<-" << srel<< "->: "  << st << " (" << out << ")";
                    found = true;
                    output.push_back(h);
                    output.push_back(r);
                    output.push_back(t);
                }
            }

            if (!found) {
                unfoundTriples.push_back(h);
                unfoundTriples.push_back(r);
                unfoundTriples.push_back(t);
            }
        }
        workingTestTriples = unfoundTriples;
        LOG(DEBUGL) << "Subgraph : " << subgraphid <<" Unfound triples = " << unfoundTriples.size()/3;
        LOG(DEBUGL) << "Subgraph : " << subgraphid <<" output triples  = " << output.size()/3;
        if (output.size()/ 3 > 100000) {
            break;
        }
        unfoundTriples.clear();
        out++;
    }
    return output;
}

void SubgraphHandler::areAnswersInSubGraphs(
        vector<uint64_t> entities,
        const std::vector<uint64_t> &subgs,
        Querier *q,
        unordered_map<uint64_t, int64_t>& entityRankMap) {
    DictMgmt *dict = q->getDictMgmt();
    char buffer[MAX_TERM_SIZE];
    int size;
    typedef unordered_map<uint64_t, int64_t>::value_type map_value_type;
    int64_t out = 0;
    for(auto subgraphid : subgs) {
        Subgraphs<double>::Metadata &meta = subgraphs->getMeta(subgraphid);

        dict->getText(meta.ent, buffer);
        string sent = string(buffer);
        dict->getTextRel(meta.rel, buffer, size);
        string srel = string(buffer, size);

        // If all entities have found the ranks
        // = there is no -1 is not found in values of the map
        // then, return
        if (entityRankMap.end() == find_if(entityRankMap.begin(),entityRankMap.end(),
                    [](const map_value_type& vt)
                    {
                    return vt.second == -1;
                    })) {
            return;
        }

        for (auto a : entities) {
            dict->getText(a, buffer);
            string aText = string(buffer, size);
            if (meta.t == Subgraphs<double>::TYPE::PO) {
                if (q->exists(a, meta.rel, meta.ent)) {
                    //LOG(INFOL) << aText << " :<-" << srel<< "->: "  << sent << " (" << out << ")";
                    entityRankMap[a] = out;
                }
            } else {
                if (q->exists(meta.ent, meta.rel, a)) {
                    //LOG(INFOL) << sent << " :<-" << srel<< "->: "  << aText << " (" << out << ")";
                    entityRankMap[a] = out;
                }
            }
        }
        out++;
    }
}

uint64_t SubgraphHandler::numberInstancesInSubgraphs(
        Querier *q,
        const std::vector<uint64_t> &subgs) {
    uint64_t out = 0;
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
        uint64_t minSubgraphSize,
        bool removeLiterals) {
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
    subgraphs->calculateEmbeddings(q.get(), E, R, removeLiterals);
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

int64_t SubgraphHandler::getDynamicThreshold(
        Querier* q,
        vector<uint64_t> &validTriples,
        Subgraphs<double>::TYPE type,
        uint64_t &r,
        uint64_t &e,
        string &embAlgo,
        string &subAlgo,
        DIST secondDist,
        int64_t &subgraphThreshold,
        bool hugeKG
        ) {
    //DictMgmt *dict = q->getDictMgmt();
    //char buffer[MAX_TERM_SIZE];
    uint64_t nSubgraphs = subgraphs->getNSubgraphs();
    // 1. Get all entities for this r and e from validation set
    unordered_map<uint64_t, int64_t> entityRankMap;
    vector<uint64_t> validEntities;
    std::chrono::system_clock::time_point start;
    std::chrono::duration<double> duration;

    start = std::chrono::system_clock::now();
    for(uint64_t j = 0; j < validTriples.size(); j+=3) {
        uint64_t hv, tv, rv;
        hv = validTriples[j];
        rv = validTriples[j + 1];
        tv = validTriples[j + 2];
        //dict->getText(hv, buffer);
        //string hvText = string(buffer);
        //int size;
        //dict->getTextRel(rv, buffer, size);
        //string rvText = string(buffer, size);
        //dict->getText(tv, buffer);
        //string tvText = string(buffer);
        if (type == Subgraphs<double>::TYPE::PO) {
            if( rv == r && tv == e) {
                // Store <ENTITY-ID, -1>
                // -1 will be replaced by the rank of the subgraph
                // where this ENTITY-ID will be found.
                entityRankMap.insert(make_pair(hv,-1));
                validEntities.push_back(hv);
                //LOG(INFOL) << "r = " << r << " , rv = " << rv;
                //LOG(INFOL) << hvText << " <-- " << rvText << " --> "<< tvText;
            }
        } else {
            if (rv == r && hv == e) {
                entityRankMap.insert(make_pair(tv,-1));
                validEntities.push_back(tv);
                //LOG(INFOL) << hvText << " <-- " << rvText << " --> "<< tvText;
            }
        }
    }
    duration = std::chrono::system_clock::now() - start;
    LOG(DEBUGL) << "Time to go through all validation triples = " << duration.count() * 1000 << " ms";

    uint64_t threshold = 10;
    if (nSubgraphs > 100) {
        threshold = (uint64_t)(nSubgraphs / 3);
    }

    start = std::chrono::system_clock::now();
    if (0 == validEntities.size()) {
        // No triples found that share the same relation+entity pair in valid set
        // Check in the database (training triples)
        int queryType = -1;
        if (type == Subgraphs<double>::TYPE::PO) {
            queryType = IDX_POS;
        } else {
            queryType = IDX_PSO;
        }
        auto itr = q->getPermuted(queryType, r, e, -1, true);
        int samples = 0;
        while (itr->hasNext()) {
            if (hugeKG) {
                if (100 == samples) {
                    break;
                }
            }
            validEntities.push_back(itr->getValue2());
            itr->next();
            samples++;
        }
    }
    duration = std::chrono::system_clock::now() - start;
    LOG(DEBUGL) << "Time to go through database for similar triples = " << duration.count() * 1000 << " ms";

    if (validEntities.size() <= 4) {
        // No similar triples or very few similar triples found
        // in validation or training set
        LOG(DEBUGL) << "Number of entities found in train/valid sets = " << validEntities.size()<<" Use Threshold = " << threshold;
        return threshold;
    }
    vector<uint64_t> allSubgraphs;
    vector<double> allDistances;

    start = std::chrono::system_clock::now();
    selectRelevantSubGraphs(L1, q, embAlgo, type,
            r, e, allSubgraphs, allDistances, 0xffffffff, subAlgo, secondDist);
    duration = std::chrono::system_clock::now() - start;
    LOG(DEBUGL) << "Time to sort all subgraphs for this query = " << duration.count() * 1000 << " ms";
    vector<int64_t> rankValues;
    assert(validEntities.size() == entityRankMap.size());

    // Make sure that all subgraphs are selected
    assert(allSubgraphs.size() == subgraphs->getNSubgraphs());

    // Filter subgraphs based on their size:
    // cap on number of total possible entities
    // TODO: review if this sampling is necessary
    vector<uint64_t> chosenSubgraphs = sampleSubgraphs(allSubgraphs);

    // find out ranks for all these entities
    start = std::chrono::system_clock::now();
    areAnswersInSubGraphs(validEntities, chosenSubgraphs, q, entityRankMap);
    duration = std::chrono::system_clock::now() - start;
    LOG(DEBUGL) << "Time to find "<< validEntities.size() <<" answers in all "  << chosenSubgraphs.size() << " subgraphs: " << duration.count()*1000 << "ms";
    for (auto kv: entityRankMap) {
        if (kv.second != -1) {
            rankValues.push_back(kv.second);
        }
    }
    int64_t maxTopK = (int64_t)(nSubgraphs / 3);
    if (0 != rankValues.size()) {
        if (-1 == subgraphThreshold) {
            LOG(DEBUGL) << "Accumulating " << rankValues.size() << " values";
            threshold = std::accumulate(rankValues.begin(), rankValues.end(), 0.0) / rankValues.size();
            LOG(DEBUGL) << "AVG (K) = " << threshold;
        } else if (-2 == subgraphThreshold) {
            threshold = *std::max_element(rankValues.begin(), rankValues.end());
        }
        // max cap is 50% of total subgraphs
        if (threshold > maxTopK) {
            threshold = maxTopK;
            LOG(DEBUGL) << "NEW (K) = " << threshold;
        }
    }
    assert(threshold != 0);
    LOG(DEBUGL) << "Use Threshold = " << threshold;
    return threshold;
}

vector<uint64_t> SubgraphHandler::removeImprobables(vector<uint64_t> & testTriples, Querier* q) {
    vector<uint64_t> output;
    vector<uint64_t> allSubIds;
    for (uint64_t i = 0; i < subgraphs->getNSubgraphs(); ++i) {
        allSubIds.push_back(i);
    }
    for (uint64_t i = 0; i < testTriples.size(); i+=3) {
        uint64_t h, t, r;
        h = testTriples[i];
        r = testTriples[i + 1];
        t = testTriples[i + 2];
        int64_t found = isTripleInSubGraphs(h, r, allSubIds, q);
        if (found >= 0) {
            output.push_back(h);
            output.push_back(r);
            output.push_back(t);
        }
        if (i % 3000 == 0) {
            LOG(DEBUGL) << i/3 << " triples processed. output =  " << output.size()/3;
        }
        if (output.size() / 3 > 10000) {
            break;
        }
    }
    return output;
}

vector<uint64_t> SubgraphHandler::removeLiterals(vector<uint64_t> & testTriples, KB &kb) {

    char buffer[MAX_TERM_SIZE];
    DictMgmt *dict = kb.getDictMgmt();
    vector<uint64_t> output;
    for(uint64_t i = 0; i < testTriples.size(); i+=3) {
        uint64_t h, t, r;
        h = testTriples[i];
        r = testTriples[i + 1];
        t = testTriples[i + 2];
        dict->getText(h, buffer);
        string sh = string(buffer);
        dict->getText(t, buffer);
        string st = string(buffer);
        int size;
        dict->getTextRel(r, buffer, size);
        string sr = string(buffer, size);

        //LOG(DEBUGL) << sh << " " << sr << " " << st;
        if (sh.find("\"") != std::string::npos ||
                st.find("\"") != std::string::npos ||
                sr.find("\"") != std::string::npos) {
            continue;
        }
        if (h == t) {
            LOG(DEBUGL) << "skipping same head and tail";
            continue;
        }
        output.push_back(h);
        output.push_back(r);
        output.push_back(t);
    }

    return output;
}

void SubgraphHandler::useDynamicKache(
        Querier* q,
        vector<uint64_t> &validTriples,
        Subgraphs<double>::TYPE type,
        uint64_t &r,
        uint64_t &e,
        string &embAlgo,
        string &subAlgo,
        DIST secondDist,
        int64_t &subgraphThreshold,
        Kache& dynamicKache,
        uint64_t& threshold,
        bool hugeKG
        )
{
    std::chrono::system_clock::time_point start;
    std::chrono::duration<double> duration;
    Kache_it got = dynamicKache.find(e);
    int64_t tempThreshold;
    if (dynamicKache.end() == got) {
        // The entity does not exist in the cache as the key
        start = std::chrono::system_clock::now();
        tempThreshold = getDynamicThreshold(q, validTriples,
                type, r, e, embAlgo,
                subAlgo, secondDist, subgraphThreshold, hugeKG);
        duration = std::chrono::system_clock::now() - start;

        LOG(DEBUGL) << "Time to compute dynamic K (head): " << duration.count() * 1000 << " ms";
        dynamicKache[e] = vector<pair<uint64_t,int64_t>>();
        dynamicKache[e].push_back(make_pair(r, threshold));
    } else {
        // Entity exists as the key
        // find out its vector and check if the pair exists for this relation
        vector<pair<uint64_t, int64_t>> existingPairs = dynamicKache[e];
        bool predFound = false;
        for (const auto &ep : existingPairs) {
            if (ep.first == r) {
                tempThreshold = ep.second;
                predFound = true;
                break;
            }
        }
        if (false == predFound) {
            start = std::chrono::system_clock::now();
            tempThreshold = getDynamicThreshold(q, validTriples,
                    type, r, e, embAlgo,
                    subAlgo, secondDist, subgraphThreshold, hugeKG);
            duration = std::chrono::system_clock::now() - start;
            LOG(DEBUGL) << "Time to compute dynamic K (head): " << duration.count() * 1000 << " ms";
            dynamicKache[e].push_back(make_pair(r,threshold));
        }
    }
    threshold = tempThreshold;
}

void SubgraphHandler::evaluate(KB &kb,
        string embAlgo,
        string embDir,
        string subFile,
        string subAlgo,
        string nameTest,
        string formatTest,
        string subgraphFilter,
        int64_t subgraphThreshold,
        string writeLogs,
        DIST secondDist,
        string kFile,
        string binEmbDir,
        bool calcDisp,
        int64_t sampleTest) {
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
    uint64_t threshold = subgraphThreshold;
    unordered_map<uint64_t, pair<int, int>> relKMap;
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
        } else if (nameTest == "test") {
            pathtest = BatchCreator::getTestPath(kb.getPath());
        } else {
            pathtest = nameTest;
        }
        LOG(INFOL) << "path test = " << pathtest;
        BatchCreator::loadTriples(pathtest, testTriples);
    }

    /*** TEST ***/
    //Stats
    uint64_t hitsHead = 0;
    uint64_t subgraphRanksHead = 0;
    uint64_t hitsTail = 0;
    uint64_t subgraphRanksTail = 0;
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
    bool hugeKG = false;

    std::chrono::system_clock::time_point start;
    std::chrono::duration<double> duration;
    Kache dynamicKache;
    std::unique_ptr<ofstream> logWriter;

    if (writeLogs != "") {
        logWriter = std::unique_ptr<ofstream>(new ofstream());
        logWriter->open(writeLogs, std::ios_base::out);
        *logWriter.get() << "Query\tTestHead\tTestTail\tComparisonHead\tComparisonTail" << std::endl;
    }

    LOG(INFOL) << "Initial number of test triples : " << testTriples.size()/3;

    // Count number of entities without literals
    uint64_t nEntitiesWithoutLiterals = 0;// for Wikidata 148546616;
    for (uint64_t i = 0; i < nents; ++i) {
        dict->getText(i, buffer);
        string sent = string(buffer);
        if (sent[0] != '\"') {
            nEntitiesWithoutLiterals++;
        }
    }

    LOG(DEBUGL) << "# Entities without literals : " << nEntitiesWithoutLiterals;
    if (testTriples.size() > 1000000) {
        hugeKG = true;
        testTriples = sampleTriples(testTriples, sampleTest);
        LOG(DEBUGL) << "After sampling: # of test triples : " << testTriples.size();
    }

    //Select most promising subgraphs to do the search
    LOG(INFOL) << "Test the queries ...";
    std::vector<uint64_t> relevantSubgraphsH;
    std::vector<uint64_t> relevantSubgraphsT;
    std::vector<double> relevantSubgraphsHDistances;
    std::vector<double> relevantSubgraphsTDistances;
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
                LOG(INFOL) << "***Hits (H): " << hitsHead << " (T): " << hitsTail <<
                    " of " << i / 3;
                LOG(INFOL) << "***Mean Rank (H): " << (double) subgraphRanksHead / hitsHead << " (T): "
                    << (double)subgraphRanksTail / hitsTail;
                LOG(INFOL) << "***Comp. reduction (H) " << cons_comparisons_h <<
                    " instead of " << nents * hitsHead;
                LOG(INFOL) << "***Comp. reduction (T) " << cons_comparisons_t <<
                    " instead of " << nents * hitsTail;
            }
        }

        //Calculate the displacements
        uint64_t displacementO = 0;
        uint64_t displacementS = 0;
        if (calcDisp) {
            getDisplacement<TranseTester<double>>(tester, test, h, t, r, nents,
                    dime, dimr, q.get(), displacementO, displacementS, scores,
                    indices, indices2);
        }

        dict->getText(h, buffer);
        string sh = string(buffer);
        dict->getText(t, buffer);
        string st = string(buffer);
        int size;
        dict->getTextRel(r, buffer, size);
        string sr = string(buffer, size);

        DIST distType = L1;

        if (-1 == subgraphThreshold || -2 == subgraphThreshold) {
            useDynamicKache(q.get(), validTriples,
                Subgraphs<double>::TYPE::PO, r, t, embAlgo,
                subAlgo, secondDist, subgraphThreshold, dynamicKache, threshold, hugeKG);
        } else if (subgraphThreshold > 100 && subgraphThreshold <= 200) {
            // Calculate new threshold based on %
            // E.g. 101 => 1% of total subgraphs
            threshold = (uint64_t) (subgraphs->getNSubgraphs() * ((double)(subgraphThreshold - 100)/(double)100));
            if (0 == threshold) {
                // If 1% of X is 0, then use absolute value 2
                // If 3% of X is 0, then use absolute value 4 for top K.
                threshold = (subgraphThreshold - 100) + 1;
            }
        }

        start = std::chrono::system_clock::now();
        selectRelevantSubGraphs(distType, q.get(), embAlgo, Subgraphs<double>::TYPE::PO,
                r, t, relevantSubgraphsH, relevantSubgraphsHDistances,threshold, subAlgo, secondDist);
        duration = std::chrono::system_clock::now() - start;
        LOG(DEBUGL) << "Time to sort " << threshold << " subgraphs based on distance (HEAD)= " << duration.count() * 1000 << " ms";

        ANSWER_METHOD ansMethod = UNION;
        if (subgraphFilter == "intersection") {
            ansMethod = INTERSECTION;
        } else if(subgraphFilter == "interunion") {
            ansMethod = INTERUNION;
        }
        int64_t foundH = -1;
        uint64_t totalSizeH = 0;
        vector<int64_t> entitiesH;
        if (UNION == ansMethod) {
            //TODO: this foundH is the rank of the subgraph
            start = std::chrono::system_clock::now();
            foundH = isAnswerInSubGraphs(h, relevantSubgraphsH, q.get());
            duration = std::chrono::system_clock::now() - start;
            LOG(DEBUGL) << "Time to check the HEAD answer = " << duration.count() * 1000 << " ms";
            //totalSizeH = numberInstancesInSubgraphs(q.get(), relevantSubgraphsH);
        } else {
            vector<int64_t>::iterator it = find(entitiesH.begin(), entitiesH.end(), h);
            if (it != entitiesH.end()) {
                // This foundH is the rank of the entity among the selected entities
                foundH = distance(entitiesH.begin(), it);
            }
        }
        LOG(DEBUGL) << "Query: " << sh << " " << sr << " ?";
        if (-1 == subgraphThreshold || -2 == subgraphThreshold) {
            useDynamicKache(q.get(), validTriples,
                Subgraphs<double>::TYPE::SP, r, h, embAlgo,
                subAlgo, secondDist, subgraphThreshold, dynamicKache, threshold, hugeKG);
        }

        start = std::chrono::system_clock::now();
        selectRelevantSubGraphs(distType, q.get(), embAlgo, Subgraphs<double>::TYPE::SP,
                r, h, relevantSubgraphsT, relevantSubgraphsTDistances,threshold, subAlgo, secondDist);
        duration = std::chrono::system_clock::now() - start;
        LOG(DEBUGL) << "Time to sort " << threshold << " subgraphs based on distance (TAIL)= " << duration.count() * 1000 << " ms";
        int64_t foundT = -1;
        uint64_t totalSizeT = 0;
        vector<int64_t> entitiesT;
        if(UNION == ansMethod) {
            //Now I have the list of relevant subgraphs. Is the answer in one of these?
            start = std::chrono::system_clock::now();
            foundT = isAnswerInSubGraphs(t, relevantSubgraphsT, q.get());
            duration = std::chrono::system_clock::now() - start;
            LOG(DEBUGL) << "Time to check the TAIL answer = " << duration.count() * 1000 << " ms";
            // Following code would not give union of all entities but is faster
            //totalSizeT = numberInstancesInSubgraphs(q.get(), relevantSubgraphsT);
        } else {
            vector<int64_t>::iterator it = find(entitiesT.begin(), entitiesT.end(), t);
            if (it != entitiesT.end()) {
                foundT = distance(entitiesT.begin(),it);
            }
        }
        if (foundH >= 0) {
            hitsHead++;
            subgraphRanksHead += foundH + 1;

            start = std::chrono::system_clock::now();
            getAllPossibleAnswers(q.get(), relevantSubgraphsH, entitiesH, ansMethod);
            duration = std::chrono::system_clock::now() - start;
            LOG(DEBUGL) << "Time to find all potential answers(union/intersection) (head) = " << duration.count() * 1000 << " ms";
            totalSizeH = entitiesH.size();
            cons_comparisons_h += totalSizeH;
            sumDisplacementSPos += displacementS;
            LOG(DEBUGL) << "HIT HEAD";
        } else {
            sumDisplacementSNeg += displacementS;
        }

        if (foundT >= 0) {
            hitsTail++;
            subgraphRanksTail += foundT + 1;
            start = std::chrono::system_clock::now();
            getAllPossibleAnswers(q.get(), relevantSubgraphsT, entitiesT, ansMethod);
            duration = std::chrono::system_clock::now() - start;
            LOG(DEBUGL) << "Time to find all potential answers(union/intersection) (tail) = " << duration.count() * 1000 << " ms";
            totalSizeT = entitiesT.size();
            cons_comparisons_t += totalSizeT;
            sumDisplacementOPos += displacementO;
            LOG(DEBUGL) << "HIT TAIL";
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

    LOG(INFOL) << "# Figurative entities: " << nEntitiesWithoutLiterals;
    double woLiteralsnormalComparisonsH = nEntitiesWithoutLiterals * hitsHead;
    double woLiteralsnormalComparisonsT = nEntitiesWithoutLiterals * hitsTail;
    double woLiteralPercentReductionH   = ((double)(woLiteralsnormalComparisonsH - cons_comparisons_h)/woLiteralsnormalComparisonsH) * 100;
    double woLiteralPercentReductionT   = ((double)(woLiteralsnormalComparisonsT - cons_comparisons_t)/woLiteralsnormalComparisonsT) * 100;
    LOG(INFOL) << "Without Literals Percent Reduction (H): " << woLiteralPercentReductionH;
    LOG(INFOL) << "Without Literals Percent Reduction (T): " << woLiteralPercentReductionT;
    LOG(INFOL) << "# entities : " << nents;
    LOG(INFOL) << "# Subgraphs: " << subgraphs->getNSubgraphs();
    LOG(INFOL) << "Hits (H): " << hitsHead << " (T): " << hitsTail << " of " << testTriples.size() / 3;

    double normalComparisonsH = nents * hitsHead;
    double normalComparisonsT = nents * hitsTail;
    LOG(INFOL) << "Comp. reduction (H) " << cons_comparisons_h;
    LOG(INFOL) << "instead          of " << normalComparisonsH;
    LOG(INFOL) << "Comp. reduction (T) " << cons_comparisons_t;
    LOG(INFOL) << "instead          of " << normalComparisonsT;
    LOG(INFOL) << "Disp(O+): " << sumDisplacementOPos / testTriples.size();
    LOG(INFOL) << "Disp(O-): " << sumDisplacementONeg / testTriples.size();
    LOG(INFOL) << "Disp(S+): " << sumDisplacementSPos / testTriples.size();
    LOG(INFOL) << "Disp(S-): " << sumDisplacementSNeg / testTriples.size();

    double percentReductionH = 0.0;
    if (normalComparisonsH >= cons_comparisons_h) {
        percentReductionH = ((double)(normalComparisonsH - cons_comparisons_h)/ normalComparisonsH)* 100;
    } else {
        percentReductionH = ((double)(cons_comparisons_h - normalComparisonsH) / cons_comparisons_h) * -100;
    }

    double percentReductionT = 0.0;
    if (normalComparisonsT >= cons_comparisons_t) {
        percentReductionT = ((double)(normalComparisonsT - cons_comparisons_t)/ normalComparisonsT) * 100;
    } else {
        percentReductionT = ((double)(cons_comparisons_t - normalComparisonsT) / cons_comparisons_t) * -100;
    }

    float hitRateH = ((float)hitsHead / (float)(testTriples.size()/3))*100;
    float hitRateT = ((float)hitsTail / (float)(testTriples.size()/3))*100;
    LOG(INFOL) << "HitRate (H): " << hitRateH;
    LOG(INFOL) << "HitRate (T): " << hitRateT;
    LOG(INFOL) << "Percent Reduction (H): " << percentReductionH;
    LOG(INFOL) << "Percent Reduction (T): " << percentReductionT;
    LOG(INFOL) << "Mean rank (H): " << (double) subgraphRanksHead / hitsHead;
    LOG(INFOL) << "Mean rank (T): " << (double) subgraphRanksTail / hitsTail;

    if (logWriter) {
        logWriter->close();
    }
}

void SubgraphHandler::findAnswers(KB &kb,
    string embAlgo,
    string embDir,
    string subFile,
    string subAlgo,
    string nameTest,
    string formatTest,
    string answerMethod,
    uint64_t threshold,
    string writeLogs,
    DIST secondDist,
    string kFile) {
    DictMgmt *dict = kb.getDictMgmt();
    std::unique_ptr<Querier> q(kb.query());

    //Load the embeddings
    LOG(INFOL) << "Loading the embeddings ...";
    loadEmbeddings(embDir);
    //Load the subgraphs
    LOG(INFOL) << "Loading the subgraphs ...";
    faiss::Index *annIndex = NULL;
    bool useANN = false;
    if (subAlgo.find("ann") != string::npos) {
        annIndex = faiss::read_index(kFile.c_str());
        assert(annIndex != NULL);
        useANN = true;
        subAlgo = subAlgo.substr(subAlgo.find("ann") + 3);
    }
    loadSubgraphs(subFile, subAlgo);
    //Load the test file
    std::vector<uint64_t> testTriples;
    std::vector<uint64_t> testTopKs;
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
    } else if (formatTest == "dynamicK") {
        // The file is uncompressed text file with each test triple has the following format
        // h <space> r <space> t <space> K_h <space> K_t
        if (!Utils::exists(nameTest)) {
        LOG(ERRORL) << "Test file " << nameTest << " not found";
        throw 10;
    }
    std::ifstream ifs(nameTest);
    string line;
    while (std::getline(ifs, line)) {
    istringstream is(line);
    string token;
    vector<uint64_t> tokens;
    while(getline(is, token, ' ')) {
    uint64_t temp = std::stoull(token);
    tokens.push_back(temp);
    }
    LOG(DEBUGL) << tokens[0] << "  , " << tokens[1] << " , " << tokens[2];
    testTriples.push_back(tokens[0]);
    testTriples.push_back(tokens[1]);
    testTriples.push_back(tokens[2]);
    testTopKs.push_back(tokens[3]);
    testTopKs.push_back(tokens[4]);
    // push the dummy value so that the for loop
    // variable incrementation works well later
    testTopKs.push_back(-1);
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

    //Stats
    uint64_t counth = 0;
    uint64_t subgraphRanksHead = 0;
    uint64_t countt = 0;
    uint64_t subgraphRanksTail = 0;

    vector<double> allQueriesPrecisionsH;
    vector<double> allQueriesPrecisionsT;
    vector<double> allQueriesRecallsH;
    vector<double> allQueriesRecallsT;
    std::chrono::system_clock::time_point start;
    std::chrono::duration<double> duration;

    char buffer[MAX_TERM_SIZE];
    const uint64_t nents = E->getN();

    TranseTester<double> tester(E, R, kb.query());
    const uint16_t dime = E->getDim();
    std::vector<double> testArray(dime);
    std::vector<std::size_t> indices(nents);
    std::iota(indices.begin(), indices.end(), 0u);
    std::vector<std::size_t> indices2(nents);
    std::iota(indices2.begin(), indices2.end(), 0u);

    std::unique_ptr<ofstream> logWriter;

    if (writeLogs != "") {
        logWriter = std::unique_ptr<ofstream>(new ofstream());
        logWriter->open(writeLogs, std::ios_base::out);
        *logWriter.get() << "Query\tTestHead\tTestTail\tComparisonHead\tComparisonTail" << std::endl;
    }

    //Select most promising subgraphs to do the search
    LOG(INFOL) << "Test the queries ...";
    vector<uint64_t> relevantSubgraphsH;
    vector<uint64_t> relevantSubgraphsT;
    vector<double> relevantSubgraphsHDistances;
    vector<double> relevantSubgraphsTDistances;
    vector<vector<int64_t>> allActualAnswersH;
    vector<vector<uint64_t>> allExpectedAnswersH;
    vector<vector<int64_t>> allActualAnswersT;
    vector<vector<uint64_t>> allExpectedAnswersT;
    uint64_t offset = testTriples.size() / 100;
    uint16_t dim = E->getDim();
    if (offset == 0) offset = 1;
    for(uint64_t i = 0; i < testTriples.size(); i+=3) {
        uint64_t h, t, r;
        h = testTriples[i];
        r = testTriples[i + 1];
        t = testTriples[i + 2];
        float tailQueriesAnn[dim];
        float headQueriesAnn[dim];
        if (useANN) {
            int iq = i/3;
            std::unique_ptr<double> tailResultAnn =
            std::unique_ptr<double>(new double[dim]);
            std::unique_ptr<double> headResultAnn =
            std::unique_ptr<double>(new double[dim]);
            add(tailResultAnn.get(), E->get(h), R->get(r), dim);
            assert(h < E->getN() && t < E->getN());
            for (uint16_t j = 0; j < dim; ++j) {
                tailQueriesAnn[j] = (float) tailResultAnn.get()[j];
            }
            tailQueriesAnn[0] += iq / 1000.;

            sub(headResultAnn.get(), E->get(t), R->get(r), dim);
            for (uint16_t j = 0; j < dim; ++j) {
                headQueriesAnn[j] = (float) headResultAnn.get()[j];
            }
            headQueriesAnn[0] += iq / 1000.;
        }
        if (i % offset == 0) {
            LOG(INFOL) << "***Tested " << i / 3 << " of "
                << testTriples.size() / 3 << " queries";
            if (i > 0) {
                LOG(INFOL) << "***Found answers (H): " << counth << " (T): " << countt <<
                    " of " << i / 3;
                //LOG(INFOL) << "***Avg (H): " << (double) subgraphRanksHead / counth << " (T): "
                //    << (double)subgraphRanksTail / countt;
                //LOG(INFOL) << "***Comp. reduction (H) " << cons_comparisons_h <<
                //    " instead of " << nents * counth;
                //LOG(INFOL) << "***Comp. reduction (T) " << cons_comparisons_t <<
                //    " instead of " << nents * countt;
            }
        }

        dict->getText(h, buffer);
        string sh = string(buffer);
        dict->getText(t, buffer);
        string st = string(buffer);
        int size;
        dict->getTextRel(r, buffer, size);
        string sr = string(buffer, size);

        DIST distType = L1;

        // The log file for dynamicK contains a triple and the CLASS 'C' for topK
        // Following is the conversion between the class and topK value
        // C : topK
        // 0 : 1
        // 1 : 3
        // 2 : 5
        // 3 : 10
        // 4 : 50
        if (formatTest == "dynamicK") {
            switch(testTopKs[i]) {
                case 0 : threshold = 1; break;
                case 1 : threshold = 3; break;
                case 2 : threshold = 5; break;
                case 3 : threshold = 10; break;
                default: threshold = 50; break;
            }
        }

        ANSWER_METHOD ansMethod = UNION;
        if (answerMethod == "intersection") {
            ansMethod = INTERSECTION;
        } else if (answerMethod == "interunion") {
            ansMethod = INTERUNION;
        }
        selectRelevantSubGraphs(distType, q.get(), embAlgo, Subgraphs<double>::TYPE::PO,
                r, t, relevantSubgraphsH, relevantSubgraphsHDistances,threshold, subAlgo, secondDist);
        int64_t foundH = isAnswerInSubGraphs(h, relevantSubgraphsH, q.get());
        uint64_t totalSizeH = numberInstancesInSubgraphs(q.get(), relevantSubgraphsH);
        // Return all answers from the subgraphs
        vector<int64_t> actualAnswersH;
        //LOG(INFOL) << "answer method = " << answerMethod;
        getAllPossibleAnswers(q.get(), relevantSubgraphsH, actualAnswersH, ansMethod);

        vector<uint64_t> expectedAnswersH;
        getExpectedAnswersFromTest(testTriples, Subgraphs<double>::TYPE::PO, r, t, expectedAnswersH);
        if (useANN) {
            int kHead = expectedAnswersH.size();
            long nnsHead[kHead];
            float disHead[kHead];
            start = std::chrono::system_clock::now();
            annIndex->search(1, headQueriesAnn, kHead, disHead, nnsHead);
            duration = std::chrono::system_clock::now() - start;
            for (int jj = 0; jj < kHead; ++jj) {
                actualAnswersH.push_back(nnsHead[jj]);
            }
        }

        allActualAnswersH.push_back(actualAnswersH);
        allExpectedAnswersH.push_back(expectedAnswersH);
        double answerPrecisionH = 0.0;
        double answerRecallH = 0.0;
        getAnswerAccuracy(expectedAnswersH, actualAnswersH, answerPrecisionH, answerRecallH);
        allQueriesPrecisionsH.push_back(answerPrecisionH);
        allQueriesRecallsH.push_back(answerRecallH);
        if (answerRecallH >= 100) {
            //LOG(INFOL) << "Found all answers for Query: ? " << sr << " " << st;
            counth++;
            subgraphRanksHead++;
        } else if (answerRecallH > 0) {
            //LOG(INFOL) << "@@@@@@@@@@@@@@ Found some answers for query : ? " << sr << " " << st;
            subgraphRanksHead++;
        }

        if (formatTest == "dynamicK") {
            switch(testTopKs[i+1]) {
                case 0 : threshold = 1; break;
                case 1 : threshold = 3; break;
                case 2 : threshold = 5; break;
                case 3 : threshold = 10; break;
                default: threshold = 50; break;
            }
        }
        selectRelevantSubGraphs(distType, q.get(), embAlgo, Subgraphs<double>::TYPE::SP,
                r, h, relevantSubgraphsT, relevantSubgraphsTDistances, threshold, subAlgo, secondDist);
        int64_t foundT = isAnswerInSubGraphs(t, relevantSubgraphsT, q.get());
        uint64_t totalSizeT = numberInstancesInSubgraphs(q.get(), relevantSubgraphsT);
        // Return all answers from subgraphs
        vector<int64_t> actualAnswersT;
        getAllPossibleAnswers(q.get(), relevantSubgraphsT, actualAnswersT, ansMethod);

        vector<uint64_t> expectedAnswersT;
        //TODO: both expected answers H and T can be collected with a single call to this function
        getExpectedAnswersFromTest(testTriples, Subgraphs<double>::TYPE::SP, r, h, expectedAnswersT);

        if (useANN) {
            int kTail = expectedAnswersT.size();
            long nnsTail[kTail];
            float disTail[kTail];
            start = std::chrono::system_clock::now();
            annIndex->search(1, tailQueriesAnn, kTail, disTail, nnsTail);
            duration = std::chrono::system_clock::now() - start;
            //LOG(DEBUGL) << "Time to find approximate nearest neighbours (tail)= " << duration.count() * 1000 << " ms";
            vector<int64_t> actualAnswersT;
            for (int jj = 0; jj < kTail; ++jj) {
                actualAnswersT.push_back(nnsTail[jj]);
            }
        }

        allActualAnswersT.push_back(actualAnswersT);
        allExpectedAnswersT.push_back(expectedAnswersT);
        double answerPrecisionT = 0.0;
        double answerRecallT = 0.0;
        getAnswerAccuracy(expectedAnswersT, actualAnswersT, answerPrecisionT, answerRecallT);
        allQueriesPrecisionsT.push_back(answerPrecisionT);
        allQueriesRecallsT.push_back(answerRecallT);
        if (answerRecallT >= 100) {
            //LOG(INFOL) << "Found all answers for Query: " << sh << " " << sr << " ?";
            countt++;
            subgraphRanksTail++;
        } else if (answerRecallT > 0) {
            //LOG(INFOL) << "####### Found some answers for query : " << sh << " " << sr << " ?";
            subgraphRanksTail++;
        }

        //LOG(INFOL) << cntActualAnswersT << " , " << expectedAnswersT.size() << " => " << answerRecallT;

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
    double macroPrecisionH = std::accumulate(
            allQueriesPrecisionsH.begin(),
            allQueriesPrecisionsH.end(), 0.0) / allQueriesPrecisionsH.size();
    double macroPrecisionT = std::accumulate(
            allQueriesPrecisionsT.begin(),
            allQueriesPrecisionsT.end(), 0.0) / allQueriesPrecisionsT.size();
    double macroRecallH = std::accumulate(
            allQueriesRecallsH.begin(),
            allQueriesRecallsH.end(), 0.0) / allQueriesRecallsH.size();
    double macroRecallT = std::accumulate(
            allQueriesRecallsT.begin(),
            allQueriesRecallsT.end(), 0.0) / allQueriesRecallsT.size();
    double microPrecisionH = 0.0;
    double microRecallH = 0.0;
    double microPrecisionT = 0.0;
    double microRecallT = 0.0;
    LOG(INFOL) << "# entities : " << nents;
    LOG(INFOL) << "Macro Precision (Head) = " << macroPrecisionH;
    LOG(INFOL) << "Macro Recall    (Head) = " << macroRecallH;
    LOG(INFOL) << "Macro Precision (Tail) = " << macroPrecisionT;
    LOG(INFOL) << "Macro Recall    (Tail) = " << macroRecallT;
    LOG(INFOL) << "All  Head answers found for " << counth << " queries out of " << testTriples.size() / 3;
    LOG(INFOL) << "All  Tail answers found for " << countt << " queries out of " << testTriples.size() / 3;
    LOG(INFOL) << "Some Head answers found for " << subgraphRanksHead << " queries out of " << testTriples.size() / 3;
    LOG(INFOL) << "Some Tail answers found for " << subgraphRanksTail << " queries out of " << testTriples.size() / 3;
    LOG(INFOL) << "Computing overall accuracy for Head answers : ";
    getModelAccuracy(allExpectedAnswersH, allActualAnswersH, microPrecisionH, microRecallH);
    LOG(INFOL) << "Computing overall accuracy for Tail answers : ";
    getModelAccuracy(allExpectedAnswersT, allActualAnswersT, microPrecisionT, microRecallT);
    LOG(INFOL) << "Micro Precision (Head) = " << microPrecisionH;
    LOG(INFOL) << "Micro Recall    (Head) = " << microRecallH;
    LOG(INFOL) << "Micro Precision (Tail) = " << microPrecisionT;
    LOG(INFOL) << "Micro Recall    (Tail) = " << microRecallT;
    //float hitRateH = ((float)counth / (float)(testTriples.size()/3))*100;
    //float hitRateT = ((float)countt / (float)(testTriples.size()/3))*100;

    //double percentReductionH = ((double)((nents*counth) - cons_comparisons_h)/ (double)(nents * counth)) * 100;
    //double percentReductionT = ((double)((nents*countt) - cons_comparisons_t)/ (double)(nents * countt)) * 100;

    //float reductionInverseH = (float)1 / (float)percentReductionH;
    //float reductionInverseT = (float)1 /  (float)percentReductionT;
    //float f1H = (2 * reductionInverseH * hitRateH) / (reductionInverseH + hitRateH);
    //float f1T = (2 * reductionInverseT * hitRateT) / (reductionInverseT + hitRateT);
    //LOG(INFOL) << "f1H = " << f1H << " , f1T = " << f1T;

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


Subgraphs<double>::Metadata SubgraphHandler::getSubgraphMetadata(size_t idx) {
    return subgraphs->getMeta(idx);
}
