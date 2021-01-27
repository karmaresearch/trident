#include <trident/ml/subgraphs.h>

#include <kognac/utils.h>

template<>
void AvgSubgraphs<double>::loadFromFile(string file) {
    LOG(INFOL) << "Loading from file : " << file;
    ifstream ifs(file);
    Subgraphs::loadFromFile(ifs);
    char buffer[10];
    ifs.read(buffer, 10);
    dim = Utils::decode_short(buffer);
    mincard = Utils::decode_long(buffer + 2);
    while (true) {
        double param;
        ifs.read((char*)&param, sizeof(double));
        if (ifs.eof()) {
            break;
        }
        params.push_back(param);
    }
    ifs.close();
}

template<>
void VarSubgraphs<double>::loadFromFile(string file) {
    LOG(INFOL) << "Loading from file : " << file;
    ifstream ifs(file);
    Subgraphs::loadFromFile(ifs);
    char buffer[18];
    ifs.read(buffer, 18);
    dim = Utils::decode_short(buffer);
    mincard = Utils::decode_long(buffer + 2);
    long nParams = Utils::decode_long(buffer + 10);
    for (uint32_t i = 0; i < nParams; ++i ) {
        double param;
        ifs.read((char*)&param, sizeof(double));
        if (ifs.eof()) {
            // Must not end here
            assert(false);
        }
        params.push_back(param);
    }
    while(true) {
        double var;
        ifs.read((char*)&var, sizeof(double));
        if (ifs.eof()) {
            break;
        }
        variances.push_back(var);
    }
    assert(params.size() == variances.size());
    ifs.close();
}

template<>
void AvgSubgraphs<double>::storeToFile(string file) {
    ofstream ofs(file);
    Subgraphs::storeToFile(ofs);
    char buffer[10];
    Utils::encode_short(buffer, dim);
    Utils::encode_long(buffer + 2, mincard);
    ofs.write(buffer, 10);
    for(uint64_t i = 0; i < params.size(); ++i) {
        double param = params[i];
        ofs.write((char*)&param, sizeof(double));
    }
    ofs.close();
}

template<>
void VarSubgraphs<double>::storeToFile(string file) {
    ofstream ofs(file);
    Subgraphs::storeToFile(ofs);
    long nParams = params.size();
    char buffer[18];
    Utils::encode_short(buffer, dim);
    Utils::encode_long(buffer + 2, mincard);
    Utils::encode_long(buffer + 10, nParams);
    ofs.write(buffer, 18);
    assert(params.size() == variances.size());
    for(uint64_t i = 0; i < params.size(); ++i) {
        double param = params[i];
        ofs.write((char*)&param, sizeof(double));
    }
    for(uint64_t i = 0; i < variances.size(); ++i) {
        double var = variances[i];
        ofs.write((char*)&var, sizeof(double));
    }
    ofs.close();
}

template<>
void AvgSubgraphs<double>::processItr(Querier *q,
        PairItr *itr,
        Subgraphs<double>::TYPE typ,
        std::shared_ptr<Embeddings<double>> E) {
    std::vector<double> current_s;
    current_s.resize(dim);
    for(uint16_t i = 0; i < dim; ++i) {
        current_s[i] = 0.0;
    }

    //DictMgmt *dict = q->getDictMgmt();
    //char buffer[MAX_TERM_SIZE];
    //int size = 0;

    LOG(DEBUGL) << "Min card " << mincard;


    int64_t count = 0;
    int64_t prevo = -1;
    int64_t prevp = -1;
    int64_t counter = 0;
    while (itr->hasNext()) {
        itr->next();
        uint64_t o = itr->getKey();
        uint64_t p = itr->getValue1();
        uint64_t s = itr->getValue2();

        /*
        While o and p are both same,
        keep adding the embeddings of 's'
        when any one of them (either p or o) change
        then, create a subgraph
        */
        if (o != prevo || p != prevp) {
            if (count > mincard) {
                //Add the averaged embedding
                for(uint16_t i = 0; i < dim; ++i) {
                    params.push_back(current_s[i] / count);
                }
                //Add metadata about the subgraph
                addSubgraph(typ, prevo, prevp, count);
            }
            count = 0;
            prevo = o;
            prevp = p;
            for(uint16_t i = 0; i < dim; ++i) {
                current_s[i] = 0.0;
            }
        }
        count++;
        double *e = E->get(s);
        for(uint16_t i = 0; i < dim; ++i) {
            current_s[i] += e[i];
        }
	if (counter++ % 10000000 == 0) {
		LOG(DEBUGL) << "Processed records " << counter << " " << getNSubgraphs();
	}
    }
    if (count > mincard) {
        //Add the averaged embedding
        for(uint16_t i = 0; i < dim; ++i) {
            params.push_back(current_s[i] / count);
        }
        //Add metadata about the subgraph
        addSubgraph(typ, prevo, prevp, count);
    }
}

template<>
void AvgSubgraphs<double>::calculateEmbeddings(Querier *q,
        std::shared_ptr<Embeddings<double>> E,
        std::shared_ptr<Embeddings<double>> R) {
    const uint16_t dim = E->getDim();
    this->dim = dim;

    LOG(INFOL) << "Creating OPS embeddings";
    auto itr = q->getIterator(IDX_OPS, -1, -1, -1);
    processItr(q, itr, Subgraphs<double>::TYPE::PO, E);
    q->releaseItr(itr);

    LOG(INFOL) << "Creating SPO embeddings";
    itr = q->getIterator(IDX_SPO, -1, -1, -1);
    processItr(q, itr, Subgraphs<double>::TYPE::SP, E);
    q->releaseItr(itr);
    LOG(INFOL) << "Done. Added subgraphs=" << getNSubgraphs();
}

template<>
void VarSubgraphs<double>::processItr(Querier *q,
        PairItr *itr,
        Subgraphs<double>::TYPE typ,
        std::shared_ptr<Embeddings<double>> E) {
    std::vector<double> current_s;
    current_s.resize(dim);
    for(uint16_t i = 0; i < dim; ++i) {
        current_s[i] = 0.0;
    }

    //DictMgmt *dict = q->getDictMgmt();
    //char buffer[MAX_TERM_SIZE];
    //int size = 0;
    std::vector<uint64_t> subjects;

    int64_t count = 0;
    int64_t prevo = -1;
    int64_t prevp = -1;
    while (itr->hasNext()) {
        itr->next();
        uint64_t o = itr->getKey();
        uint64_t p = itr->getValue1();
        uint64_t s = itr->getValue2();

        if (o != prevo || p != prevp) {
            if (count > mincard) {
                //Add the variance embedding
                for(uint16_t i = 0; i < dim; ++i) {
                    double columnSquareDiffs = 0.0;
                    double mean = current_s[i] / count;
                    for (auto sub: subjects) {
                        double *emb = E->get(sub);
                        columnSquareDiffs += ((emb[i] - mean) * (emb[i] - mean));
                    }
                    double var = 0.0;
                    if (count <= 1) {
                        var = columnSquareDiffs;
                    } else {
                        var = columnSquareDiffs / (count-1);
                    }
                    params.push_back(mean);
                    variances.push_back(var);
                }
                //Add metadata about the subgraph
                addSubgraph(typ, prevo, prevp, count);
            }
            count = 0;
            prevo = o;
            prevp = p;
            for(uint16_t i = 0; i < dim; ++i) {
                current_s[i] = 0.0;
            }
            vector<uint64_t>().swap(subjects);
        }
        count++;
        double *e = E->get(s);
        for(uint16_t i = 0; i < dim; ++i) {
            current_s[i] += e[i];
        }
        subjects.push_back(s);
    }
    if (count > mincard) {
        //Add the averaged embedding
        for(uint16_t i = 0; i < dim; ++i) {
            double columnSquareDiffs = 0.0;
            double mean = current_s[i] / count;
            for (auto sub : subjects) {
                double *emb = E->get(sub);
                columnSquareDiffs += ((emb[i] - mean) * (emb[i] - mean));
            }
            double var = 0.0;
            if (count <= 1) {
                var = columnSquareDiffs;
            } else {
                var = columnSquareDiffs / (count-1);
            }
            params.push_back(mean);
            variances.push_back(var);
        }
        //Add metadata about the subgraph
        addSubgraph(typ, prevo, prevp, count);
    }
}
