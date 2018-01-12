#include <trident/ml/subgraphs.h>

#include <kognac/utils.h>

template<>
void AvgSubgraphs<double>::loadFromFile(string file) {
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
void AvgSubgraphs<double>::processItr(PairItr *itr,
        Subgraphs<double>::TYPE typ,
        std::shared_ptr<Embeddings<double>> E) {
    std::vector<double> current_s;
    current_s.resize(dim);
    for(uint16_t i = 0; i < dim; ++i) {
        current_s[i] = 0.0;
    }

    long count = 0;
    long prevo = -1;
    long prevp = -1;
    while (itr->hasNext()) {
        itr->next();
        uint64_t o = itr->getKey();
        uint64_t p = itr->getValue1();
        uint64_t s = itr->getValue2();
        if (o != prevo || p != prevp) {
            if (count > mincard) {
                //Add the averaged embedding
                for(uint16_t i = 0; i < dim; ++i) {
                    params.push_back(current_s[i] / count);
                }
                //Add metadata about the subgraph
                addSubgraph(typ, prevo, prevp);
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
    }
    if (count > mincard) {
        //Add the averaged embedding
        for(uint16_t i = 0; i < dim; ++i) {
            params.push_back(current_s[i] / count);
        }
        //Add metadata about the subgraph
        addSubgraph(typ, prevo, prevp);
    }
}

template<>
void AvgSubgraphs<double>::calculateEmbeddings(Querier *q,
        std::shared_ptr<Embeddings<double>> E,
        std::shared_ptr<Embeddings<double>> R) {
    //DictMgmt *dict = q->getDictMgmt();
    //char buffer[MAX_TERM_SIZE];
    /*int size = 0;
      dict->getText(o, buffer, size);
      std::cout << string(buffer,size) << " ";
      dict->getTextRel(p, buffer, size);
      std::cout << string(buffer,size) << " ";
      dict->getText(s, buffer, size);
      std::cout << string(buffer,size) << std::endl;*/
    const uint16_t dim = E->getDim();
    this->dim = dim;

    auto itr = q->get(IDX_OPS, -1, -1, -1);
    processItr(itr, Subgraphs<double>::TYPE::PO, E);
    q->releaseItr(itr);

    itr = q->get(IDX_SPO, -1, -1, -1);
    processItr(itr, Subgraphs<double>::TYPE::SP, E);
    q->releaseItr(itr);
}
