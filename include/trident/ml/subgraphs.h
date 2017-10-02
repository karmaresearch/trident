#ifndef _SUBGRAPHS_H
#define _SUBGRAPHS_H

#include <trident/ml/embeddings.h>
#include <trident/kb/querier.h>

#include <inttypes.h>
#include <vector>

template<typename K>
class Subgraphs {
    public:
        typedef enum { PO, SP } TYPE;

    private:
        struct Metadata {
            uint64_t id;
            TYPE t;
            uint64_t ent, rel;
        };

        std::vector<Metadata> subgraphs;

    public:
        void addSubgraph(const TYPE t, uint64_t ent, uint64_t rel) {
            Metadata m;
            m.id = subgraphs.size();
            m.t = t;
            m.ent = ent;
            m.rel = rel;
            subgraphs.push_back(m);
        }

        virtual void calculateEmbeddings(Querier *q,
                std::shared_ptr<Embeddings<K>> E,
                std::shared_ptr<Embeddings<K>> R) = 0;

        virtual void loadFromFile(string file) = 0;

        virtual double l1(Querier *q, uint32_t subgraphid, K *emb, uint16_t dim) {
            BOOST_LOG_TRIVIAL(error) << "Not implemented";
            throw 10;
        }

        void getDistanceToAllSubgraphs(DIST dist,
                Querier *q,
                std::vector<double> &distances,
                K* emb,
                uint16_t dim) {
            for(size_t i = 0; i < subgraphs.size(); ++i) {
            switch (dist) {
                case L1:
                    distances.push_back(l1(q, i, emb, dim));
                    break;
                default:
                    BOOST_LOG_TRIVIAL(error) << "Not implemented";
                    throw 10;
            };
            }
        }
};

template<typename K>
class CIKMSubgraphs : public Subgraphs<K> {
    private:
        std::vector<K> params;
        uint16_t dim;

    public:
        void loadFromFile(string file) {
            std::ifstream ifs;
            ifs.open(file, std::ifstream::in);
            char bdim[2];
            ifs.read(bdim, 2);
            dim = *(uint16_t*) bdim;
            const uint16_t sizeline = 17 + dim * 8;
            std::unique_ptr<char> buffer = std::unique_ptr<char>(new char[sizeline]);
            while (true) {
                ifs.read(buffer.get(), sizeline);
                if (ifs.eof()) {
                    break;
                }
                //Parse the subgraph
                uint64_t ent = *(uint64_t*)(buffer.get() + 1);
                uint64_t rel = *(uint64_t*)(buffer.get() + 9);
                if (buffer.get()[0]) {
                    this->addSubgraph(Subgraphs<K>::TYPE::SP, ent, rel);
                } else {
                    this->addSubgraph(Subgraphs<K>::TYPE::PO, ent, rel);
                }
                //Parse the features vector
                const uint8_t lenParam = sizeof(K);
                for(uint16_t i = 0; i < dim; ++i) {
                    K param = *(K*) (buffer.get() + 17 + i * lenParam); 
                    params.push_back(param);
                }
            }
        }

        void calculateEmbeddings(Querier *q,
                std::shared_ptr<Embeddings<K>> E,
                std::shared_ptr<Embeddings<K>> R) {}
};

template<typename K>
class GaussianSubgraphs : public Subgraphs<K> {
    private:
        std::vector<double> mu;
        std::vector<double> sigma;

    public:
        void calculateEmbeddings(Querier *q,
                std::shared_ptr<Embeddings<K>> E,
                std::shared_ptr<Embeddings<K>> R) {
            //TODO
        }
};

#endif
