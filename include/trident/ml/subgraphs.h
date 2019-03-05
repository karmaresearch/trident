#ifndef _SUBGRAPHS_H
#define _SUBGRAPHS_H

#include <trident/ml/embeddings.h>
#include <trident/kb/querier.h>

#include <kognac/logs.h>

#include <inttypes.h>
#include <vector>

template<typename K>
class Subgraphs {
    public:
        typedef enum { PO, SP } TYPE;

        struct Metadata {
            uint64_t id;
            TYPE t;
            uint64_t ent, rel;
            uint64_t size;
        };

    private:
        std::vector<Metadata> subgraphs;

    protected:
        void loadFromFile(std::ifstream &ifile) {
            const uint16_t sizeline = 25;
            std::unique_ptr<char> buffer = std::unique_ptr<char>(new char[sizeline]);
            ifile.read(buffer.get(), 8);
            uint64_t nsubs = *(uint64_t*)buffer.get();
            uint64_t i = 0;
            while (i < nsubs) {
                ifile.read(buffer.get(), sizeline);
                uint64_t ent = *(uint64_t*)(buffer.get() + 1);
                uint64_t rel = *(uint64_t*)(buffer.get() + 9);
                uint64_t size = *(uint64_t*)(buffer.get() + 17);
                if (buffer.get()[0]) {
                    this->addSubgraph(Subgraphs<K>::TYPE::SP, ent, rel, size);
                } else {
                    this->addSubgraph(Subgraphs<K>::TYPE::PO, ent, rel, size);
                }
                i++;
            }
        }

        void storeToFile(std::ofstream &ofile) {
            const uint16_t sizeline = 25;
            std::unique_ptr<char> buffer = std::unique_ptr<char>(new char[sizeline]);
            (*(uint64_t*)(buffer.get())) = subgraphs.size();
            ofile.write(buffer.get(), 8);
            for(auto &el : subgraphs) {
                if (el.t == PO) {
                    buffer.get()[0] = 0;
                } else {
                    buffer.get()[0] = 1;
                }
                (*(uint64_t*)(buffer.get() + 1)) = el.ent;
                (*(uint64_t*)(buffer.get() + 9)) = el.rel;
                (*(uint64_t*)(buffer.get() + 17)) = el.size;
                ofile.write(buffer.get(), sizeline);
            }
        }

    public:
        void addSubgraph(const TYPE t, uint64_t ent, uint64_t rel,
                uint64_t size) {
            Metadata m;
            m.id = subgraphs.size();
            m.t = t;
            m.ent = ent;
            m.rel = rel;
            m.size = size;
            subgraphs.push_back(m);
        }

        virtual void calculateEmbeddings(Querier *q,
                std::shared_ptr<Embeddings<K>> E,
                std::shared_ptr<Embeddings<K>> R) = 0;

        virtual void loadFromFile(string file) = 0;

        virtual void storeToFile(string file) {
            LOG(ERRORL) << "Not implemented";
            throw 10;
        }

        virtual double l1(Querier *q, uint32_t subgraphid, K *emb, uint16_t dim) {
            LOG(ERRORL) << "Not implemented";
            throw 10;
        }

        Metadata &getMeta(uint64_t subgraphid) {
            return subgraphs[subgraphid];
        }

        uint64_t getNSubgraphs() const {
            return subgraphs.size();
        }

        void getDistanceToAllSubgraphs(DIST dist,
                Querier *q,
                std::vector<std::pair<double, uint64_t>> &distances,
                K* emb,
                uint16_t dim,
                Subgraphs::TYPE excludeType,
                uint64_t excludeRel,
                uint64_t excludeEnt) {
            for(size_t i = 0; i < subgraphs.size(); ++i) {
                if (subgraphs[i].t == excludeType &&
                    subgraphs[i].rel == excludeRel &&
                    subgraphs[i].ent == excludeEnt) {
                    continue;
                }
                switch (dist) {
                    case L1:
                        distances.push_back(make_pair(l1(q, i, emb, dim), i));
                        break;
                    default:
                        LOG(ERRORL) << "Not implemented";
                        throw 10;
                };
            }
        }

        virtual ~Subgraphs() {}
};

template<typename K>
class AvgSubgraphs : public Subgraphs<K> {
    private:
        std::vector<K> params;
        uint16_t dim;
        uint64_t mincard;

        void processItr(Querier *q, PairItr *itr, Subgraphs<double>::TYPE typ,
                std::shared_ptr<Embeddings<double>> E);

    public:
        AvgSubgraphs() : dim(0), mincard(0) {}

        AvgSubgraphs(uint16_t dim, uint64_t mincard) : dim(dim), mincard(mincard) {}

	AvgSubgraphs(uint64_t mincard) : dim(0), mincard(mincard) {}

        void loadFromFile(string file);

        double l1(Querier *q, uint32_t subgraphid, K *emb, uint16_t dim) {
            double out = 0;
            for(uint16_t i = 0; i < dim; ++i) {
                out += abs(emb[i] - params[dim * subgraphid + i]);
            }
            return out;
        }

        void storeToFile(string file);

        void calculateEmbeddings(Querier *q,
                std::shared_ptr<Embeddings<K>> E,
                std::shared_ptr<Embeddings<K>> R);
};

template<typename K>
class GaussianSubgraphs : public Subgraphs<K> {
    private:
        std::vector<double> mu;
        std::vector<double> sigma;

    public:
        void calculateEmbeddings(Querier *q,
                std::shared_ptr<Embeddings<K>> E,
                std::shared_ptr<Embeddings<K>> R);
};

#endif
