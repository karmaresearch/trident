#ifndef _SUBGRAPHS_H
#define _SUBGRAPHS_H

#include <trident/ml/embeddings.h>
#include <trident/kb/querier.h>

#include <kognac/logs.h>

#include <inttypes.h>
#include <vector>
#include <cmath>

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

    protected:
        double alpha = 0.75;
        std::vector<Metadata> subgraphs;
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
                std::shared_ptr<Embeddings<K>> R,
                bool removeLiterals = false) = 0;

        virtual void loadFromFile(string file) = 0;

        virtual void storeToFile(string file) {
            LOG(ERRORL) << "Not implemented";
            throw 10;
        }

        virtual double l1(Querier *q, uint32_t subgraphid, K *emb, uint16_t dim) {
            LOG(ERRORL) << "Not implemented";
            throw 10;
        }

        virtual double l3(Querier *q, uint32_t subgraphid, K *emb, uint16_t dim) {
            LOG(ERRORL) << "Not implemented";
            throw 10;
        }

        virtual double l3Div(Querier *q, uint32_t subgraphid, K *emb, uint16_t dim) {
            LOG(ERRORL) << "Not implemented";
            throw 10;
        }

        virtual double l3NoSquare(Querier *q, uint32_t subgraphid, K *emb, uint16_t dim) {
            LOG(ERRORL) << "Not implemented";
            throw 10;
        }

        virtual double kl(Querier *q, uint32_t subgraphid, K *emb, uint16_t dim, vector<double>& trueAverages, vector<double>& trueVar) {
            LOG(ERRORL) << "Not implemented";
            throw 10;
        }

        Metadata &getMeta(uint64_t subgraphid) {
            return subgraphs[subgraphid];
        }

        uint64_t getNSubgraphs() const {
            return subgraphs.size();
        }

        void computeAV(vector<double*>& te, vector<double>& avg, vector<double>& var, uint16_t dim) {
            avg.resize(dim);
            var.resize(dim);
            for (uint16_t i = 0; i < dim; ++i) {
                avg[i] = 0;
                var[i] = 0;
            }
            uint32_t cntTrueEmb = te.size();
            // Calculate average
            for (auto e: te) {
                for (uint32_t i = 0; i < dim; ++i) {
                    avg[i] += e[i];
                }
            }
            for (uint32_t i = 0; i < dim; ++i) {
                avg[i] /= cntTrueEmb;
            }
            // Calculate variance
            for (auto e: te) {
                for (uint32_t i = 0; i < dim; ++i) {
                    var[i] += ((e[i] - avg[i]) * (e[i] - avg[i]));
                }
            }
            for (uint32_t i = 0; i < dim; ++i) {
                if (cntTrueEmb > 2) {
                    var[i] /= (cntTrueEmb-1);
                }
            }
        }

        void getDistanceToAllSubgraphs(DIST dist,
                Querier *q,
                std::vector<std::pair<double, uint64_t>> &distances,
                K* emb,
                uint16_t dim,
                Subgraphs::TYPE excludeType,
                uint64_t excludeRel,
                uint64_t excludeEnt,
                std::shared_ptr<Embeddings<double>> E) {
                vector<double> trueAverages;
                vector<double> trueVariances;

                std::chrono::system_clock::time_point start;
                std::chrono::duration<double> duration;
                if (dist == KL) {
                    start = std::chrono::system_clock::now();
                    if (excludeType == Subgraphs<double>::TYPE::SP) {
                        auto itr = q->getPermuted(IDX_SPO, excludeEnt, excludeRel, -1, true);
                        uint64_t countResultsO = 0;
                        vector<double*> trueEmbeddings;
                        while(itr->hasNext()) {
                            int64_t key = itr->getKey();
                            int64_t entity1 = itr->getValue1();
                            int64_t entity2 = itr->getValue2();
                            itr->next();
                            if (entity2 != -1){
                                trueEmbeddings.push_back(E->get(entity2));
                            }
                            countResultsO++;
                            if (countResultsO == 10) {
                                break;
                            }
                        }
                        computeAV(trueEmbeddings, trueAverages, trueVariances, dim);
                    } else {
                        auto itr = q->getPermuted(IDX_POS, excludeRel, excludeEnt, -1, true);
                        uint64_t countResultsS = 0;
                        vector<double*> trueEmbeddings;
                        while (itr->hasNext()) {
                            int64_t entity1 = itr->getValue1();
                            int64_t entity2 = itr->getValue2();
                            int64_t key = itr->getKey();
                            itr->next();
                            if (entity2 != -1) {
                                trueEmbeddings.push_back(E->get(entity2));
                            }
                            countResultsS++;
                            if (countResultsS == 10) {
                                break;
                            }
                        }
                        computeAV(trueEmbeddings, trueAverages, trueVariances, dim);
                    }
                }
                for(size_t i = 0; i < subgraphs.size(); ++i) {
                    if (subgraphs[i].t == excludeType &&
                        subgraphs[i].rel == excludeRel &&
                        subgraphs[i].ent == excludeEnt) {
                        continue;
                    }
                    switch (dist) {
                        case L1:
                            start = std::chrono::system_clock::now();
                            distances.push_back(make_pair(l1(q, i, emb, dim), i));
                            duration = std::chrono::system_clock::now() - start;
                            LOG(DEBUGL) << "L1 distance time = " << duration.count() * 1000000 << " us";
                            break;
                        case L3:
                            start = std::chrono::system_clock::now();
                            distances.push_back(make_pair(l3(q, i, emb, dim), i));
                            duration = std::chrono::system_clock::now() - start;
                            LOG(DEBUGL) << "Var1 distance time = " << duration.count() * 1000000 << " us";
                            break;
                        case L4:
                            distances.push_back(make_pair(l3NoSquare(q, i, emb, dim),i));
                            break;
                        case L5:
                            start = std::chrono::system_clock::now();
                            distances.push_back(make_pair(l3Div(q, i, emb, dim),i));
                            duration = std::chrono::system_clock::now() - start;
                            LOG(DEBUGL) << "Var2 distance time = " << duration.count() * 1000000 << " us";
                            break;
                        case KL:
                            distances.push_back(make_pair(kl(q, i, emb, dim, trueAverages, trueVariances), i));
                            duration = std::chrono::system_clock::now() - start;
                            LOG(DEBUGL) << "KL distance time = " << duration.count() * 1000000 << " us";
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
    protected:
        std::vector<K> params;
        uint16_t dim;
        uint64_t mincard;


    public:
        AvgSubgraphs() : dim(0), mincard(0) {}

        AvgSubgraphs(uint16_t dim, uint64_t mincard) : dim(dim), mincard(mincard) {}

        virtual void loadFromFile(string file);

        virtual void processItr(Querier *q, PairItr *itr, Subgraphs<double>::TYPE typ,
                std::shared_ptr<Embeddings<double>> E, bool removeLiterals = false);

        double l1(Querier *q, uint32_t subgraphid, K *emb, uint16_t dim) {
            double out = 0;
            for(uint16_t i = 0; i < dim; ++i) {
                out += abs(emb[i] - params[dim * subgraphid + i]);
            }
            return out;
        }

        virtual void storeToFile(string file);

        void calculateEmbeddings(Querier *q,
                std::shared_ptr<Embeddings<K>> E,
                std::shared_ptr<Embeddings<K>> R,
                bool removeLiterals = false);
};

template<typename K>
class VarSubgraphs : public AvgSubgraphs<K> {
    private:
        std::vector<K> variances;
    public:
        VarSubgraphs(double alpha = 0.75) : AvgSubgraphs<K>() {
            this->alpha = alpha;
        }

        VarSubgraphs(uint16_t dim, uint64_t mincard) : AvgSubgraphs<K>(dim, mincard) {}

        void processItr(Querier *q, PairItr *itr, Subgraphs<double>::TYPE typ,
                std::shared_ptr<Embeddings<double>> E, bool removeLiterals = false);

        double l3(Querier *q, uint32_t subgraphid, K *emb, uint16_t dim) {
            double distance = 0.0;
            for(uint16_t i = 0; i < dim; ++i) {
                double diff = (emb[i] - this->params[dim * subgraphid + i]) * (emb[i] - this->params[dim * subgraphid + i]);
                double varDiff = abs(variances[dim * subgraphid + i] - diff);
                distance += varDiff;
            }
            return distance;
        }

        double l3NoSquare(Querier *q, uint32_t subgraphid, K *emb, uint16_t dim) {
            double distance = 0.0;
            for(uint16_t i = 0; i < dim; ++i) {
                double diff = abs(emb[i] - this->params[dim * subgraphid + i]);
                double varDiff = variances[dim * subgraphid + i] - diff;
                distance += varDiff;
            }
            return distance;
        }

        double l3Div(Querier *q, uint32_t subgraphid, K *emb, uint16_t dim) {
            double distance = 0.0;
            for(uint16_t i = 0; i < dim; ++i) {
                double diff = abs(emb[i] - this->params[dim * subgraphid + i]);// * abs(emb[i] - this->params[dim * subgraphid + i]);
                double div = diff;
                if (variances[dim * subgraphid + i] != 0.0) {
                    div = diff / variances[dim * subgraphid + i];
                }
                distance += div;
            }
            return distance;
        }

        double kl(Querier *q, uint32_t subgraphid, K *emb, uint16_t dim, vector<double> &trueAverages, vector<double>& trueVariances) {
            double distance = 0.0;
            for (uint16_t i = 0; i < dim; ++i) {
                double avg = this->params[dim * subgraphid + i];
                double var = this->variances[dim * subgraphid + i];
                double avgT = trueAverages[i];
                double varT = trueVariances[i];
                //https://stats.stackexchange.com/questions/7440/kl-divergence-between-two-univariate-gaussians
                double klDiv = (((avgT - avg) * (avgT - avg) + (varT * varT) ) / (2 * var * var)) + (log(sqrt(var)/sqrt(varT))) - 0.5;
                distance += klDiv;
            }
            return distance;
        }

        void loadFromFile(string file);
        void storeToFile(string file);
};

#endif
