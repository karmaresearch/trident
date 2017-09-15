#ifndef _SUBGRAPHS_H
#define _SUBGRAPHS_H

#include <trident/ml/embeddings.h>
#include <trident/kb/querier.h>

#include <inttypes.h>
#include <vector>

template<typename K>
class Subgraph {
    public:
        typedef enum { PO, SP } TYPE;

        TYPE t;
        uint64_t ent, rel;
        std::vector<double> mu;
        std::vector<double> sigma;

        Subgraph(TYPE t, uint64_t ent, uint64_t rel) :
            t(t),
            ent(ent),
            rel(rel) {
            }

        void calculateEmbeddings(
                Querier *q,
                std::shared_ptr<Embeddings<K>> E,
                std::shared_ptr<Embeddings<K>> R) {
            //TODO
        }
};

#endif
