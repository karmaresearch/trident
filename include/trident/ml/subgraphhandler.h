#ifndef _SUBGRAPH_HANDLER_H
#define _SUBGRAPH_HANDLER_H

#include <trident/ml/subgraphs.h>

class SubgraphHandler {
    private:
        std::vector<Subgraph> subgraphs;

    public:
        SubgraphHandler() {}

        void getAllSubgraphs(Querier *q);
};

#endif
