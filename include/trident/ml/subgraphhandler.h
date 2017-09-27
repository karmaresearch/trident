#ifndef _SUBGRAPH_HANDLER_H
#define _SUBGRAPH_HANDLER_H

#include <trident/ml/subgraphs.h>
#include <trident/kb/kb.h>

class SubgraphHandler {
    private:
        enum TYPEQUERY { PO, SP };
        std::shared_ptr<Embeddings<double>> E;
        std::shared_ptr<Embeddings<double>> R;
        std::shared_ptr<Subgraphs<double>> subgraphs;

        void loadEmbeddings(string embdir);

        void loadSubgraphs(string subgraphsFile, string subFormat);

        bool isAnswerInSubGraphs(uint64_t a,
                const std::vector<uint64_t> &subgraphs, Querier *q);

        void selectRelevantSubGraphs(TYPEQUERY t, uint64_t v1, uint64_t v2,
                std::vector<uint64_t> &output);

    public:
        SubgraphHandler() {}

        void evaluate(KB &kb,
                string algo,
                string embdir,
                string subfile,
                string subformat,
                string nametest,
                string format);

        void getAllSubgraphs(Querier *q);
};

#endif
