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

        long isAnswerInSubGraphs(uint64_t a,
                const std::vector<uint64_t> &subgraphs, Querier *q);

        void selectRelevantSubGraphs(DIST dist,
                Querier *q,
                string algo,
                TYPEQUERY t, uint64_t v1, uint64_t v2,
                std::vector<uint64_t> &output,
                uint32_t topk);

        static void add(double *dest, double *v1, double *v2, uint16_t dim);

        static void sub(double *dest, double *v1, double *v2, uint16_t dim);

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
