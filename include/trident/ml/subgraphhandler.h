#ifndef _SUBGRAPH_HANDLER_H
#define _SUBGRAPH_HANDLER_H

#include <trident/ml/subgraphs.h>
#include <trident/kb/kb.h>

class SubgraphHandler {
    private:
        std::shared_ptr<Embeddings<double>> E;
        std::shared_ptr<Embeddings<double>> R;
        std::shared_ptr<Subgraphs<double>> subgraphs;

        void loadEmbeddings(string embdir);

        void loadBinarizedEmbeddings(string embfile, vector<double>& embeddings);

        void processBinarizedEmbeddingsDirectory(string embdir, vector<double>& emb1, vector<double>& emb2, vector<double> &e3);

        void loadSubgraphs(string subgraphsFile,
                string subFormat, double varThreshold);

        template<typename K>
            void getDisplacement(K &tester,
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
                    );

        int64_t isAnswerInSubGraphs(uint64_t a,
                const std::vector<uint64_t> &subgraphs, Querier *q);

        void selectRelevantSubGraphs(DIST dist,
                Querier *q,
                string algo,
                Subgraphs<double>::TYPE t, uint64_t v1, uint64_t v2,
                std::vector<uint64_t> &output,
                uint32_t topk,
                string &subgraphType,
                DIST secondDist
                );

        void selectRelevantBinarySubgraphs(
                Subgraphs<double>::TYPE t, uint64_t v1, uint64_t v2,
                uint32_t topk,
                vector<double>& subEmb,
                vector<double>& entEmb,
                vector<double>& relEmb,
                std::vector<uint64_t> &output
                );

        void getAllPossibleAnswers(Querier *q,
                vector<uint64_t> &relevantSubgraphs,
                Subgraphs<double>::TYPE t,
                vector<int64_t> &output
                );

        void getAnswerAccuracy(vector<uint64_t> & actualEntities,
            vector<int64_t>& expectedEntities,
            double& accuracy);

        void getActualAnswersFromTest(vector<uint64_t>& testTriples,
            Subgraphs<double>::TYPE type,
            uint64_t rel,
            uint64_t ent,
            vector<uint64_t> &output);

        int64_t numberInstancesInSubgraphs(
                Querier *q,
                const std::vector<uint64_t> &subgs);

        static void add(double *dest, double *v1, double *v2, uint16_t dim);

        static void sub(double *dest, double *v1, double *v2, uint16_t dim);

    public:
        SubgraphHandler() {}

        void evaluate(KB &kb,
                string embAlgo,
                string embDir,
                string subFile,
                string subType,
                string nameTest,
                string formatTest,
                uint64_t threshold,
                double varThreshold,
                string writeLogs,
                DIST secondDist,
                string kFile,
                string binEmbDir);

        void findAnswers(KB &kb,
                string embAlgo,
                string embDir,
                string subFile,
                string subType,
                string nameTest,
                string formatTest,
                uint64_t threshold,
                double varThreshold,
                string writeLogs,
                DIST secondDist);

        void create(KB &kb,
                string subType,
                string embDir,
                string subFile,
                uint64_t minSubgraphSize);

        void getAllSubgraphs(Querier *q);
};

#endif
