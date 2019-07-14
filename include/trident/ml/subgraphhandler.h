#ifndef _SUBGRAPH_HANDLER_H
#define _SUBGRAPH_HANDLER_H

#include <trident/ml/subgraphs.h>
#include <trident/kb/kb.h>
#include <unordered_map>
typedef enum {UNION, INTERSECTION, INTERUNION} ANSWER_METHOD;

class SubgraphHandler {
    private:
        std::shared_ptr<Embeddings<double>> E;
        std::shared_ptr<Embeddings<double>> R;
        std::shared_ptr<Subgraphs<double>> subgraphs;


        void loadBinarizedEmbeddings(string embfile, vector<double>& embeddings);

        void processBinarizedEmbeddingsDirectory(string embdir, vector<double>& emb1, vector<double>& emb2, vector<double> &e3);

    public:
        void loadEmbeddings(string embdir);

        void loadSubgraphs(string subgraphsFile,
                string subFormat, double varThreshold);

    private:

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

        int64_t isAnswerInSubGraphs(uint64_t a, const std::vector<uint64_t> &subgraphs, Querier *q);

        int64_t isTripleInSubGraphs(uint64_t h, uint64_t t, const std::vector<uint64_t> &subgs, Querier *q);
        vector<uint64_t> areTriplesInSubGraphs(vector<uint64_t> &testTriples,const std::vector<uint64_t> &subgs, Querier *q);

        void areAnswersInSubGraphs(
                vector<uint64_t> entities,
                const std::vector<uint64_t> &subgs,
                Querier *q,
                unordered_map<uint64_t, int64_t>& entityRankMap
                );

        int64_t getDynamicThreshold(
                Querier* q,
                vector<uint64_t> &validTriples,
                Subgraphs<double>::TYPE type,
                uint64_t &r,
                uint64_t &e,
                string &embAlgo,
                string &subAlgo,
                DIST secondDist,
                int64_t &subgraphThreshold,
                bool hugeKG = false
                );

        vector<uint64_t> sampleSubgraphs(vector<uint64_t>& subgs, int percent=25);

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
                vector<int64_t> &output,
                ANSWER_METHOD answerMethod
                );

        double calculateScore(uint64_t ent,
                vector<uint64_t>& subgs,
                Querier* q);

        vector<uint64_t> removeLiterals(vector<uint64_t>& triples, KB& kb);
        vector<uint64_t> removeImprobables(vector<uint64_t>& triples, Querier* q);

        void getAnswerAccuracy(vector<uint64_t> & actualEntities,
            vector<int64_t>& expectedEntities,
            double& precision,
            double& recall);

        void getModelAccuracy(vector<vector<uint64_t>> & actualEntities,
            vector<vector<int64_t>> & expectedEntities,
            double& precision,
            double& recall);

        void getExpectedAnswersFromTest(vector<uint64_t>& testTriples,
            Subgraphs<double>::TYPE type,
            uint64_t rel,
            uint64_t ent,
            vector<uint64_t> &output);

        uint64_t numberInstancesInSubgraphs(
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
                string subgraphFilter,
                int64_t threshold,
                double varThreshold,
                string writeLogs,
                DIST secondDist,
                string kFile,
                string binEmbDir,
                bool calcDisp,
                int64_t sampleTest);

        /*void findAnswers(KB &kb,
                string embAlgo,
                string embDir,
                string subFile,
                string subType,
                string nameTest,
                string formatTest,
                string answerMethod,
                uint64_t threshold,
                double varThreshold,
                string writeLogs,
                DIST secondDist);*/

        Subgraphs<double>::Metadata getSubgraphMetadata(size_t idx);

        void create(KB &kb,
                string subType,
                string embDir,
                string subFile,
                uint64_t minSubgraphSize,
                bool removeLiterals);

        void getAllSubgraphs(Querier *q);

        void selectRelevantSubGraphs(DIST dist,
                Querier *q,
                string algo,
                Subgraphs<double>::TYPE t, uint64_t v1, uint64_t v2,
                std::vector<uint64_t> &output,
                std::vector<double> &outputDistances,
                uint32_t topk,
                string &subgraphType,
                DIST secondDist
                );
};

#endif
