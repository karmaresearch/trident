#ifndef _FAISS_WRAPPER_H
#define _FAISS_WRAPPER_H

#include <trident/ml/subgraphhandler.h>
#include <faiss/IndexPQ.h>
#include <faiss/IndexIVFPQ.h>
#include <faiss/IndexFlat.h>
#include <faiss/IndexIVFFlat.h>
#include <faiss/index_io.h>

class FaissWrapper{
    private:
        std::shared_ptr<Embeddings<double>> E;
        std::shared_ptr<Embeddings<double>> R;
        std::shared_ptr<Subgraphs<double>> subgraphs;
        faiss::Index *annIndex = NULL;

    public:
        void loadEmbeddings(string embdir);

        void loadSubgraphs(string subgraphsFile, string subFormat);

    private:

        static void add(double *dest, double *v1, double *v2, uint16_t dim);

        static void sub(double *dest, double *v1, double *v2, uint16_t dim);

    public:
        FaissWrapper() {}

        void evaluate(KB &kb,
                string embAlgo,
                string embDir,
                string subFile,
                string subAlgo,
                string nameTest,
                int64_t threshold,
                string subgraphFilter,
                DIST secondDist,
                string kFile);

        void create(string embDir, string subfile);

        void nearestNeighbours(
            Querier* q,
            int iq,
            string embAlgo,
            string subAlgo,
            int64_t threshold,
            DIST secondDist,
            ANSWER_METHOD ansMethod,
            Subgraphs<double>::TYPE type,
            uint64_t ent,
            uint64_t rel,
            SubgraphHandler &sh,
            vector<int64_t>& actualAnswers,
            int& k,
            double& searchTime);

        std::shared_ptr<Embeddings<double>> getR() {
            return R;
        }

        std::shared_ptr<Embeddings<double>> getE() {
            return E;
        }
};

#endif
