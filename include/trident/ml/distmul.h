#ifndef _DISTMUL_H
#define _DISTMUL_H

#include <trident/ml/learner.h>

#include <cmath>

class DistMulLearner: public Learner {
    private:
        const uint64_t numneg;
        std::random_device rd;
        std::mt19937 gen;

        void update_gradient_matrix(std::vector<EntityGradient> &gm,
                std::vector<std::unique_ptr<double>> &gradmatrix,
                std::vector<uint16_t> &inputTripleID,
                std::vector<uint64_t> &inputTerms);

        double softmax(double *h, double *r, double *t, uint16_t dim, int so,
                std::vector<double*> &negs);

        double escore(double *h, double *r, double *t, uint16_t dim);

        double derNeg_r(int idx, double *h, double *r, double *t, uint16_t dim,
                int so, std::vector<double*> &negs);

        double derNeg_t(int idx, double *h, double *r, double *t, uint16_t dim,
                std::vector<double*> &negs);

        double derNeg_h(int idx, double *h, double *r, double *t, uint16_t dim,
                std::vector<double*> &negs);

        double sumNeg(double *h, double *r, double *t, uint16_t dim,
                int so, std::vector<double*> &negs);

        double loss(double *h, double *r, double *t, uint16_t dim,
                std::vector<double*> &negs1,
                std::vector<double*> &negs2);

        double loss(std::vector<uint64_t> &output1,
                std::vector<uint64_t> &output2,
                std::vector<uint64_t> &output3,
                uint16_t dim,
                std::vector<uint64_t> &negativeHeadEntities,
                std::vector<uint64_t> &negativeTailEntities);

        void getRandomEntities(uint16_t n, std::vector<double*> &negs,
                std::vector<uint64_t> &entities);

    public:
        DistMulLearner(KB &kb, LearnParams &p) :
            Learner(kb, p), numneg(p.numneg) { }

        void process_batch(BatchIO &io, const uint32_t epoch, const uint16_t
                nbatches);

        std::string getName() {
            return "Distmul";
        }
};


#endif
