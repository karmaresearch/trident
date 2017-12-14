#ifndef _DISTMUL_H
#define _DISTMUL_H

#include <trident/ml/learner.h>

#include <cmath>

class DistMulLearner: public Learner {
    private:
        const uint64_t numneg;

        /*static float e_score(float h, float r, float t) {
            return exp(h * t * r);
        }*/

        void update_gradient_matrix(std::vector<EntityGradient> &gm,
                std::vector<std::unique_ptr<float>> &gradmatrix,
                std::vector<uint16_t> &inputTripleID,
                std::vector<uint64_t> &inputTerms);

        float softmax(double *h, double *r, double *t, uint16_t dim, int so,
                std::vector<double*> &negs);

        void getRandomEntities(uint16_t n, std::vector<double*> &negs);

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
