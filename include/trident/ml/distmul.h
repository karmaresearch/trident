#ifndef _DISTMUL_H
#define _DISTMUL_H

#include <trident/ml/learner.h>
#include <cmath>

class DistMulLearner: public Learner {
    private:
        const uint64_t numneg;

        static float e_score(float h, float r, float t) {
            return exp(h * t * r);
        }

        void update_gradient_matrix(std::vector<EntityGradient> &gm,
                std::vector<std::unique_ptr<float>> &gradmatrix,
                std::vector<uint16_t> &inputTripleID,
                std::vector<uint64_t> &inputTerms);

    public:
        DistMulLearner(KB &kb, const uint16_t epochs, const uint32_t ne,
                const uint32_t nr,
                const uint16_t dim, const float margin, const float learningrate,
                const uint16_t batchsize, const bool adagrad,
                const uint64_t numneg) :
            Learner(kb, epochs, ne, nr, dim, margin, learningrate, batchsize,
                    adagrad), numneg(numneg) { }


        void process_batch(BatchIO &io, const uint16_t epoch, const uint16_t
                nbatches);
};


#endif
