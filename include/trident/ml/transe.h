#ifndef _TRANSE_H
#define _TRANSE_H

#include <trident/ml/pairwiselearner.h>

class TranseLearner : public PairwiseLearner {
    private:
       void update_gradient_matrix(std::vector<EntityGradient> &gm,
                std::vector<std::unique_ptr<float>> &signmatrix,
                std::vector<uint32_t> &inputTripleID,
                std::vector<uint64_t> &inputTerms,
                int pos, int neg);

    public:
        TranseLearner(KB &kb, const uint16_t epochs, const uint32_t ne,
                const uint32_t nr,
                const uint16_t dim, const float margin, const float learningrate,
                const uint16_t batchsize, const bool adagrad) :
            PairwiseLearner(kb, epochs, ne, nr, dim, margin, learningrate,
                    batchsize, adagrad) {
            }

        void process_batch(BatchIO &io, std::vector<uint64_t> &oneg,
                std::vector<uint64_t> &sneg);
};
#endif
