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

       bool shouldUpdate(uint32_t idx);

    public:
        TranseLearner(KB &kb, LearnParams &p) :
            PairwiseLearner(kb, p) {
            }

        void process_batch(BatchIO &io, std::vector<uint64_t> &oneg,
                std::vector<uint64_t> &sneg);
};
#endif
