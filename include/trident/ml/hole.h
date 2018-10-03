#ifndef _HOLE_H
#define _HOLE_H

#include <trident/ml/pairwiselearner.h>

class HoleLearner : public PairwiseLearner {
    private:
       void update_gradient_matrix(std::vector<EntityGradient> &gm,
            EntityGradient& eg1,
            EntityGradient& eg2,
            uint64_t term);

       bool shouldUpdate(uint32_t idx);

    public:
        HoleLearner(KB &kb, LearnParams &p) :
            PairwiseLearner(kb, p) {
            }

        void process_batch_withnegs(BatchIO &io, std::vector<uint64_t> &oneg,
                std::vector<uint64_t> &sneg);

        std::string getName() {
            return "HolE";
        }

        float score(double*, double*, double*);
};
#endif
