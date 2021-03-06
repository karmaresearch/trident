#ifndef _PAIRWISE_H
#define _PAIRWISE_H

#include <trident/ml/learner.h>
#include <trident/ml/embeddings.h>
#include <trident/ml/batch.h>
#include <trident/kb/querier.h>
#include <trident/utils/parallel.h>

struct _PairwiseSorter {
    const std::vector<uint64_t> &vec;
    _PairwiseSorter(const std::vector<uint64_t> &vec) : vec(vec) {}
    bool operator() (const uint32_t v1, const uint32_t v2) const {
        return vec[v1] < vec[v2];
    }
};

class PairwiseLearner : public Learner {
    private:
        std::random_device rd;
        std::mt19937 gen;
        std::uniform_int_distribution<> dis;

        void gen_random(Querier *q,
                BatchIO &io,
                std::vector<uint64_t> &input,
                const bool subjObjs,
                const uint16_t ntries);

    public:
        PairwiseLearner(KB &kb, LearnParams &p) :
            Learner(kb, p), gen(rd()), dis(0, ne - 1) {
            }

        void process_batch(BatchIO &io, const uint32_t epoch, const uint16_t
                nbatches);

        virtual void process_batch_withnegs(BatchIO &io, std::vector<uint64_t> &oneg,
                std::vector<uint64_t> &sneg) = 0;

        bool generateViolations() {
            return true;
        }
};
#endif
