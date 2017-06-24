#ifndef _TRANSE_H
#define _TRANSE_H

#include <trident/ml/embeddings.h>
#include <trident/utils/batch.h>

#include <boost/log/trivial.hpp>

struct EntityGradient {
    const uint64_t id;
    uint32_t n;
    std::vector<float> dimensions;
    EntityGradient(const uint64_t id, uint16_t ndims) : id(id) {
        dimensions.resize(ndims);
        n = 0;
    }
};

struct _TranseSorter {
    const std::vector<uint64_t> &vec;
    _TranseSorter(const std::vector<uint64_t> &vec) : vec(vec) {}
    bool operator() (const uint32_t v1, const uint32_t v2) const {
        return vec[v1] < vec[v2];
    }
};

class Transe {
    private:
        const uint16_t epochs;
        const uint32_t ne;
        const uint32_t nr;
        const uint16_t dim;
        const float margin;
        const float learningrate;
        const uint16_t batchsize;

        std::shared_ptr<Embeddings<float>> E;
        std::shared_ptr<Embeddings<float>> R;

        float dist_l1(float* head, float* rel, float* tail,
                float *matrix);

        void gen_random(std::vector<uint64_t> &input, const uint64_t max);

        void update_gradient_matrix(std::vector<EntityGradient> &gm,
                std::vector<std::unique_ptr<float>> &signmatrix,
                std::vector<uint32_t> &inputTripleID,
                std::vector<uint64_t> &inputTerms,
                int pos, int neg);

    public:
        Transe(const uint16_t epochs, const uint32_t ne, const uint32_t nr,
                const uint16_t dim, const float margin, const float learningrate,
                const uint16_t batchsize) :
            epochs(epochs), ne(ne), nr(nr), dim(dim), margin(margin),
            learningrate(learningrate), batchsize(batchsize) {
            }

        void setup(const uint16_t nthreads);

        void train(BatchCreator &batcher, const uint16_t nthreads);

        std::shared_ptr<Embeddings<float>> getE() {
            return E;
        }

        std::shared_ptr<Embeddings<float>> getR() {
            return R;
        }
};
#endif
