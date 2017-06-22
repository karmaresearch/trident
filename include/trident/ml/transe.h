#ifndef _TRANSE_H
#define _TRANSE_H

#include <trident/ml/embeddings.h>
#include <trident/utils/batch.h>

#include <boost/log/trivial.hpp>

class Transe {
    private:
        const uint16_t epochs;
        const uint32_t ne;
        const uint32_t nr;
        const uint16_t dim;
        const float margin;

        std::shared_ptr<Embeddings<float>> E;
        std::shared_ptr<Embeddings<float>> R;

        float dist_l1(float* head, float* rel, float* tail);

    public:
        Transe(const uint16_t epochs, const uint32_t ne, const uint32_t nr,
                const uint16_t dim, const float margin) :
            epochs(epochs), ne(ne), nr(nr), dim(dim), margin(margin) {
        }

        void setup(const uint16_t nthreads);

        void train(BatchCreator &batcher);

        std::shared_ptr<Embeddings<float>> getE() {
            return E;
        }

        std::shared_ptr<Embeddings<float>> getR() {
            return R;
        }
};
#endif
