#ifndef _TRANSE_H
#define _TRANSE_H

#include <trident/ml/embeddings.h>
#include <trident/utils/batch.h>

#include <boost/log/trivial.hpp>
#include <tbb/concurrent_queue.h>

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

struct BatchIO {
    //Input
    const uint16_t batchsize;
    const uint16_t dims;
    std::vector<uint64_t> field1;
    std::vector<uint64_t> field2;
    std::vector<uint64_t> field3;
    std::vector<std::unique_ptr<float>> posSignMatrix;
    std::vector<std::unique_ptr<float>> neg1SignMatrix;
    std::vector<std::unique_ptr<float>> neg2SignMatrix;
    //Output
    uint64_t violations;

    BatchIO(uint16_t batchsize, const uint16_t dims) : batchsize(batchsize), dims(dims) {
        for(uint16_t i = 0; i < batchsize; ++i) {
            posSignMatrix.push_back(std::unique_ptr<float>(new float[dims]));
            neg1SignMatrix.push_back(std::unique_ptr<float>(new float[dims]));
            neg2SignMatrix.push_back(std::unique_ptr<float>(new float[dims]));
        }
        clear();
    }

    void clear() {
        violations = 0;
        for(uint16_t i = 0; i < batchsize; ++i) {
            memset(posSignMatrix[i].get(), 0, sizeof(float) * dims);
            memset(neg1SignMatrix[i].get(), 0, sizeof(float) * dims);
            memset(neg2SignMatrix[i].get(), 0, sizeof(float) * dims);
        }
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

        void process_batch(BatchIO &io);

        void update_gradient_matrix(std::vector<EntityGradient> &gm,
                std::vector<std::unique_ptr<float>> &signmatrix,
                std::vector<uint32_t> &inputTripleID,
                std::vector<uint64_t> &inputTerms,
                int pos, int neg);

        void batch_processer(
                tbb::concurrent_bounded_queue<std::shared_ptr<BatchIO>> *inputQueue,
                tbb::concurrent_bounded_queue<std::shared_ptr<BatchIO>> *outputQueue,
                uint64_t *violations);

        //Store E and R into a file
        void store_model(string pathmodel);

    public:
        Transe(const uint16_t epochs, const uint32_t ne, const uint32_t nr,
                const uint16_t dim, const float margin, const float learningrate,
                const uint16_t batchsize) :
            epochs(epochs), ne(ne), nr(nr), dim(dim), margin(margin),
            learningrate(learningrate), batchsize(batchsize) {
            }

        void setup(const uint16_t nthreads);

        void train(BatchCreator &batcher, const uint16_t nthreads,
                const uint32_t eval_its,
                const string storefolder);

        //Load the model (=two sets of embeddings, E and R) from disk
        static std::pair<std::shared_ptr<Embeddings<float>>,std::shared_ptr<Embeddings<float>>>
            loadModel(string path);

        std::shared_ptr<Embeddings<float>> getE() {
            return E;
        }

        std::shared_ptr<Embeddings<float>> getR() {
            return R;
        }
};
#endif
