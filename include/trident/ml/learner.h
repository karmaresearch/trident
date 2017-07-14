#ifndef _LEARNER_H
#define _LEARNER_H

#include <trident/ml/embeddings.h>
#include <trident/utils/batch.h>
#include <trident/kb/querier.h>

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
    Querier *q;
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

class Learner {
    protected:
        KB &kb;
        const uint16_t epochs;
        const uint32_t ne;
        const uint32_t nr;
        const uint16_t dim;
        const float margin;
        const float learningrate;
        const uint16_t batchsize;
        const bool adagrad;

        std::shared_ptr<Embeddings<double>> E;
        std::shared_ptr<Embeddings<double>> R;
        std::unique_ptr<double> pe2; //used for adagrad
        std::unique_ptr<double> pr2; //used for adagrad

        float dist_l1(double* head, double* rel, double* tail,
                float *matrix);

        void batch_processer(
                Querier *q,
                tbb::concurrent_bounded_queue<std::shared_ptr<BatchIO>> *inputQueue,
                tbb::concurrent_bounded_queue<std::shared_ptr<BatchIO>> *outputQueue,
                uint64_t *violation,
                uint16_t epoch);

        //Store E and R into a file
        void store_model(string pathmodel, const bool gzipcompress,
                const uint16_t nthreads);

    public:
        Learner(KB &kb, const uint16_t epochs, const uint32_t ne,
                const uint32_t nr,
                const uint16_t dim, const float margin, const float learningrate,
                const uint16_t batchsize, const bool adagrad) :
            kb(kb), epochs(epochs), ne(ne), nr(nr), dim(dim), margin(margin),
            learningrate(learningrate), batchsize(batchsize), adagrad(adagrad) {
            }


        void setup(const uint16_t nthreads);

        void setup(const uint16_t nthreads,
                std::shared_ptr<Embeddings<double>> E,
                std::shared_ptr<Embeddings<double>> R,
                std::unique_ptr<double> pe,
                std::unique_ptr<double> pr);

        void train(BatchCreator &batcher, const uint16_t nthreads,
                const uint16_t nstorethreads,
                const uint32_t evalits,
                const uint32_t storeits,
                const string pathvalid,
                const string storefolder,
                const bool compressstorage);

        virtual void process_batch(BatchIO &io,
                const uint16_t epoch,
                const uint16_t
                nbatches) = 0;

        //Load the model (=two sets of embeddings, E and R) from disk
        static std::pair<std::shared_ptr<Embeddings<double>>,
            std::shared_ptr<Embeddings<double>>>
            loadModel(string path);

        std::shared_ptr<Embeddings<double>> getE() {
            return E;
        }

        std::shared_ptr<Embeddings<double>> getR() {
            return R;
        }
};
#endif
