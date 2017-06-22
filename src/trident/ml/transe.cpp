#include <trident/ml/transe.h>

void Transe::setup(const uint16_t nthreads) {
    BOOST_LOG_TRIVIAL(debug) << "Creating E ...";
    E = std::shared_ptr<Embeddings<float>>(new Embeddings<float>(ne, dim));
    //Initialize it
    E->init(nthreads);
    BOOST_LOG_TRIVIAL(debug) << "Creating R ...";
    R = std::shared_ptr<Embeddings<float>>(new Embeddings<float>(nr, dim));    
    R->init(nthreads);
}

float Transe::dist_l1(float* head, float* rel, float* tail) {
    float result = 0.0;
    for (uint16_t i = 0; i < dim; ++i) {
        result += abs(head[i] + rel[i] - tail[i]);
    }
    return result;
}

void Transe::train(BatchCreator &batcher) {
    std::vector<uint64_t> output1;
    std::vector<uint64_t> output2;
    std::vector<uint64_t> output3;
    for (uint16_t epoch = 0; epoch < epochs; ++epoch) {
        BOOST_LOG_TRIVIAL(info) << "Start epoch " << epoch;
        batcher.start();
        while (batcher.getBatch(output1, output2, output3)) {
            //TODO: Get the corresponding embeddings
        }
    }
}
