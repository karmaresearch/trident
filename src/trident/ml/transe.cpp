#include <trident/ml/transe.h>

void Transe::setup() {
    BOOST_LOG_TRIVIAL(debug) << "Creating E ...";
    E = std::shared_ptr<Embeddings<float>>(new Embeddings<float>(ne, dim));
    //Initialize it
    E->init();
    BOOST_LOG_TRIVIAL(debug) << "Creating R ...";
    R = std::shared_ptr<Embeddings<float>>(new Embeddings<float>(nr, dim));    
    R->init();
}

void Transe::train(BatchCreator &batcher) {
    std::vector<uint64_t> output1;
    std::vector<uint64_t> output2;
    std::vector<uint64_t> output3;
    for (uint16_t epoch = 0; epoch < epochs; ++epoch) {
        BOOST_LOG_TRIVIAL(debug) << "Start epoch " << epoch;
        batcher.start();
        while (batcher.getBatch(output1, output2, output3)) {
            //TODO: Get the corresponding embeddings
        }
    }
}
