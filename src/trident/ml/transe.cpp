#include <trident/ml/transe.h>

void Transe::setup() {
    BOOST_LOG_TRIVIAL(debug) << "Creating E ...";
    E = std::shared_ptr<Embeddings<float>>(new Embeddings<float>(ne, dim));
    BOOST_LOG_TRIVIAL(debug) << "Creating R ...";
    R = std::shared_ptr<Embeddings<float>>(new Embeddings<float>(nr, dim));
}

void Transe::train(BatchCreator &batcher) {
}
