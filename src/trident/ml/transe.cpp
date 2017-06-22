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

void Transe::train(BatchCreator &batcher, const uint16_t nthreads) {
    std::vector<uint64_t> output1;
    std::vector<uint64_t> output2;
    std::vector<uint64_t> output3;
    for (uint16_t epoch = 0; epoch < epochs; ++epoch) {
        BOOST_LOG_TRIVIAL(info) << "Start epoch " << epoch;
        batcher.start();
        double sum = 0;
        uint32_t batchcounter = 0;
        while (batcher.getBatch(output1, output2, output3)) {
            // Get the corresponding embeddings
            uint32_t sizebatch = output1.size();
            for(uint32_t i = 0; i < sizebatch; ++i) {
                //Get corresponding embeddings
                float* emb1 = E->get(output1[i]);
                float* emb2 = R->get(output2[i]);
                float* emb3 = E->get(output3[i]);
                for(uint16_t j = 0; j < dim; ++j) {
                    sum += emb1[j];
                    sum += emb2[j];
                    sum += emb3[j];
                }
            }
            if (batchcounter % 10000 == 0) {
                BOOST_LOG_TRIVIAL(debug) << "Processed " << batchcounter << " batches";
            }
            batchcounter++;
        }
        BOOST_LOG_TRIVIAL(info) << "End epoch" << sum;
    }
}
