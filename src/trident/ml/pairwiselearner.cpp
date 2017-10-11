#include <trident/ml/pairwiselearner.h>
#include <trident/ml/transetester.h>
#include <kognac/utils.h>

#include <tbb/concurrent_queue.h>

#include <iostream>
#include <fstream>
#include <chrono>
#include <cmath>

using namespace std;

void PairwiseLearner::gen_random(
        Querier *q,
        BatchIO &io,
        std::vector<uint64_t> &input,
        const bool subjObjs,
        const uint16_t ntries) {
    for(uint32_t i = 0; i < input.size(); ++i) {
        long s, p, o;
        s = io.field1[i];
        p = io.field2[i];
        o = io.field3[i];
        uint16_t attemptId;
        for(attemptId = 0; attemptId < ntries; ++attemptId) {
            input[i] = dis(gen);
            //Check if the resulting triple is existing ...
            if (subjObjs) {
                s = input[i];
            } else {
                o = input[i];
            }
            if (!q->exists(s, p, o)) {
                break;
            }
        }
    }
}

void PairwiseLearner::process_batch(BatchIO &io, const uint16_t epoch,
        const uint16_t nbatches) {
    //Generate negative samples
    std::vector<uint64_t> oneg;
    std::vector<uint64_t> sneg;
    uint32_t sizebatch = io.field1.size();
    oneg.resize(sizebatch);
    sneg.resize(sizebatch);
    gen_random(io.q, io, sneg, true, 10);
    gen_random(io.q, io, oneg, false, 10);
    process_batch(io, oneg, sneg);
}
