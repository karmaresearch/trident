#ifndef _FEEDBACK_H
#define _FEEDBACK_H

#include <trident/ml/tester.h>

#include <unordered_map>

class Feedback {
    private:
        struct QueryDetails {
            std::vector<uint32_t> positions;
            double avg = -1;
        };

        std::unordered_map<uint64_t, QueryDetails> queries_po;
        std::unordered_map<uint64_t, QueryDetails> queries_sp;
        uint16_t currentEpoch = 0;
        uint64_t excluded;
        uint64_t threshold = 0;
        uint32_t minFullEpochs = 0;

    public:
        Feedback(uint64_t threshold, uint32_t minFullEpochs) :
            threshold(threshold),
            minFullEpochs(minFullEpochs) {}

        bool shouldBeIncluded(long s, long p, long o);

        void setCurrentEpoch(uint16_t epoch) {
            currentEpoch = epoch;
        }

        void addFeedbacks(std::shared_ptr<Tester<double>::OutputTest>);
};

#endif
