#ifndef _FEEDBACK_H
#define _FEEDBACK_H

#include <trident/ml/tester.h>

#include <unordered_map>

class Feedback {
    private:
        std::unordered_map<uint64_t, std::vector<uint32_t>> queries_po;
        std::unordered_map<uint64_t, std::vector<uint32_t>> queries_sp;
        uint16_t currentEpoch = 0;
        uint64_t excluded;
        uint64_t threshold = 0;

    public:
        Feedback(uint64_t threshold) : threshold(threshold) {}

        bool shouldBeIncluded(long s, long p, long o);

        void setCurrentEpoch(uint16_t epoch) {
            currentEpoch = epoch;
        }

        void addFeedbacks(std::shared_ptr<Tester<double>::OutputTest>);
};

#endif
