#include <trident/ml/feedback.h>

#include <boost/log/trivial.hpp>

bool Feedback::shouldBeIncluded(long s, long p, long o) {
    if (currentEpoch < 20) {
        return true;
    }
    uint64_t key_spo = (uint64_t)s << 32;
    key_spo |= p;
    if (queries_sp.count(key_spo)) {
        auto el = queries_sp.find(key_spo);
        long positions = 0;
        for (auto pos : el->second) {
            positions += pos;
        }
        double avgpos = positions / el->second.size();
        if (avgpos < threshold) {
            excluded++;
            return false;
        }
    }
    uint64_t key_pos = (uint64_t)p << 32;
    key_pos |= o;
    if (queries_po.count(key_pos)) {
        auto el = queries_po.find(key_pos);
        long positions = 0;
        for (auto pos : el->second) {
            positions += pos;
        }
        double avgpos = positions / el->second.size();
        if (avgpos < threshold) {
            excluded++;
            return false;
        }
    }
    return true;
}

void Feedback::addFeedbacks(std::shared_ptr<Tester<double>::OutputTest> out) {
    BOOST_LOG_TRIVIAL(debug) << "Adding feedbacks ...";
    BOOST_LOG_TRIVIAL(debug) << "Excluded triples so far: " << excluded;
    excluded = 0;
    queries_sp.clear();
    queries_po.clear();
    for (auto q : out->results) {
        uint64_t key_sp = (uint64_t)q.s << 32;
        key_sp |= (uint64_t)q.p;
        uint64_t key_po = (uint64_t)q.p << 32;
        key_po |= (uint64_t)q.o;
        if (!queries_sp.count(key_sp)) {
            queries_sp.insert(std::make_pair(key_sp, std::vector<uint32_t>()));
        }
        queries_sp[key_sp].push_back(q.posO);
        if (!queries_po.count(key_po)) {
            queries_po.insert(std::make_pair(key_po, std::vector<uint32_t>()));
        }
        queries_po[key_po].push_back(q.posS);
    }
    BOOST_LOG_TRIVIAL(debug) << "done.";
}
