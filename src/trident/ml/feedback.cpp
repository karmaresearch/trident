#include <trident/ml/feedback.h>

bool Feedback::shouldBeIncluded(int64_t s, int64_t p, int64_t o) {
    if (currentEpoch < minFullEpochs) {
        return true;
    }
    uint64_t key_spo = (uint64_t)s << 32;
    key_spo |= p;
    if (queries_sp.count(key_spo)) {
        auto el = queries_sp.find(key_spo);
        //int64_t positions = 0;
        //for (auto pos : el->second) {
        //    positions += pos;
        //}
        //double avgpos = positions / el->second.size();
        double avgpos = el->second.avg;
        if (avgpos < threshold) {
            excluded++;
            return false;
        }
    }
    uint64_t key_pos = (uint64_t)p << 32;
    key_pos |= o;
    if (queries_po.count(key_pos)) {
        auto el = queries_po.find(key_pos);
        //int64_t positions = 0;
        //for (auto pos : el->second) {
        //    positions += pos;
        //}
        //double avgpos = positions / el->second.size();
        double avgpos = el->second.avg;
        if (avgpos < threshold) {
            excluded++;
            return false;
        }
    }
    return true;
}

bool Feedback::_querySorter(const std::pair<uint32_t, QueriesItr>& a,
        const std::pair<uint32_t, QueriesItr>& b) {
    return a.first > b.first;
}

void Feedback::addFeedbacks(std::shared_ptr<Tester<double>::OutputTest> out) {
    LOG(DEBUGL) << "Adding feedbacks ...";
    LOG(DEBUGL) << "Excluded triples in a epoch so far: " << excluded;
    excluded = 0;
    queries_sp.clear();
    queries_po.clear();
    for (auto q : out->results) {
        uint64_t key_sp = (uint64_t)q.s << 32;
        key_sp |= (uint64_t)q.p;
        uint64_t key_po = (uint64_t)q.p << 32;
        key_po |= (uint64_t)q.o;
        if (!queries_sp.count(key_sp)) {
            queries_sp.insert(std::make_pair(key_sp, QueryDetails()));
        }
        queries_sp[key_sp].positions.push_back(q.posO);
        if (!queries_po.count(key_po)) {
            queries_po.insert(std::make_pair(key_po, QueryDetails()));
        }
        queries_po[key_po].positions.push_back(q.posS);
    }
    std::vector<std::pair<uint32_t, QueriesItr>> sp;
    std::vector<std::pair<uint32_t, QueriesItr>> po;
    //Calculate the average
    for(auto itr = queries_sp.begin(); itr != queries_sp.end(); ++itr) {
        uint64_t positions = 0;
        for(auto pos : itr->second.positions) {
            positions += pos;
        }
        itr->second.avg = positions / (double) itr->second.positions.size();
        sp.push_back(std::make_pair(itr->second.avg, itr));
    }
    for(auto itr = queries_po.begin(); itr != queries_po.end(); ++itr) {
        uint64_t positions = 0;
        for(auto pos : itr->second.positions) {
            positions += pos;
        }
        itr->second.avg = positions / (double) itr->second.positions.size();
        po.push_back(std::make_pair(itr->second.avg, itr));
    }
    //Sort the queries by descending position
    std::sort(sp.begin(), sp.end(), Feedback::_querySorter);
    std::sort(po.begin(), po.end(), Feedback::_querySorter);

    //Print them for debugging

    LOG(DEBUGL) << "done.";
}
