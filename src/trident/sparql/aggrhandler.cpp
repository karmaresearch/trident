#include <trident/sparql/aggrhandler.h>

#include <kognac/logs.h>

unsigned AggregateHandler::getNewOrExistingVar(AggregateHandler::FUNC funID,
        std::vector<unsigned> &signature) {
    if (signature.size() != 1) {
        LOG(ERRORL) << "For now, I only support aggregates with one variable in input";
        throw 10;
    }
    unsigned v = signature[0];
    if (!assignments.count(funID)) {
        assignments[funID] = std::map<unsigned, unsigned>();
    }
    auto &map = assignments[funID];
    if (!map.count(v)) {
        map[v] = varcount;
        varcount++;
    }
    return map[v];
}
