#include <trident/sparql/aggrhandler.h>

#include <kognac/logs.h>

#include <assert.h>

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

void AggregateHandler::startUpdate() {
    inputmask = 0;
}

void AggregateHandler::prepare() {
    for(auto &el : assignments) {
        FUNC id = el.first;
        for(auto &assignment : el.second) {
            FunctCall call;
            call.id = id;
            call.inputmask = (uint64_t) 1 << assignment.first;
            call.outputmask = (uint64_t) 1 << assignment.second;
            call.inputvar = assignment.first;
            call.outputvar = assignment.second;
            executions.push_back(call);
        }
    }
    reset();
}

void AggregateHandler::reset() {
    for (auto &f : executions) {
        f.reset();
    }
}

void AggregateHandler::updateVar(unsigned var, uint64_t value) {
    assert(var <= 63);
    varvalues[var] = value;
    inputmask |= (uint64_t)1 << var;
}

void AggregateHandler::stopUpdate() {
    do {
        uint64_t outputmask = 0;
        for(auto &call : executions) {
            if (inputmask & call.inputmask) {
                bool res = executeFunction(call);
                if (res) {
                    outputmask |= call.outputmask;
                }
            }
        }
        inputmask = outputmask;
    } while (inputmask != 0);
}

uint64_t AggregateHandler::getValue(unsigned var) const {
    return varvalues[var];
}

bool AggregateHandler::executeFunction(FunctCall &call) {
    switch (call.id) {
        case FUNC::COUNT:
            return execCount(call);
        default:
            LOG(ERRORL) << "This should not happen. Aggregated function is unknown";
            throw 10;
    }
}

bool AggregateHandler::execCount(FunctCall &call) {
    //Get value of the var in input. If ~0lu, then return true and update the
    //output var
    if (varvalues[call.inputvar] == ~0lu) {
        varvalues[call.outputvar] = call.arg1;
        return true;
    } else {
        call.arg1++;
        return false;
    }
}
