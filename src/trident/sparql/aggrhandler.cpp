#include <trident/sparql/aggrhandler.h>

#include <kognac/logs.h>

#include <assert.h>
#include <set>

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
    executions.clear();
    varvalues.resize(64); //Max number of vars
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

void AggregateHandler::updateVar(unsigned var, uint64_t value, uint64_t count) {
    //For the moment I ignore "count" but later it might be taken into account
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
        case FUNC::MIN:
        case FUNC::MAX:
        case FUNC::SUM:
        case FUNC::GROUP_CONCAT:
        case FUNC::AVG:
        case FUNC::SAMPLE:
            LOG(ERRORL) << "Not yet implemented";
            throw 10;
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

std::pair<std::vector<unsigned>,
    std::vector<unsigned>> AggregateHandler::getInputOutputVars() const {
    std::set<unsigned> inputvars;
    std::set<unsigned> outputvars;
    for(auto &assignment : assignments) {
        for(auto &entry : assignment.second) {
            outputvars.insert(entry.second);
            inputvars.insert(entry.first);
        }
    }
    std::pair<std::vector<unsigned>,std::vector<unsigned>> out;
    for(auto &v : inputvars) {
        if (!outputvars.count(v)) {
            out.first.push_back(v);
        }
    }
    for(auto &v : outputvars) {
        if (!inputvars.count(v)) {
            out.second.push_back(v);
        }
    }
    return out;
}
