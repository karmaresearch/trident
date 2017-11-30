#include <rts/operator/AggrFunctions.hpp>
#include <rts/operator/PlanPrinter.hpp>

#include <trident/kb/dictmgmt.h>

#include <kognac/logs.h>

#include <assert.h>

AggrFunctions::AggrFunctions(Operator* child,
        std::map<unsigned, Register *> bindings,
        const AggregateHandler &hdl,
        const std::vector<unsigned> &groupKeys,
        double expectedOutputCardinality) : Operator(expectedOutputCardinality),
    child(child), hdl(hdl) {
        for(unsigned v : groupKeys) {
            assert(bindings.count(v));
            this->groupKeys.push_back(bindings.find(v)->second);
        }
        currentGroupKeys.resize(groupKeys.size());
        this->hdl.prepare();
        auto iovars = this->hdl.getInputOutputVars();
        for(auto v : iovars.first) {
            Register *reg = bindings.find(v)->second;
            varsToUpdate.push_back(std::make_pair(v, reg));
        }
        for(auto v : iovars.second) {
            Register *reg = bindings.find(v)->second;
            varsToReturn.push_back(std::make_pair(v, reg));
        }
    }

/// Destructor
AggrFunctions::~AggrFunctions() {
}

void AggrFunctions::readKeys() {
    for(uint8_t i = 0; i < currentGroupKeys.size(); ++i) {
        currentGroupKeys[i] = groupKeys[i]->value;
    }
}

void AggrFunctions::copyVars() {
    for(uint8_t i = 0; i < varsToReturn.size(); ++i) {
        uint64_t val = hdl.getValue(varsToReturn[i].first);
        val = val | DICTMGMT_INTEGER;
        varsToReturn[i].second->value = val;
    }
}

bool AggrFunctions::sameKeyAsCurrent() {
    for(uint8_t i = 0; i < currentGroupKeys.size(); ++i) {
        if (groupKeys[i]->value !=  currentGroupKeys[i]) {
            return false;
        }
    }
    return true;
}

void AggrFunctions::processGroup() {
    //Add the current row
    hdl.startUpdate();
    for(auto &var : varsToUpdate) {
        hdl.updateVar(var.first, var.second->value, currentCount);
    }
    hdl.stopUpdate();

    //Add all the other rows which belong to the same group
    currentCount = child->next();
    while (currentCount && sameKeyAsCurrent()) {
        hdl.startUpdate();
        for(auto &var : varsToUpdate) {
            hdl.updateVar(var.first, var.second->value, currentCount);
        }
        hdl.stopUpdate();
        currentCount = child->next();
    }
    //Add an empty row to tell the handler that the group is finished
    hdl.startUpdate();
    for(auto &var : varsToUpdate) {
        hdl.updateVar(var.first, ~0lu, 0);
    }
    hdl.stopUpdate();
    copyVars();
    hdl.reset();
}

/// Produce the first tuple
uint64_t AggrFunctions::first() {
    currentCount = child->first();
    if (currentCount) {
        //Read the first group
        readKeys();
        //Read all the tuples in the group
        processGroup();
        //Update the variables produced by the aggregated functions
        observedOutputCardinality++;
        return 1;
    } else {
        return 0;
    }
}

/// Produce the next tuple
uint64_t AggrFunctions::next() {
    if (currentCount) {
        readKeys();
        processGroup();
        observedOutputCardinality++;
        return 1;
    } else {
        return 0;
    }
}

/// Print the operator tree. Debugging only.
void AggrFunctions::print(PlanPrinter& out) {
    out.beginOperator("AggrFunctions",expectedOutputCardinality, observedOutputCardinality);
    child->print(out);
    out.endOperator();
}

/// Add a merge join hint
void AggrFunctions::addMergeHint(Register* reg1,Register* reg2) {
    LOG(ERRORL) << "Not implemented yet";
    throw 10;
}

/// Register parts of the tree that can be executed asynchronous
void AggrFunctions::getAsyncInputCandidates(Scheduler& scheduler) {
    LOG(ERRORL) << "Not implemented yet";
    throw 10;
}
