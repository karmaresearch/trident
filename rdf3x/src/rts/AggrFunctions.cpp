#include <rts/operator/AggrFunctions.hpp>

#include <kognac/logs.h>

AggrFunctions::AggrFunctions(Operator* child,
        std::map<unsigned, Register *> bindings,
        AggregateHandler *hdl,
        double expectedOutputCardinality) : Operator(expectedOutputCardinality) {
    //TODO: setup
}

/// Destructor
AggrFunctions::~AggrFunctions() {
}

/// Produce the first tuple
uint64_t AggrFunctions::first() {
    LOG(ERRORL) << "Not implemented yet";
    throw 10;
}

/// Produce the next tuple
uint64_t AggrFunctions::next() {
    LOG(ERRORL) << "Not implemented yet";
    throw 10;
}

/// Print the operator tree. Debugging only.
void AggrFunctions::print(PlanPrinter& out) {
    LOG(ERRORL) << "Not implemented yet";
    throw 10;
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
