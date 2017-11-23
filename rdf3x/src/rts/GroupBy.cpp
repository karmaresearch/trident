#include <rts/operator/GroupBy.hpp>
#include <rts/operator/PlanPrinter.hpp>

GroupBy::GroupBy(Operator* child, double expectedOutputCardinality) : Operator(expectedOutputCardinality), child(child) {
}

/// Destructor
GroupBy::~GroupBy() {
    delete child;
}

/// Produce the first tuple
uint64_t GroupBy::first() {
    observedOutputCardinality=0;
    return child->first(); //TODO
}

/// Produce the next tuple
uint64_t GroupBy::next() {
    observedOutputCardinality++;
    return child->next(); //TODO
}

/// Print the operator tree. Debugging only.
void GroupBy::print(PlanPrinter& out) {
    out.beginOperator("GroupBy",expectedOutputCardinality, observedOutputCardinality);
    child->print(out);
    out.endOperator();
}

/// Add a merge join hint
void GroupBy::addMergeHint(Register* reg1,Register* reg2) {
    child->addMergeHint(reg1, reg2);
}

/// Register parts of the tree that can be executed asynchronous
void GroupBy::getAsyncInputCandidates(Scheduler& scheduler) {
    child->getAsyncInputCandidates(scheduler);
}
