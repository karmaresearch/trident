#include <rts/operator/ValuesScan.hpp>
#include <rts/operator/PlanPrinter.hpp>

ValuesScan::ValuesScan(std::vector<Register*> regs,
        const std::vector<uint64_t> &values,
        double expectedOutputCardinality) : Operator(expectedOutputCardinality),
    regs(regs), values(values) {
        if (regs.size() > 0)
            toBeProcessed = values.size() / regs.size();
        else
            toBeProcessed = 0;
        processed = 0;
    }

/// Destructor
ValuesScan::~ValuesScan() {
}

/// Produce the first tuple
uint64_t ValuesScan::first() {
    if (processed < toBeProcessed) {
        for (auto v : regs) {
            v->value = values[processed++];
        }
        return 1;
    } else {
        return 0;
    }
}

/// Produce the next tuple
uint64_t ValuesScan::next() {
    if (processed < toBeProcessed) {
        for (auto v : regs) {
            v->value = values[processed++];
        }
        return 1;
    } else {
        return 0;
    }

}

/// Print the operator tree. Debugging only.
void ValuesScan::print(PlanPrinter& out) {
    out.beginOperator("VALUES",expectedOutputCardinality, processed);
    out.endOperator();
}

/// Add a merge join hint
void ValuesScan::addMergeHint(Register* reg1,Register* reg2) {
}

/// Register parts of the tree that can be executed asynchronous
void ValuesScan::getAsyncInputCandidates(Scheduler& scheduler) {
}
