#include <rts/operator/Minus.hpp>
#include <rts/operator/PlanPrinter.hpp>
#include <cts/codegen/CodeGen.hpp>

#include <vector>

Minus::Minus(Operator* mainTree,
        Operator* minusTree,
        std::vector<std::pair<Register*, Register*>> regsToCheck,
        double expectedOutputCardinality) : Operator(expectedOutputCardinality),
    mainTree(mainTree), minusTree(minusTree),
    regsToCheck(regsToCheck), loaded(false),
    row(regsToCheck.size()),
    hashF(row, valuesHashMap, regsToCheck.size()),
    equalF(row, valuesHashMap, regsToCheck.size()),
    valuesToCheck(10, hashF, equalF) {
    }

Minus::~Minus() {
    delete minusTree;
    delete mainTree;
}

uint64_t Minus::first() {
    observedOutputCardinality=0;
    uint64_t res = mainTree->first();
    bool found = false;
    if (res) {
        if (!loaded) {
            //Load the hashmap
            if (minusTree->first()) {
                do {
                    //Read the values in the registers and copy them in row
                    unsigned i = 0;
                    for(auto itr : regsToCheck) {
                        row[i++] = itr.second->value;
                    }
                    if (!valuesToCheck.count(-1)) {
                        //Add the values
                        size_t idx = valuesHashMap.size() / row.size();
                        for(auto v : row) {
                            valuesHashMap.push_back(v);
                        }
                        valuesToCheck.insert(idx);
                    }
                } while (minusTree->next());
            }
            loaded = true;
        }
        do {
            unsigned i = 0;
            for(auto itr : regsToCheck) {
                row[i++] = itr.first->value;
            }
            found = valuesToCheck.count(-1);
            if (found) {
                res = mainTree->next();
                if (!res)
                    break;
            }
        } while (found);
    }
    if (!found) {
        return res;
    } else {
        return 0;
    }
}

uint64_t Minus::next() {
    uint64_t res = mainTree->next();
    bool found = false;
    if (res) {
        do {
            unsigned i = 0;
            for(auto itr : regsToCheck) {
                row[i++] = itr.first->value;
            }
            found = valuesToCheck.count(-1);
            if (found) {
                res = mainTree->next();
                if (!res)
                    break;
            }
        } while (found);
    }
    if (!found) {
        return res;
    } else {
        return 0;
    }

}

void Minus::print(PlanPrinter& out) {
    out.beginOperator("Minus",expectedOutputCardinality, observedOutputCardinality);
    mainTree->print(out);
    minusTree->print(out);
    out.endOperator();

}

void Minus::addMergeHint(Register* reg1, Register* reg2) {
    mainTree->addMergeHint(reg1, reg2);
}

void Minus::getAsyncInputCandidates(Scheduler& scheduler) {
    mainTree->getAsyncInputCandidates(scheduler);
}
