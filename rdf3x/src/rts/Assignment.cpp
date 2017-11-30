#include "rts/operator/Assignment.hpp"
#include "rts/operator/PlanPrinter.hpp"
#include "rts/operator/TableFunction.hpp"
#include "rts/operator/PlanPrinter.hpp"
#include "rts/runtime/Runtime.hpp"
#include "rts/runtime/TemporaryDictionary.hpp"

//#include "rts/database/Database.hpp"
//#include "rts/segment/DictionarySegment.hpp"

#include <iostream>
#include <sstream>

//---------------------------------------------------------------------------
Assignment::Assignment(Operator *input, const std::vector<AssignmentArgument>& inputArgs,
        const std::vector<Register*>& outputVars,
        double expectedOutputCardinality) :
    Operator(expectedOutputCardinality), input(input), inputArgs(inputArgs),
    outputVars(outputVars) {
    }
//---------------------------------------------------------------------------
void Assignment::print(PlanPrinter& out)
    // Print the operator tree. Debugging only.
{
    out.beginOperator("Bind", expectedOutputCardinality, observedOutputCardinality);
    input->print(out);
    out.endOperator();
}
//---------------------------------------------------------------------------
uint64_t Assignment::first() {
    if (input->first() == 0)
        return false;
    for (size_t j = 0; j < inputArgs.size(); ++j) {
        if (inputArgs[j].reg != NULL)
            outputVars[j]->value = inputArgs[j].reg->value;
        else
            outputVars[j]->value = inputArgs[j].defaultValue;
    }
    return true;
}
//---------------------------------------------------------------------------
uint64_t Assignment::next() {
    if (input->next()) {
        for (size_t j = 0; j < inputArgs.size(); ++j) {
            if (inputArgs[j].reg != NULL)
                outputVars[j]->value = inputArgs[j].reg->value;
            else
                outputVars[j]->value = inputArgs[j].defaultValue;
        }
        return true;
    }
    return false;
}
