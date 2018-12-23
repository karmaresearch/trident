#include "rts/operator/DuplLimit.hpp"
#include "rts/operator/PlanPrinter.hpp"

using namespace std;


DuplLimit::DuplLimit(
    Operator* input,
    const vector<Register*>& outputArgs,
    DuplicateHandling duplicateHandling,
    uint64_t limit, uint64_t offset)
    : Operator(1), output(outputArgs), input(input),
      duplicateHandling(duplicateHandling), limit(limit), offset(offset) {
    // Constructor
    currentCount = 0;
}


//---------------------------------------------------------------------------
uint64_t DuplLimit::first()
// Produce the first tuple
{
    if (observedOutputCardinality < limit) {
        if ((entryCount = input->first()) != 0) {
	    // If there is an offset, skip those.
	    uint64_t o = offset;
            currentCount++;
	    while (o > 0) {
		if (next() == 0) {
		    return 0;
		}
		observedOutputCardinality--;
		o--;
	    }
            observedOutputCardinality++;
            return 1;
        } else {
            return 0;
        }
    } else {
        return 0;
    }
}
//---------------------------------------------------------------------------
uint64_t DuplLimit::next()
// Produce the next tuple
{
    if (observedOutputCardinality < limit) {
        if (currentCount < entryCount && duplicateHandling == Duplicates) {
            currentCount++;
        } else if ((entryCount = input->next()) != 0) {
            currentCount = 1;
        } else {
            return 0;
        }
        observedOutputCardinality++;
        return 1;
    } else {
        return 0;
    }
}
//---------------------------------------------------------------------------
void DuplLimit::print(PlanPrinter& out)
// Print the operator tree. Debugging only.
{
    out.beginOperator("DuplLimit", expectedOutputCardinality, observedOutputCardinality);
    out.addMaterializationAnnotation(output);
    input->print(out);
    out.endOperator();
}
//---------------------------------------------------------------------------
void DuplLimit::addMergeHint(Register* /*reg1*/, Register* /*reg2*/)
// Add a merge join hint
{
    // Do not propagate as we break the pipeline
}
//---------------------------------------------------------------------------
void DuplLimit::getAsyncInputCandidates(Scheduler& scheduler)
// Register parts of the tree that can be executed asynchronous
{
    input->getAsyncInputCandidates(scheduler);
}
//---------------------------------------------------------------------------
