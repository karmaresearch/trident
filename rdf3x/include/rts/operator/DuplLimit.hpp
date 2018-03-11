#ifndef H_rts_operator_DuplLimit
#define H_rts_operator_DuplLimit

#include "rts/operator/Operator.hpp"
#include <vector>
//---------------------------------------------------------------------------
class Register;
class Runtime;
//---------------------------------------------------------------------------
/// Consumes its input and prints it. Produces a single empty tuple.
class DuplLimit: public Operator {
public:
    /// Duplicate handling
    enum DuplicateHandling { Duplicates, NoDuplicates };

private:
    std::vector<Register*> output;
    /// The input
    Operator* input;
    /// The duplicate handling
    DuplicateHandling duplicateHandling;
    /// Maximum number of output tuples
    uint64_t limit;
    uint64_t entryCount, currentCount;

public:
    /// Constructor
    DuplLimit(Operator* input,
              const std::vector<Register*>& outputArgs,
              DuplicateHandling duplicateHandling,
              uint64_t limit = UINT64_MAX);

    /// Destructor
    ~DuplLimit() {
        delete input;
    }

    /// Produce the first tuple
    uint64_t first();
    /// Produce the next tuple
    uint64_t next();

    /// Print the operator tree. Debugging only.
    void print(PlanPrinter& out);
    /// Add a merge join hint
    void addMergeHint(Register* reg1, Register* reg2);
    /// Register parts of the tree that can be executed asynchronous
    void getAsyncInputCandidates(Scheduler& scheduler);
};
//---------------------------------------------------------------------------
#endif
