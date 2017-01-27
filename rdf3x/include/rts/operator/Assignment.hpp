#ifndef H_rts_operator_Assignment
#define H_rts_operator_Assignment
//---------------------------------------------------------------------------
// RDF-3X
// (c) 2008 Thomas Neumann. Web site: http://www.mpi-inf.mpg.de/~neumann/rdf3x
//
// This work is licensed under the Creative Commons
// Attribution-Noncommercial-Share Alike 3.0 Unported License. To view a copy
// of this license, visit http://creativecommons.org/licenses/by-nc-sa/3.0/
// or send a letter to Creative Commons, 171 Second Street, Suite 300,
// San Francisco, California, 94105, USA.
//---------------------------------------------------------------------------
#include "rts/operator/Operator.hpp"
#include "rts/operator/TableFunction.hpp"
//---------------------------------------------------------------------------
/// The SPARQL 1.1 BIND operator
class Register;
class Assignment: public Operator {
public:
    // A function argument
    struct AssignmentArgument {
        /// The register if any
        Register* reg;
        uint64_t defaultValue;
    };

private:
    Operator* input;
    std::vector<AssignmentArgument> inputArgs;
    std::vector<Register*> outputVars;

public:
    /// Constructor
    Assignment(Operator *input, const std::vector<AssignmentArgument>& inputArgs,
               const std::vector<Register*>& outputVars,
               double expectedOutputCardinality);

    /// Destructor
    ~Assignment() {
        delete input;
    }

    /// Produce the first tuple
    uint64_t first();
    /// Produce the next tuple
    uint64_t next();

    /// Print the operator tree. Debugging only.
    void print(PlanPrinter& out);

    /// Add a merge join hint
    void addMergeHint(Register* reg1, Register* reg2) {
        input->addMergeHint(reg1, reg2);
    }

    /// Register parts of the tree that can be executed asynchronous
    void getAsyncInputCandidates(Scheduler& scheduler) {
        input->getAsyncInputCandidates(scheduler);
    }
};
//---------------------------------------------------------------------------
#endif
