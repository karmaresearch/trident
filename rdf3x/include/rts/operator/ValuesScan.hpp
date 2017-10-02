#ifndef H_rts_operator_ValuesScan
#define H_rts_operator_ValuesScan

#include <rts/operator/Operator.hpp>
#include <cts/plangen/Plan.hpp>
#include <rts/runtime/Runtime.hpp>
#include <set>
#include <unordered_set>
#include <map>
#include <vector>

//---------------------------------------------------------------------------
/// A sort operator
class ValuesScan: public Operator
{
    private:
        std::vector<Register*> regs;
        const std::vector<uint64_t> &values;
        unsigned processed, toBeProcessed;


    public:
        /// Constructor
        ValuesScan(std::vector<Register*> regs,
                const std::vector<uint64_t> &values,
                double expectedOutputCardinality);

        /// Destructor
        ~ValuesScan();

        /// Produce the first tuple
        uint64_t first();
        /// Produce the next tuple
        uint64_t next();
        /// Print the operator tree. Debugging only.
        void print(PlanPrinter& out);
        /// Add a merge join hint
        void addMergeHint(Register* reg1,Register* reg2);
        /// Register parts of the tree that can be executed asynchronous
        void getAsyncInputCandidates(Scheduler& scheduler);
};
//---------------------------------------------------------------------------
#endif
