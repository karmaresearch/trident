#ifndef H_rts_operator_groupby
#define H_rts_operator_groupby

#include <rts/operator/Operator.hpp>

#include <vector>

//---------------------------------------------------------------------------
class GroupBy : public Operator
{
    private:
        Operator *child;
        std::vector<Register*> regs;
        const bool distinct;

    public:
        /// Constructor
        GroupBy(Operator* child,
                std::vector<Register*> regs,
                bool distinct,
                double expectedOutputCardinality);

        /// Destructor
        ~GroupBy();

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
