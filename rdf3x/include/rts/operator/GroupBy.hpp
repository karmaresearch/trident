#ifndef H_rts_operator_groupby
#define H_rts_operator_groupby

#include <rts/runtime/Runtime.hpp>
#include <rts/operator/Operator.hpp>

#include <map>
#include <vector>

//---------------------------------------------------------------------------
class GroupBy : public Operator
{
    private:
        Operator *child;
        std::map<unsigned, Register *> bindings;
        std::vector<unsigned> regs;
        const bool distinct;
        std::vector<std::unique_ptr<std::vector<uint64_t>>> values;
        std::vector<unsigned> keys;
        uint64_t index;
        std::vector<unsigned> fields;

        bool sameThanPrevious(uint64_t index);
        std::vector<unsigned> calculateFieldsToCompare(
                std::vector<unsigned> &keys,
                std::vector<unsigned> &regs);

    public:
        /// Constructor
        GroupBy(Operator* child,
                std::map<unsigned, Register *> bindings,
                std::vector<unsigned> regs,
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
