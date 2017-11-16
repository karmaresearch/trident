#ifndef H_rts_operator_CartProd
#define H_rts_operator_CartProd
//---------------------------------------------------------------------------
#include <rts/operator/Operator.hpp>
#include <rts/operator/Scheduler.hpp>
#include <infra/util/VarPool.hpp>
#include <vector>
//---------------------------------------------------------------------------
class Register;
//---------------------------------------------------------------------------
/// A memory based hash join
class CartProd: public Operator {
    private:
        /// The input
        Operator* left, *right;
        /// The non-join attributes
        std::vector<Register*> leftTail, rightTail;
        bool leftOptional, rightOptional;

    public:
        /// Constructor
        CartProd(Operator* left,
                const std::vector<Register*>& leftTail,
                Operator* right,
                const std::vector<Register*>& rightTail,
                double expectedOutputCardinality,
                bool leftOptional,
                bool rightOptional,
                int bitset);

        /// Destructor
        ~CartProd();

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
