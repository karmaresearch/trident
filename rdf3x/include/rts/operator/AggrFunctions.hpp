#ifndef H_rts_operator_aggrfunctions
#define H_rts_operator_aggrfunctions

#include <rts/runtime/Runtime.hpp>
#include <rts/operator/Operator.hpp>

#include <trident/sparql/aggrhandler.h>

#include <map>
#include <vector>

//---------------------------------------------------------------------------
class AggrFunctions : public Operator {
    private:
        Operator *child;
        AggregateHandler hdl;
        std::vector<Register*> groupKeys;
        std::vector<std::pair<unsigned,Register*>> varsToUpdate;
        std::vector<std::pair<unsigned,Register*>> varsToReturn;
        std::vector<uint64_t> currentGroupKeys;
        uint64_t currentCount;

        void readKeys();
        void processGroup();
        bool sameKeyAsCurrent();
        void copyVars();

    public:
        /// Constructor
        AggrFunctions(Operator* child,
                std::map<unsigned, Register *> bindings,
                const AggregateHandler &hdl,
                const std::vector<unsigned> &groupKeys,
                double expectedOutputCardinality);

        /// Destructor
        ~AggrFunctions();

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
