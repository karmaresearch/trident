#ifndef _AGGR_HDL_H
#define _AGGR_HDL_H

#include <vector>
#include <map>

class AggregateHandler {
    public:
        typedef enum { COUNT, MIN, MAX, SUM, GROUP_CONCAT, AVG, SAMPLE } FUNC;

    private:
        struct FunctCall {
            FUNC id;
            uint64_t inputmask;
            uint64_t outputmask;
            unsigned inputvar;
            unsigned outputvar;

            uint64_t arg1, arg2; //various arguments used by the functions

            void reset() {
                arg1 = arg2 = 0;
            }
        };

        unsigned varcount;
        std::map<FUNC,std::map<unsigned,unsigned>> assignments;
        uint64_t inputmask;
        std::vector<uint64_t> varvalues;
        std::vector<FunctCall> executions;

        bool executeFunction(FunctCall &call);

        //Concrete implementations of the various functions
        bool execCount(FunctCall &call);

    public:
        AggregateHandler(unsigned varcount) : varcount(varcount) {
        }

        unsigned getNewOrExistingVar(FUNC funID,
                std::vector<unsigned> &signature);

        unsigned getVarCount() const {
            return varcount;
        }

        bool empty() const {
            return assignments.empty();
        }

        void prepare();

        void reset();

        void startUpdate();

        std::pair<std::vector<unsigned>,std::vector<unsigned>> getInputOutputVars();

        void updateVar(unsigned var, uint64_t value, uint64_t count);

        uint64_t getValue(unsigned var) const;

        void stopUpdate();
};

#endif
