#ifndef _AGGR_HDL_H
#define _AGGR_HDL_H

#include <vector>
#include <map>

class AggregateHandler {
    public:
        typedef enum { COUNT, MIN, MAX, SUM, GROUP_CONCAT, AVG, SAMPLE } FUNC;

        struct VarValue {
            typedef enum {INT, DEC, SYMBOL, NUL} TYPE;
            int64_t v_int;
            double v_dec;
            TYPE type;
            bool requiresNumber;
        };

    private:
        struct FunctCall {
            FUNC id;
            uint64_t inputmask;
            uint64_t outputmask;
            unsigned inputvar; //Id input var
            unsigned outputvar; //Id output var

            int64_t arg1_int, arg2_int; //various arguments used by the functions
            double arg1_dec, arg2_dec;
            bool arg1_bool, arg2_bool;

            void reset() {
                arg1_int = arg2_int = 0;
                arg1_dec = arg2_dec = 0;
                arg1_bool = arg2_bool = true;
            }
        };


        unsigned varcount;
        std::map<FUNC,std::map<unsigned,unsigned>> assignments;
        uint64_t inputmask;
        std::vector<VarValue> varvalues;
        std::vector<FunctCall> executions;

        bool executeFunction(FunctCall &call);

        //Concrete implementations of the various functions
        bool execCount(FunctCall &call);
        bool execSum(FunctCall &call);
        bool execAvg(FunctCall &call);
        bool execMax(FunctCall &call);
        bool execMin(FunctCall &call);

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

        std::pair<std::vector<unsigned>,
            std::vector<unsigned>> getInputOutputVars() const;

        void updateVarInt(unsigned var, int64_t value, uint64_t count);

        void updateVarSymbol(unsigned var, uint64_t value, uint64_t count);

        void updateVarDec(unsigned var, double value, uint64_t count);

        void updateVarNull(unsigned var);

        int64_t getValueInt(unsigned var) const;

        double getValueDec(unsigned var) const;

        VarValue::TYPE getValueType(unsigned var) const;

        bool requiresNumber(unsigned var) const;

        void stopUpdate();
};

#endif
