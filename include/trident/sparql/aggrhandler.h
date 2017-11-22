#ifndef _AGGR_HDL_H
#define _AGGR_HDL_H

#include <vector>
#include <map>

class AggregateHandler {
    public:
        typedef enum { COUNT, MIN, MAX, SUM, GROUP_CONCAT, AVG, SAMPLE } FUNC;

    private:
        unsigned varcount;
        std::map<FUNC,std::map<unsigned,unsigned>> assignments;

    public:
        AggregateHandler(unsigned varcount) : varcount(varcount) {
        }

        unsigned getNewOrExistingVar(FUNC funID,
                std::vector<unsigned> &signature);

        unsigned getVarCount() {
            return varcount;
        }
};

#endif
