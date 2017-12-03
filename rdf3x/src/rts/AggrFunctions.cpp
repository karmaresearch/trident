#include <rts/operator/AggrFunctions.hpp>
#include <rts/operator/PlanPrinter.hpp>

#include <trident/kb/dictmgmt.h>

#include <kognac/logs.h>

#include <assert.h>

AggrFunctions::AggrFunctions(DBLayer& db, Operator* child,
        std::map<unsigned, Register *> bindings,
        const AggregateHandler &hdl,
        const std::vector<unsigned> &groupKeys,
        double expectedOutputCardinality) : Operator(expectedOutputCardinality),
    dict(db), child(child), hdl(hdl) {
        for(unsigned v : groupKeys) {
            assert(bindings.count(v));
            this->groupKeys.push_back(bindings.find(v)->second);
        }
        currentGroupKeys.resize(groupKeys.size());
        this->hdl.prepare();
        auto iovars = this->hdl.getInputOutputVars();
        for(auto v : iovars.first) {
            Register *reg = bindings.find(v)->second;
            varsToUpdate.push_back(std::make_pair(v, reg));
        }
        for(auto v : iovars.second) {
            Register *reg = bindings.find(v)->second;
            varsToReturn.push_back(std::make_pair(v, reg));
        }
    }

/// Destructor
AggrFunctions::~AggrFunctions() {
}

void AggrFunctions::readKeys() {
    for(uint8_t i = 0; i < currentGroupKeys.size(); ++i) {
        currentGroupKeys[i] = groupKeys[i]->value;
    }
}

void AggrFunctions::copyVars() {
    for(uint8_t i = 0; i < varsToReturn.size(); ++i) {
        if (hdl.getValueType(varsToReturn[i].first)
                == AggregateHandler::VarValue::TYPE::INT) {
            uint64_t val = hdl.getValueInt(varsToReturn[i].first);
            val = val | DICTMGMT_INTEGER;
            varsToReturn[i].second->value = val;
        } else {
            assert(hdl.getValueType(varsToReturn[i].first)
                    == AggregateHandler::VarValue::TYPE::DEC);
            double val = hdl.getValueDec(varsToReturn[i].first);
            uint64_t uint_val = 0;
            uint_val = uint_val | DICTMGMT_FLOAT;
            *(float*)((char*)(&uint_val) + 4) = val;
            varsToReturn[i].second->value = val;
        }
    }
}

bool AggrFunctions::sameKeyAsCurrent() {
    for(uint8_t i = 0; i < currentGroupKeys.size(); ++i) {
        if (groupKeys[i]->value !=  currentGroupKeys[i]) {
            return false;
        }
    }
    return true;
}

bool __endsWith(const std::string &s, const std::string &suffix) {
    if (s.length() >= suffix.length()) {
        return (0 == s.compare(s.length() - suffix.length(),
                    suffix.length(), suffix));
    }
    return false;
}

long __getLong(const std::string &s) {
    auto pos = s.find_first_of('"',1);
    std::string number;
    if (pos != string::npos)
        number = s.substr(1, pos-1);
    else
        number = s;
    return std::stol(number);
}

double __getDouble(const std::string &s) {
    std::string number = s.substr(1, s.find_first_of('"',1)-1);
    return std::stod(number);
}

void AggrFunctions::updateVar(std::pair<unsigned,Register*> &var,
        uint64_t currentCount) {
    uint64_t value = var.second->value;
    if (!hdl.requiresNumber(var.first)) {
        hdl.updateVarSymbol(var.first, value, currentCount);
    } else {
        if (DictMgmt::isnumeric(value)) {
            if (DictMgmt::getType(value) == DICTMGMT_INTEGER) {
                hdl.updateVarInt(var.first, DictMgmt::getIntValue(value), currentCount);
            } else { //Dec
                hdl.updateVarDec(var.first, DictMgmt::getFloatValue(value), currentCount);
            }
        } else {
            //The input is a symbol. Must do a dictionary lookup to check whether
            //I can retrieve the type and value
            const char *start;
            const char *end;
            ::Type::ID type;
            unsigned subType;
            bool lkp = dict.lookupById(value, start, end, type, subType);
            if (lkp) {
                std::string d = "^^<http://www.w3.org/2001/XMLSchema#double>";
                std::string f = "^^<http://www.w3.org/2001/XMLSchema#float>";
                std::string i = "^^<http://www.w3.org/2001/XMLSchema#integer>";
                string s = string(start, end);
                if (__endsWith(s,d) || __endsWith(s,f)) {
                    //Decimal number
                    double v1 = __getDouble(s);
                    hdl.updateVarDec(var.first, v1, currentCount);
                } else if (__endsWith(s,i)) {
                    //Integer number
                    long v1 = __getLong(s);
                    hdl.updateVarInt(var.first, v1, currentCount);
                } else {
                    //Treat the symbol as integer
                    hdl.updateVarSymbol(var.first, value, currentCount);
                }
            } else {
                //Treat the symbol as integer
                hdl.updateVarSymbol(var.first, value, currentCount);
            }
        }
    }
}

void AggrFunctions::processGroup() {
    //Add the current row
    hdl.startUpdate();
    for(auto &var : varsToUpdate) {
        updateVar(var, currentCount);
    }
    hdl.stopUpdate();

    //Add all the other rows which belong to the same group
    currentCount = child->next();
    while (currentCount && sameKeyAsCurrent()) {
        hdl.startUpdate();
        for(auto &var : varsToUpdate) {
            updateVar(var, currentCount);
        }
        hdl.stopUpdate();
        currentCount = child->next();
    }

    //The key values are changed need to restore them to the ones of the previous group
    if (currentCount) {
        for(uint8_t i = 0; i < currentGroupKeys.size(); ++i) {
            uint64_t next = groupKeys[i]->value;
            groupKeys[i]->value = currentGroupKeys[i];
            currentGroupKeys[i] = next;
        }
    }

    //Add an empty row to tell the handler that the group is finished
    hdl.startUpdate();
    for(auto &var : varsToUpdate) {
        hdl.updateVarNull(var.first);
    }
    hdl.stopUpdate();
    copyVars();
    hdl.reset();
}

/// Produce the first tuple
uint64_t AggrFunctions::first() {
    currentCount = child->first();
    if (currentCount) {
        //Read the first group
        readKeys();
        //Read all the tuples in the group
        processGroup();
        //Update the variables produced by the aggregated functions
        observedOutputCardinality++;
        return 1;
    } else {
        return 0;
    }
}

/// Produce the next tuple
uint64_t AggrFunctions::next() {
    if (currentCount) {
        processGroup();
        observedOutputCardinality++;
        return 1;
    } else {
        return 0;
    }
}

/// Print the operator tree. Debugging only.
void AggrFunctions::print(PlanPrinter& out) {
    out.beginOperator("AggrFunctions",expectedOutputCardinality, observedOutputCardinality);
    child->print(out);
    out.endOperator();
}

/// Add a merge join hint
void AggrFunctions::addMergeHint(Register* reg1,Register* reg2) {
    LOG(ERRORL) << "Not implemented yet";
    throw 10;
}

/// Register parts of the tree that can be executed asynchronous
void AggrFunctions::getAsyncInputCandidates(Scheduler& scheduler) {
    LOG(ERRORL) << "Not implemented yet";
    throw 10;
}
