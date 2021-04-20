#include <trident/sparql/aggrhandler.h>
#include <trident/kb/dictmgmt.h>

#include <kognac/logs.h>

#include <assert.h>
#include <set>
#include <cfloat>

unsigned AggregateHandler::getNewOrExistingVar(AggregateHandler::FUNC funID,
        std::vector<unsigned> &signature) {
    if (signature.size() != 1) {
        LOG(ERRORL) << "For now, I only support aggregates with one variable in input";
        throw 10;
    }
    unsigned v = signature[0];
    if (!assignments.count(funID)) {
        assignments[funID] = std::map<unsigned, unsigned>();
    }
    auto &map = assignments[funID];
    if (!map.count(v)) {
        map[v] = varcount;
        varcount++;
    }
    return map[v];
}

void AggregateHandler::startUpdate() {
    inputmask = 0;
}

void AggregateHandler::prepare() {
    executions.clear();
    varvalues.resize(64); //Max number of vars
    for(auto &el : assignments) {
        FUNC id = el.first;
        for(auto &assignment : el.second) {
            FunctCall call;
            call.id = id;
            call.inputmask = (uint64_t) 1 << assignment.first;
            call.outputmask = (uint64_t) 1 << assignment.second;
            call.inputvar = assignment.first;
            call.outputvar = assignment.second;
	    if (id == FUNC::MIN) {
		call.arg1_int = INT64_MAX;
		call.arg1_dec = DBL_MAX;
	    } else if (id == FUNC::MAX) {
		call.arg1_int = INT64_MIN;
		call.arg1_dec = -DBL_MAX;
	    }
            //Update the variables and mentions if they need to be numberic
            if (id == COUNT)
                varvalues[assignment.first].requiresNumber = false;
            else
                varvalues[assignment.first].requiresNumber = true;
            executions.push_back(call);
        }
    }
    reset();
}

void AggregateHandler::reset() {
    for (auto &f : executions) {
        f.reset();
    }
}

void AggregateHandler::updateVarInt(unsigned var,
        int64_t value, uint64_t count) {
    LOG(DEBUGL) << "updateVarInt: var = " << var << ", value = " << value << ", count = " << count;
    assert(var <= 63);
    varvalues[var].v_int = value;
    varvalues[var].v_count = count;
    varvalues[var].type = VarValue::TYPE::INT;
    inputmask |= (uint64_t)1 << var;
}

void AggregateHandler::updateVarDec(unsigned var,
        double value, uint64_t count) {
    LOG(DEBUGL) << "updateVarDec: var = " << var << ", value = " << value << ", count = " << count;
    assert(var <= 63);
    varvalues[var].v_dec = value;
    varvalues[var].v_count = count;
    varvalues[var].type = VarValue::TYPE::DEC;
    inputmask |= (uint64_t)1 << var;
}

void AggregateHandler::updateVarSymbol(unsigned var,
        uint64_t value, uint64_t count) {
    LOG(DEBUGL) << "updateVarSymbol: var = " << var << ", value = " << value << ", count = " << count;
    assert(var <= 63);
    varvalues[var].v_int = value;
    varvalues[var].v_count = count;
    varvalues[var].type = VarValue::TYPE::SYMBOL;
    inputmask |= (uint64_t)1 << var;
}


void AggregateHandler::updateVarNull(unsigned var) {
    assert(var <= 63);
    varvalues[var].type = VarValue::TYPE::NUL;
    inputmask |= (uint64_t)1 << var;
}

void AggregateHandler::stopUpdate() {
    do {
        uint64_t outputmask = 0;
        for(auto &call : executions) {
            if (inputmask & call.inputmask) {
                bool res = executeFunction(call);
                if (res) {
                    outputmask |= call.outputmask;
                }
            }
        }
        inputmask = outputmask;
    } while (inputmask != 0);
}

int64_t AggregateHandler::getValueInt(unsigned var) const {
    return varvalues[var].v_int;
}

double AggregateHandler::getValueDec(unsigned var) const {
    return varvalues[var].v_dec;
}

bool AggregateHandler::requiresNumber(unsigned var) const {
    return varvalues[var].requiresNumber;
}

AggregateHandler::VarValue::TYPE AggregateHandler::getValueType(
        unsigned var) const {
    return varvalues[var].type;
}

bool AggregateHandler::executeFunction(FunctCall &call) {
    switch (call.id) {
        case FUNC::COUNT:
            return execCount(call);
        case FUNC::SUM:
            return execSum(call);
        case FUNC::AVG:
	    return execAvg(call);
        case FUNC::MIN:
	    return execMin(call);
        case FUNC::MAX:
	    return execMax(call);
        case FUNC::GROUP_CONCAT:
        case FUNC::SAMPLE:
            LOG(ERRORL) << "Not yet implemented";
            throw 10;
        default:
            LOG(ERRORL) << "This should not happen. Aggregated function is unknown";
            throw 10;
    }
}

bool AggregateHandler::execCount(FunctCall &call) {
    //Get value of the var in input. If ~0lu, then return true and update the
    //output var
    if (varvalues[call.inputvar].type  == VarValue::TYPE::NUL) {
        varvalues[call.outputvar].v_int = call.arg1_int;
        varvalues[call.outputvar].type = VarValue::TYPE::INT;
        return true;
    } else {
        call.arg1_int += distinct[call.id] ? 1 : varvalues[call.inputvar].v_count;
        LOG(DEBUGL) << "arg1_int = " << call.arg1_int;
        return false;
    }
}

bool AggregateHandler::execSum(FunctCall &call) {
    //Get value of the var in input. If ~0lu, then return true and update the
    //output var
    if (varvalues[call.inputvar].type  == VarValue::TYPE::NUL) {
        if (call.arg1_bool) { //Int
            varvalues[call.outputvar].v_int = call.arg1_int;
            varvalues[call.outputvar].type = VarValue::TYPE::INT;
        } else { //Dec
            varvalues[call.outputvar].v_dec = call.arg1_dec;
            varvalues[call.outputvar].type = VarValue::TYPE::DEC;
        }
	return true;
    } else {
        //Need to get the numerical value of the input
        if (varvalues[call.inputvar].type  == VarValue::TYPE::INT) {
            //Check the internal value
            if (call.arg1_bool) {
                call.arg1_int += varvalues[call.inputvar].v_int;
            } else {
                call.arg1_dec += varvalues[call.inputvar].v_int;
            }
        } else { //Dec
            if (call.arg1_bool) {
                //Switch to decimal representation
                call.arg1_dec = call.arg1_int;
                call.arg1_bool = false;
            }
            call.arg1_dec += varvalues[call.inputvar].v_dec;
        }
	return false;
    }
}

bool AggregateHandler::execAvg(FunctCall &call) {
    if (varvalues[call.inputvar].type  == VarValue::TYPE::NUL) {
	if (call.arg2_int == 0) {
	    // No values
            varvalues[call.outputvar].v_int = 0;
            varvalues[call.outputvar].type = VarValue::TYPE::INT;
	} else {
	    if (call.arg1_bool) {
		varvalues[call.outputvar].v_int = call.arg1_int / call.arg2_int;
		varvalues[call.outputvar].type = VarValue::TYPE::INT;
	    } else {
		varvalues[call.outputvar].v_dec = call.arg1_dec / call.arg2_int;
		varvalues[call.outputvar].type = VarValue::TYPE::DEC;
	    }
	}
	return true;
    } else {
	call.arg2_int++;
        //Need to get the numerical value of the input
        if (varvalues[call.inputvar].type  == VarValue::TYPE::INT) {
            //Check the internal value
            if (call.arg1_bool) {
                call.arg1_int += varvalues[call.inputvar].v_int;
            } else {
                call.arg1_dec += varvalues[call.inputvar].v_int;
            }
        } else { //Dec
            if (call.arg1_bool) {
                //Switch to decimal representation
                call.arg1_dec = call.arg1_int;
                call.arg1_bool = false;
            }
            call.arg1_dec += varvalues[call.inputvar].v_dec;
        }
	return false;
    }
}

// TODO: MIN and MAX should work for strings as well
bool AggregateHandler::execMax(FunctCall &call) {
    if (varvalues[call.inputvar].type  == VarValue::TYPE::NUL) {
	if (call.arg2_bool) {
	    // No values yet. This is an error. How to deal with that? TODO!
            LOG(ERRORL) << "No values for MAX";
	    throw 10;
	    // varvalues[call.outputvar].v_int = 0;
            // varvalues[call.outputvar].type = VarValue::TYPE::INT;
	} else {
	    if (call.arg1_bool) { //Int
		varvalues[call.outputvar].v_int = call.arg1_int;
		varvalues[call.outputvar].type = VarValue::TYPE::INT;
	    } else { //Dec
		varvalues[call.outputvar].v_dec = call.arg1_dec;
		varvalues[call.outputvar].type = VarValue::TYPE::DEC;
	    }
        }
	return true;
    } else {
        //Need to get the numerical value of the input
	if (call.arg2_bool) {
	    call.arg2_bool = false;
	    if (varvalues[call.inputvar].type  == VarValue::TYPE::INT) {
		call.arg1_bool = true;
		call.arg1_int = varvalues[call.inputvar].v_int;
	    } else {
		call.arg1_bool = false;
		call.arg1_dec = varvalues[call.inputvar].v_dec;
	    }
	} else {
	    if (varvalues[call.inputvar].type  == VarValue::TYPE::INT) {
		//Check the internal value
		long value = varvalues[call.inputvar].v_int;
		if (call.arg1_bool) {
		    if (value > call.arg1_int) {
			call.arg1_int = value;
		    }
		} else {
		    if (value > call.arg1_dec) {
			call.arg1_bool = true;
			call.arg1_int = value;
		    }
		}
	    } else {
		double value = varvalues[call.inputvar].v_dec;
		if (call.arg1_bool) {
		    //Switch to decimal representation
		    if (value > call.arg1_int) {
			call.arg1_bool = false;
			call.arg1_dec = value;
		    }
		} else {
		    if (value > call.arg1_dec) {
			call.arg1_dec = value;
		    }
		}
	    }
	}
	return false;
    }
}

bool AggregateHandler::execMin(FunctCall &call) {
    // TODO: MIN and MAX should work for strings as well ...
    if (varvalues[call.inputvar].type  == VarValue::TYPE::NUL) {
	if (call.arg2_bool) {
	    // No values yet. This is an error. How to deal with that? TODO!
            LOG(ERRORL) << "No values for MIN";
	    throw 10;
	    // varvalues[call.outputvar].v_int = 0;
            // varvalues[call.outputvar].type = VarValue::TYPE::INT;
	} else {
	    if (call.arg1_bool) { //Int
		varvalues[call.outputvar].v_int = call.arg1_int;
		varvalues[call.outputvar].type = VarValue::TYPE::INT;
	    } else { //Dec
		varvalues[call.outputvar].v_dec = call.arg1_dec;
		varvalues[call.outputvar].type = VarValue::TYPE::DEC;
	    }
        }
	return true;
    } else {
        //Need to get the numerical value of the input
	if (call.arg2_bool) {
	    call.arg2_bool = false;
	    if (varvalues[call.inputvar].type  == VarValue::TYPE::INT) {
		call.arg1_bool = true;
		call.arg1_int = varvalues[call.inputvar].v_int;
	    } else {
		call.arg1_bool = false;
		call.arg1_dec = varvalues[call.inputvar].v_dec;
	    }
	} else {
	    if (varvalues[call.inputvar].type  == VarValue::TYPE::INT) {
		//Check the internal value
		long value = varvalues[call.inputvar].v_int;
		if (call.arg1_bool) {
		    if (value < call.arg1_int) {
			call.arg1_int = value;
		    }
		} else {
		    if (value < call.arg1_dec) {
			call.arg1_bool = true;
			call.arg1_int = value;
		    }
		}
	    } else {
		double value = varvalues[call.inputvar].v_dec;
		if (call.arg1_bool) {
		    //Switch to decimal representation
		    if (value < call.arg1_int) {
			call.arg1_bool = false;
			call.arg1_dec = value;
		    }
		} else {
		    if (value < call.arg1_dec) {
			call.arg1_dec = value;
		    }
		}
	    }
	}
	return false;
    }
}

std::pair<std::vector<unsigned>,
    std::vector<unsigned>> AggregateHandler::getInputOutputVars() const {
        std::set<unsigned> inputvars;
        std::set<unsigned> outputvars;
        for(auto &assignment : assignments) {
            for(auto &entry : assignment.second) {
                outputvars.insert(entry.second);
                inputvars.insert(entry.first);
            }
        }
        std::pair<std::vector<unsigned>,std::vector<unsigned>> out;
        for(auto &v : inputvars) {
            if (!outputvars.count(v)) {
                out.first.push_back(v);
            }
        }
        for(auto &v : outputvars) {
            if (!inputvars.count(v)) {
                out.second.push_back(v);
            }
        }
        return out;
    }
