#include "rts/operator/Selection.hpp"
#include "rts/operator/PlanPrinter.hpp"
#include "rts/operator/ResultsPrinter.hpp"
#include "rts/runtime/Runtime.hpp"
#include "cts/codegen/CodeGen.hpp"
#include "cts/plangen/PlanGen.hpp"

#include <trident/kb/dictmgmt.h>

#include <sstream>
#include <cassert>
#include <cstdlib>
#include <cstdio>
#ifdef __GNUC__
#if (__GNUC__>4)||((__GNUC__==4)&&(__GNUC_MINOR__>=9))
#define CONFIG_TR1
#endif
#endif
#ifdef CONFIG_TR1
#include <tr1/regex>
#endif
//---------------------------------------------------------------------------
// RDF-3X
// (c) 2008 Thomas Neumann. Web site: http://www.mpi-inf.mpg.de/~neumann/rdf3x
//
// This work is licensed under the Creative Commons
// Attribution-Noncommercial-Share Alike 3.0 Unported License. To view a copy
// of this license, visit http://creativecommons.org/licenses/by-nc-sa/3.0/
// or send a letter to Creative Commons, 171 Second Street, Suite 300,
// San Francisco, California, 94105, USA.
//---------------------------------------------------------------------------
using namespace std;

int64_t _getLong(const std::string &s) {
    auto pos = s.find_first_of('"',1);
    std::string number;
    if (pos != string::npos)
        number = s.substr(1, pos-1);
    else
        number = s;
    return std::stoll(number);
}

double _getDouble(const std::string &s) {
    std::string number = s.substr(1, s.find_first_of('"',1)-1);
    return std::stod(number);
}

int64_t _getTime(const std::string &s, Selection::NumType tp) {
    std::string time = s.substr(1, s.find_first_of('"',1)-1);
    int year;
    int month;
    int day;
    int hour = 0;
    int min = 0;
    int sec = 0;
    if (tp == Selection::NumType::DATE) {
	int retval = sscanf(time.c_str(), "%d-%d-%d", &year, &month, &day);
	if (retval != 3) {
	    throw 10;
	}
    } else {
	int retval = sscanf(time.c_str(), "%d-%d-%dT%d:%d:%d", &year, &month, &day, &hour, &min, &sec);
	if (retval != 6) {
	    throw 10;
	}
    }
    return ((int64_t) year << 48) + ((int64_t) month << 40) + ((int64_t) day << 32) + (hour << 24)
	+ (min << 16) + sec;
}

bool _endsWith(const std::string &s, const std::string &suffix) {
    if (s.length() >= suffix.length()) {
        return (0 == s.compare(s.length() - suffix.length(), suffix.length(), suffix));
    }
    return false;
}

Selection::NumType Selection::getNumType(std::string s) {
    std::string dbl = "^^<http://www.w3.org/2001/XMLSchema#double>";
    std::string flt = "^^<http://www.w3.org/2001/XMLSchema#float>";
    std::string integer = "^^<http://www.w3.org/2001/XMLSchema#integer>";
    std::string datetime = "^^<http://www.w3.org/2001/XMLSchema#dateTime>";
    std::string date = "^^<http://www.w3.org/2001/XMLSchema#date>";
    if (_endsWith(s,dbl) || _endsWith(s,flt)) {
        return NumType::DECIMAL;
    } else if (_endsWith(s,integer)) {
        return NumType::INT;
    } else if (_endsWith(s,datetime)) {
	return NumType::DATETIME;
    } else if (_endsWith(s,date)) {
	return NumType::DATE;
    }
    return NumType::UNKNOWN;
}

bool Selection::numeric(const Result &v) {
    auto r = runtime.valueMap.find(v.id);
    if (r == runtime.valueMap.end()) {
	IdValue val;
	val.tp = getNumType(v.value);
	if (val.tp == NumType::DECIMAL) {
	    val.val.dv = _getDouble(v.value);
	    runtime.valueMap[v.id] = val;
	    return true;
	} else if (val.tp == NumType::INT) {
	    val.val.iv = _getLong(v.value);
	    runtime.valueMap[v.id] = val;
	    return true;
	} else if (val.tp == NumType::DATE || val.tp == NumType::DATETIME) {
	    val.val.iv = _getTime(v.value, val.tp);
	    runtime.valueMap[v.id] = val;
	    return true;
	}
	return false;
    }
    return true;
}

bool Selection::isNumericComparison(const Result &l, const Result &r) {
    return numeric(l) && numeric(r);
}

bool Selection::numLess(const Result &l, const Result &r) {
    IdValue v1 = runtime.valueMap[l.id];
    IdValue v2 = runtime.valueMap[r.id];
    if (v1.tp != NumType::DECIMAL) {
	if (v2.tp != NumType::DECIMAL) {
	    return v1.val.iv < v2.val.iv;
	}
	return v1.val.iv < v2.val.dv;
    }
    if (v2.tp != NumType::DECIMAL) {
	return v1.val.dv < v2.val.iv;
    }
    return v1.val.dv < v2.val.dv;
}

//---------------------------------------------------------------------------
Selection::Result::~Result()
    // Destructor
{
}
//---------------------------------------------------------------------------
void Selection::Result::ensureString(Selection* selection)
    // Ensure that a string is available
{
    if (!(flags & stringAvailable)) {
        if (flags & idAvailable) {
            const char* start, *stop;
            if ((~id) && (selection->runtime.getDatabase().lookupById(id, start, stop, type, subType))) {
                value = string(start, stop);
                flags |= typeAvailable;
            } else {
                value = "NULL";
            }
        } else if (flags & booleanAvailable) {
            if (boolean)
                value = "true";
            else
                value = "false";
        } else {
            value = "";
        }
        flags |= stringAvailable;
    }
}
//---------------------------------------------------------------------------
void Selection::Result::ensureType(Selection* selection)
    // Ensure that the type is available
{
    if (!(flags & typeAvailable)) {
        if (flags & idAvailable) {
            const char* start, *stop;
            if ((~id) && (selection->runtime.getDatabase().lookupById(id, start, stop, type, subType))) {
                value = string(start, stop);
                flags |= stringAvailable;
            } else {
                type = Type::Literal; // XXX NULL type?
            }
        } else if (flags & booleanAvailable) {
            type = Type::Boolean;
        } else {
            type = Type::Literal;
        }
        flags |= typeAvailable;
    }
}
//---------------------------------------------------------------------------
void Selection::Result::ensureSubType(Selection* selection)
    // Ensure that the type is available
{
    ensureType(selection);
    if (!(flags & subTypeAvailable)) {
        if ((type == Type::CustomLanguage) || (type == Type::CustomType)) {
            const char* start, *stop;
            Type::ID t;
            unsigned st;
            if (selection->runtime.getDatabase().lookupById(subType, start, stop, t, st)) {
                subTypeValue = string(start, stop);
            } else {
                subTypeValue.clear();
            }
        } else {
            subTypeValue.clear();
        }
        flags |= subTypeAvailable;
    }
}
//---------------------------------------------------------------------------
void Selection::Result::ensureBoolean(Selection* runtime)
    // Ensure that a boolean interpretation is available
{
    if (!(flags & booleanAvailable)) {
        ensureString(runtime);
        boolean = (value == "true");
        flags |= booleanAvailable;
    }
}
//---------------------------------------------------------------------------
void Selection::Result::setBoolean(bool v)
    // Set to a boolean value
{
    flags = booleanAvailable | typeAvailable;
    type = Type::Boolean;
    boolean = v;
}
//---------------------------------------------------------------------------
void Selection::Result::setId(uint64_t v)
    // Set to an id value
{
    flags = idAvailable;
    id = v;
}
//---------------------------------------------------------------------------
void Selection::Result::setLiteral(const std::string& v)
    // Set to a string value
{
    flags = stringAvailable | typeAvailable;
    type = Type::Literal;
    value = v;
}
//---------------------------------------------------------------------------
void Selection::Result::setIRI(const std::string& v)
    // Set to a string value
{
    flags = stringAvailable | typeAvailable;
    type = Type::URI;
    value = v;
}
//---------------------------------------------------------------------------
Selection::Predicate::Predicate()
    : selection(0)
      // Constructor
{
}
//---------------------------------------------------------------------------
Selection::Predicate::~Predicate()
    // Destructor
{
}
//---------------------------------------------------------------------------
void Selection::Predicate::setSelection(Selection* s)
    // Set the selection
{
    selection = s;
}
//---------------------------------------------------------------------------
bool Selection::Predicate::check()
    // Check the predicate
{
    Result r;
    eval(r);
    r.ensureBoolean(selection);
    return r.boolean;
}
//---------------------------------------------------------------------------
Selection::BinaryPredicate::~BinaryPredicate()
    // Destructor
{
    delete left;
    delete right;
}
//---------------------------------------------------------------------------
void Selection::BinaryPredicate::setSelection(Selection* s)
    // Set the selection
{
    Predicate::setSelection(s);
    left->setSelection(s);
    right->setSelection(s);
}
//---------------------------------------------------------------------------
Selection::UnaryPredicate::~UnaryPredicate()
    // Destructor
{
    delete input;
}
//---------------------------------------------------------------------------
void Selection::UnaryPredicate::setSelection(Selection* s)
    // Set the selection
{
    Predicate::setSelection(s);
    input->setSelection(s);
}
//---------------------------------------------------------------------------
void Selection::Or::eval(Result& result)
    // Evaluate the predicate
{
    result.setBoolean(left->check() || right->check());
}
//---------------------------------------------------------------------------
string Selection::Or::print(PlanPrinter& out)
    // Print the predicate (debugging only)
{
    return "(" + left->print(out) + ")||(" + right->print(out) + ")";
}
//---------------------------------------------------------------------------
void Selection::And::eval(Result& result)
    // Evaluate the predicate
{
    result.setBoolean(left->check() && right->check());
}
//---------------------------------------------------------------------------
string Selection::And::print(PlanPrinter& out)
    // Print the predicate (debugging only)
{
    return "(" + left->print(out) + ")&&(" + right->print(out) + ")";
}
//---------------------------------------------------------------------------
void Selection::Equal::eval(Result& result)
    // Evaluate the predicate
{
    Result l, r;
    left->eval(l);
    right->eval(r);

    // Cheap case first
    if (l.hasId() && r.hasId()) {
        result.setBoolean(l.id == r.id);
        return;
    }

    // Now compare for real
    l.ensureString(selection);
    r.ensureString(selection);
    result.setBoolean(l.value == r.value);
}
//---------------------------------------------------------------------------
string Selection::Equal::print(PlanPrinter& out)
    // Print the predicate (debugging only)
{
    return "(" + left->print(out) + ")==(" + right->print(out) + ")";
}
//---------------------------------------------------------------------------
void Selection::NotEqual::eval(Result& result)
    // Evaluate the predicate
{
    Result l, r;
    left->eval(l);
    right->eval(r);

    // Cheap case first
    if (l.hasId() && r.hasId()) {
        result.setBoolean(l.id != r.id);
        return;
    }

    // Now compare for real
    l.ensureString(selection);
    r.ensureString(selection);
    result.setBoolean(l.value != r.value);
}
//---------------------------------------------------------------------------
string Selection::NotEqual::print(PlanPrinter& out)
    // Print the predicate (debugging only)
{
    return "(" + left->print(out) + ")!=(" + right->print(out) + ")";
}
//---------------------------------------------------------------------------
void Selection::Less::eval(Result& result)
    // Evaluate the predicate
{
    Result l, r;
    left->eval(l);
    right->eval(r);

    bool num1 =  DictMgmt::isnumeric(l.id);
    bool num2 =  DictMgmt::isnumeric(r.id);
    if (!num1) {
        l.ensureString(selection);
    }
    if (!num2) {
        r.ensureString(selection);
    }

    if (!num1 && !num2) {
        if (selection->isNumericComparison(l,r)) {
            result.setBoolean(selection->numLess(l,r));
        } else {
            result.setBoolean(l.value < r.value);
        }
    } else if (num1 && !num2) {
        //The comparison is numeric
        auto tr = getNumType(r.value);
        uint64_t t2;
        uint64_t v2 = 0;
        if (tr != NumType::DECIMAL) {
            t2 = DICTMGMT_INTEGER;
            v2 = _getLong(r.value);
        } else {
            t2 = DICTMGMT_FLOAT;
            float v2_tmp1 = _getDouble(r.value);
            uint32_t v2_tmp;
	    memcpy(&v2_tmp, &v2_tmp1, sizeof(float));
            v2 += v2_tmp;
        }
        int res = DictMgmt::compare(DictMgmt::getType(l.id), l.id, t2, v2);
        result.setBoolean(res < 0);
    } else if (!num1 && num2) {
        //The comparison is numeric
        auto tl = getNumType(l.value);
        uint64_t t1;
        uint64_t v1 = 0;
        if (tl != NumType::DECIMAL) {
            t1 = DICTMGMT_INTEGER;
            v1 = _getLong(l.value);
        } else {
            t1 = DICTMGMT_FLOAT;
            float v1_tmp1 = _getDouble(l.value);
            uint32_t v1_tmp;
	    memcpy(&v1_tmp, &v1_tmp1, sizeof(float));
            v1 += v1_tmp;
        }
        int res = DictMgmt::compare(t1, v1, DictMgmt::getType(r.id), r.id);
        result.setBoolean(res < 0);
    } else {
        //Numbers can be int or float
        int res = DictMgmt::compare(DictMgmt::getType(l.id), l.id, DictMgmt::getType(r.id), r.id);
        result.setBoolean(res < 0);
    }
}
//---------------------------------------------------------------------------
string Selection::Less::print(PlanPrinter& out)
    // Print the predicate (debugging only)
{
    return "(" + left->print(out) + ")<(" + right->print(out) + ")";
}
//---------------------------------------------------------------------------
void Selection::LessOrEqual::eval(Result& result)
    // Evaluate the predicate
{
    /*Result l, r;
    left->eval(l);
    right->eval(r);

    l.ensureString(selection);
    r.ensureString(selection);
    if (isNumericComparison(l,r)) {
        result.setBoolean(! numLess(r,l));
    } else {
        result.setBoolean(l.value <= r.value);
    }*/
    Result l, r;
    left->eval(l);
    right->eval(r);

    bool num1 =  DictMgmt::isnumeric(l.id);
    bool num2 =  DictMgmt::isnumeric(r.id);
    if (!num1) {
        l.ensureString(selection);
    }
    if (!num2) {
        r.ensureString(selection);
    }

    if (!num1 && !num2) {
        if (selection->isNumericComparison(l,r)) {
            result.setBoolean(!selection->numLess(r,l));
        } else {
            result.setBoolean(l.value <= r.value);
        }
    } else if (num1 && !num2) {
        //The comparison is numeric
        auto tr = getNumType(r.value);
        uint64_t t2;
        uint64_t v2 = 0;
        if (tr != NumType::DECIMAL) {
            t2 = DICTMGMT_INTEGER;
            v2 = _getLong(r.value);
        } else {
            t2 = DICTMGMT_FLOAT;
            float v2_tmp1 = _getDouble(r.value);
            uint32_t v2_tmp;
	    memcpy(&v2_tmp, &v2_tmp1, sizeof(float));
            v2 += v2_tmp;
        }
        int res = DictMgmt::compare(DictMgmt::getType(l.id), l.id, t2, v2);
        result.setBoolean(res <= 0);
    } else if (!num1 && num2) {
        //The comparison is numeric
        auto tl = getNumType(l.value);
        uint64_t t1;
        uint64_t v1 = 0;
        if (tl != NumType::DECIMAL) {
            t1 = DICTMGMT_INTEGER;
            v1 = _getLong(l.value);
        } else {
            t1 = DICTMGMT_FLOAT;
            float v1_tmp1 = _getDouble(l.value);
            uint32_t v1_tmp;
	    memcpy(&v1_tmp, &v1_tmp1, sizeof(float));
            v1 += v1_tmp;
        }
        int res = DictMgmt::compare(t1, v1, DictMgmt::getType(r.id), r.id);
        result.setBoolean(res <= 0);
    } else {
        //Numbers can be int or float
        int res = DictMgmt::compare(DictMgmt::getType(l.id), l.id, DictMgmt::getType(r.id), r.id);
        result.setBoolean(res <= 0);
    }

}
//---------------------------------------------------------------------------
string Selection::LessOrEqual::print(PlanPrinter& out)
    // Print the predicate (debugging only)
{
    return "(" + left->print(out) + ")<=(" + right->print(out) + ")";
}
//---------------------------------------------------------------------------
void Selection::Plus::eval(Result& result)
    // Evaluate the predicate
{
    Result l, r;
    left->eval(l);
    right->eval(r);

    l.ensureString(selection);
    r.ensureString(selection);
    stringstream s;
    s << (atof(l.value.c_str()) + atof(r.value.c_str()));
    result.setLiteral(s.str());
}
//---------------------------------------------------------------------------
string Selection::Plus::print(PlanPrinter& out)
    // Print the predicate (debugging only)
{
    return "(" + left->print(out) + ")+(" + right->print(out) + ")";
}
//---------------------------------------------------------------------------
void Selection::Minus::eval(Result& result)
    // Evaluate the predicate
{
    Result l, r;
    left->eval(l);
    right->eval(r);

    l.ensureString(selection);
    r.ensureString(selection);
    stringstream s;
    s << (atof(l.value.c_str()) - atof(r.value.c_str()));
    result.setLiteral(s.str());
}
//---------------------------------------------------------------------------
string Selection::Minus::print(PlanPrinter& out)
    // Print the predicate (debugging only)
{
    return "(" + left->print(out) + ")-(" + right->print(out) + ")";
}
//---------------------------------------------------------------------------
void Selection::Mul::eval(Result& result)
    // Evaluate the predicate
{
    Result l, r;
    left->eval(l);
    right->eval(r);

    l.ensureString(selection);
    r.ensureString(selection);
    stringstream s;
    s << (atof(l.value.c_str())*atof(r.value.c_str()));
    result.setLiteral(s.str());
}
//---------------------------------------------------------------------------
string Selection::Mul::print(PlanPrinter& out)
    // Print the predicate (debugging only)
{
    return "(" + left->print(out) + ")*(" + right->print(out) + ")";
}
//---------------------------------------------------------------------------
void Selection::Div::eval(Result& result)
    // Evaluate the predicate
{
    Result l, r;
    left->eval(l);
    right->eval(r);

    l.ensureString(selection);
    r.ensureString(selection);
    stringstream s;
    s << (atof(l.value.c_str()) / atof(r.value.c_str()));
    result.setLiteral(s.str());
}
//---------------------------------------------------------------------------
string Selection::Div::print(PlanPrinter& out)
    // Print the predicate (debugging only)
{
    return "(" + left->print(out) + ")/(" + right->print(out) + ")";
}
//---------------------------------------------------------------------------
void Selection::Not::eval(Result& result)
    // Evaluate the predicate
{
    result.setBoolean(!input->check());
}
//---------------------------------------------------------------------------
string Selection::Not::print(PlanPrinter& out)
    // Print the predicate (debugging only)
{
    return "!" + input->print(out);
}
//---------------------------------------------------------------------------
void Selection::Neg::eval(Result& result)
    // Evaluate the predicate
{
    Result i;
    input->eval(i);

    i.ensureString(selection);
    stringstream s;
    s << (-atof(i.value.c_str()));
    result.setLiteral(s.str());
}
//---------------------------------------------------------------------------
string Selection::Neg::print(PlanPrinter& out)
    // Print the predicate (debugging only)
{
    return "-" + input->print(out);
}
//---------------------------------------------------------------------------
void Selection::Null::eval(Result& result)
    // Evaluate the predicate
{
    result.setId(~0u);
}
//---------------------------------------------------------------------------
string Selection::Null::print(PlanPrinter& /*out*/)
    // Print the predicate (debugging only)
{
    return "NULL";
}
//---------------------------------------------------------------------------
void Selection::False::eval(Result& result)
    // Evaluate the predicate
{
    result.setBoolean(false);
}
//---------------------------------------------------------------------------
string Selection::False::print(PlanPrinter& /*out*/)
    // Print the predicate (debugging only)
{
    return "false";
}
//---------------------------------------------------------------------------
void Selection::Variable::eval(Result& result)
    // Evaluate the predicate
{
    result.setId(reg->value);
}
//---------------------------------------------------------------------------
string Selection::Variable::print(PlanPrinter& out)
    // Print the predicate (debugging only)
{
    return out.formatRegister(reg);
}
//---------------------------------------------------------------------------
void Selection::ConstantLiteral::eval(Result& result)
    // Evaluate the predicate
{
    result.setId(id);
}
//---------------------------------------------------------------------------
string Selection::ConstantLiteral::print(PlanPrinter& out)
    // Print the predicate (debugging only)
{
    return out.formatValue(id);
}
//---------------------------------------------------------------------------
void Selection::TemporaryConstantLiteral::eval(Result& result)
    // Evaluate the predicate
{
    result.setLiteral(value);
}
//---------------------------------------------------------------------------
bool Selection::TemporaryConstantLiteral::check() {
    return true;
}
//---------------------------------------------------------------------------
string Selection::TemporaryConstantLiteral::print(PlanPrinter& /*out*/)
    // Print the predicate (debugging only)
{
    return value;
}
//---------------------------------------------------------------------------
void Selection::ConstantIRI::eval(Result& result)
    // Evaluate the predicate
{
    result.setId(id);
}
//---------------------------------------------------------------------------
string Selection::ConstantIRI::print(PlanPrinter& out)
    // Print the predicate (debugging only)
{
    return out.formatValue(id);
}
//---------------------------------------------------------------------------
void Selection::TemporaryConstantIRI::eval(Result& result)
    // Evaluate the predicate
{
    result.setIRI(value);
}
//---------------------------------------------------------------------------
string Selection::TemporaryConstantIRI::print(PlanPrinter& /*out*/)
    // Print the predicate (debugging only)
{
    return value;
}
//---------------------------------------------------------------------------
void Selection::FunctionCall::setSelection(Selection* s)
    // Set the selection
{
    Predicate::setSelection(s);
    for (vector<Predicate*>::iterator iter = args.begin(), limit = args.end(); iter != limit; ++iter)
        (*iter)->setSelection(s);
}
//---------------------------------------------------------------------------
void Selection::FunctionCall::eval(Result& result)
    // Evaluate the predicate
{
    result.setId(~0u); // XXX perform the call
}
//---------------------------------------------------------------------------
string Selection::FunctionCall::print(PlanPrinter& out)
    // Print the predicate (debugging only)
{
    string result = "<" + func + ">(";
    for (vector<Predicate*>::iterator iter = args.begin(), limit = args.end(); iter != limit; ++iter) {
        if (iter != args.begin())
            result += ",";
        result += (*iter)->print(out);
    }
    result += ")";
    return result;
}
//---------------------------------------------------------------------------
void Selection::BuiltinStr::eval(Result& result)
    // Evaluate the predicate
{
    input->eval(result);
    result.ensureString(selection);
    result.flags |= Result::typeAvailable;
    result.type = Type::Literal;
}
//---------------------------------------------------------------------------
string Selection::BuiltinStr::print(PlanPrinter& out)
    // Print the predicate (debugging only)
{
    return "str(" + input->print(out) + ")";
}
//---------------------------------------------------------------------------
void Selection::BuiltinXSD::eval(Result& result) {
    input->eval(result);
    result.ensureString(selection);
    result.flags |= Result::typeAvailable;
    result.type = Type::Decimal;
}
//---------------------------------------------------------------------------
string Selection::BuiltinXSD::print(PlanPrinter& out)
    // Print the predicate (debugging only)
{
    return "xsd:decimal(" + input->print(out) + ")";
}
//---------------------------------------------------------------------------
void Selection::BuiltinLang::eval(Result& result)
    // Evaluate the predicate
{
    Result i;
    input->eval(i);
    i.ensureType(selection);

    if (i.type != Type::CustomLanguage) {
        result.setLiteral("");
    } else {
        result.ensureSubType(selection);
        result.setLiteral(i.subTypeValue);
    }
}
//---------------------------------------------------------------------------
string Selection::BuiltinLang::print(PlanPrinter& out)
    // Print the predicate (debugging only)
{
    return "lang(" + input->print(out) + ")";
}
//---------------------------------------------------------------------------
void Selection::BuiltinLangMatches::eval(Result& result)
    // Evaluate the predicate
{
    Result l, r;
    left->eval(l);
    l.ensureType(selection);

    if (l.type != Type::CustomLanguage) {
        result.setBoolean(false);
        return;
    }
    l.ensureSubType(selection);

    right->eval(r);
    r.ensureString(selection);

    result.setBoolean(l.subTypeValue == r.value); // XXX implement language range checks
}
//---------------------------------------------------------------------------
string Selection::BuiltinLangMatches::print(PlanPrinter& out)
    // Print the predicate (debugging only)
{
    return  "langMatches(" + left->print(out) + "," + right->print(out) + ")";
}
//---------------------------------------------------------------------------
void Selection::BuiltinContains::eval(Result& result)
    // Evaluate the predicate
{
    Result l, r;
    left->eval(l);
    right->eval(r);

    l.ensureString(selection);
    r.ensureString(selection);
    if (!l.hasString() || !r.hasString()) {
        result.setBoolean(false);
        return;
    }

    result.setBoolean(l.value.find(r.value) != std::string::npos);
}
//---------------------------------------------------------------------------
string Selection::BuiltinContains::print(PlanPrinter& out)
    // Print the predicate (debugging only)
{
    return  "contains(" + left->print(out) + "," + right->print(out) + ")";
}
//---------------------------------------------------------------------------
void Selection::BuiltinDatatype::eval(Result& result)
    // Evaluate the predicate
{
    Result i;
    input->eval(result);
    i.ensureType(selection);

    switch (i.type) {
        case Type::URI:
            result.setLiteral("http://www.w3.org/2001/XMLSchema#URI");
            break;
        case Type::Literal:
        case Type::CustomLanguage:
        case Type::String:
            result.setLiteral("http://www.w3.org/2001/XMLSchema#string");
            break;
        case Type::Integer:
            result.setLiteral("http://www.w3.org/2001/XMLSchema#integer");
            break;
        case Type::Decimal:
            result.setLiteral("http://www.w3.org/2001/XMLSchema#decimal");
            break;
        case Type::Double:
            result.setLiteral("http://www.w3.org/2001/XMLSchema#double");
            break;
        case Type::Boolean:
            result.setLiteral("http://www.w3.org/2001/XMLSchema#boolean");
            break;
        case Type::CustomType:
            i.ensureSubType(selection);
            result.setLiteral(i.subTypeValue);
            break;
        case Type::Date:
            result.setLiteral("http://www.w3.org/2001/XMLSchema#date");
            break;
    }
}
//---------------------------------------------------------------------------
string Selection::BuiltinDatatype::print(PlanPrinter& out)
    // Print the predicate (debugging only)
{
    return "datatype(" + input->print(out) + ")";
}
//---------------------------------------------------------------------------
void Selection::BuiltinBound::eval(Result& result)
    // Evaluate the predicate
{
    result.setBoolean(~reg->value);
}
//---------------------------------------------------------------------------
string Selection::BuiltinBound::print(PlanPrinter& out)
    // Print the predicate (debugging only)
{
    return "bound(" + out.formatRegister(reg) + ")";
}
//---------------------------------------------------------------------------
void Selection::BuiltinSameTerm::eval(Result& result)
    // Evaluate the predicate
{
    Result l, r;
    left->eval(l);
    right->eval(r);

    // Cheap case
    if (l.hasId() && r.hasId()) {
        result.setBoolean(l.id == r.id);
        return;
    }

    // Expensive tests
    l.ensureType(selection);
    r.ensureType(selection);
    if ((l.type != r.type) || (Type::hasSubType(l.type) && (l.subType != r.subType))) {
        result.setBoolean(false);
        return;
    }
    l.ensureString(selection);
    r.ensureString(selection);
    result.setBoolean(l.value == r.value);
}
//---------------------------------------------------------------------------
string Selection::BuiltinSameTerm::print(PlanPrinter& out)
    // Print the predicate (debugging only)
{
    return "sameTerm(" + left->print(out) + "," + right->print(out) + ")";
}
//---------------------------------------------------------------------------
void Selection::BuiltinIsIRI::eval(Result& result)
    // Evaluate the predicate
{
    Result i;
    input->eval(i);
    i.ensureType(selection);
    result.setBoolean(i.type == Type::URI);
}
//---------------------------------------------------------------------------
string Selection::BuiltinIsIRI::print(PlanPrinter& out)
    // Print the predicate (debugging only)
{
    return "isIRI(" + input->print(out) + ")";
}
//---------------------------------------------------------------------------
void Selection::BuiltinIsBlank::eval(Result& result)
    // Evaluate the predicate
{
    Result i;
    input->eval(i);
    i.ensureType(selection);
    if (i.type != Type::URI) {
        result.setBoolean(false);
    } else {
        i.ensureString(selection);
        result.setBoolean(i.value.substr(0, 2) == "_:");
    }
}
//---------------------------------------------------------------------------
string Selection::BuiltinIsBlank::print(PlanPrinter& out)
    // Print the predicate (debugging only)
{
    return "isBlanl(" + input->print(out) + ")";
}
//---------------------------------------------------------------------------
void Selection::BuiltinIsLiteral::eval(Result& result)
    // Evaluate the predicate
{
    Result i;
    input->eval(i);
    i.ensureType(selection);
    result.setBoolean(i.type != Type::URI);
}
//---------------------------------------------------------------------------
string Selection::BuiltinIsLiteral::print(PlanPrinter& out)
    // Print the predicate (debugging only)
{
    return "isLiteral(" + input->print(out) + ")";
}
//---------------------------------------------------------------------------
Selection::BuiltinRegEx::~BuiltinRegEx()
    // Destructor
{
    delete arg1;
    delete arg2;
    delete arg3;
}
//---------------------------------------------------------------------------
void Selection::BuiltinRegEx::setSelection(Selection* s)
    // Set the selection
{
    Predicate::setSelection(s);
    arg1->setSelection(s);
    arg2->setSelection(s);
    if (arg3) arg3->setSelection(s);
}
//---------------------------------------------------------------------------
void Selection::BuiltinRegEx::eval(Result& result)
    // Evaluate the predicate
{
    /*#ifdef CONFIG_TR1
      Result text,pattern;
      arg1->eval(text);
      arg2->eval(pattern);

      try {
      pattern.ensureString(selection);
      std::tr1::regex r(pattern.value.c_str());
      text.ensureString(selection);
      result.setBoolean(std::tr1::regex_match(text.value.begin(),text.value.end(),r));
      return;
      } catch (const std::tr1::regex_error&) {
      result.setBoolean(false);
      }
#else */
    result.setBoolean(false);
    //#endif
}
//---------------------------------------------------------------------------
void Selection::BuiltinReplace::eval(Result& result)
    // Evaluate the predicate
{
    input->eval(result);
    result.ensureString(selection);

    std::string stringToReplace = result.value;

    arg2->eval(result);
    std::string pattern = result.value;

    arg3->eval(result);
    std::string replacement = result.value;

    arg4->eval(result);
    std::string flags = result.value;

    //Replacement:
    result.value = stringToReplace;
    size_t f = result.value.find(pattern);
    if (f != string::npos) {
        result.value = result.value.replace(f, pattern.length(), replacement);
    }
    result.flags |= Result::typeAvailable;
    result.type = Type::Literal;
    result.ensureString(selection);
}
//---------------------------------------------------------------------------
string Selection::BuiltinRegEx::print(PlanPrinter& out)
    // Print the predicate (debugging only)
{
    string result = "regex(" + arg1->print(out) + "," + arg2->print(out);
    if (arg3) {
        result += ",";
        result += arg3->print(out);
    }
    result += ")";
    return result;
}
//---------------------------------------------------------------------------
Selection::BuiltinReplace::~BuiltinReplace()
    // Destructor
{
    delete input;
    delete arg2;
    delete arg3;
    delete arg4;
}
//---------------------------------------------------------------------------
void Selection::BuiltinReplace::setSelection(Selection* s)
    // Set the selection
{
    Predicate::setSelection(s);
    input->setSelection(s);
    arg2->setSelection(s);
    if (arg3) arg3->setSelection(s);
    if (arg4) arg4->setSelection(s);
}
//---------------------------------------------------------------------------
string Selection::BuiltinReplace::print(PlanPrinter& out)
    // Print the predicate (debugging only)
{
    string result = "replace(" + input->print(out) + "," + arg2->print(out);
    if (arg3) {
        result += ",";
        result += arg3->print(out);
    }
    result += ")";
    return result;
}
//---------------------------------------------------------------------------
void Selection::BuiltinIn::setSelection(Selection* s)
    // Set the selection
{
    Predicate::setSelection(s);
    probe->setSelection(s);
    for (vector<Predicate*>::iterator iter = args.begin(), limit = args.end(); iter != limit; ++iter)
        (*iter)->setSelection(s);
}
//---------------------------------------------------------------------------
void Selection::BuiltinIn::eval(Result& result)
    // Evaluate the predicate
{
    Result p, c;
    probe->eval(p);

    if (in) {
        for (vector<Predicate*>::iterator iter = args.begin(), limit = args.end(); iter != limit; ++iter) {
            (*iter)->eval(c);
            if (p.hasId() && c.hasId()) {
                if (p.id == c.id) {
                    result.setBoolean(true);
                    return;
                }
            } else {
                p.ensureString(selection);
                c.ensureString(selection);
                if (p.value == c.value) {
                    result.setBoolean(true);
                    return;
                }
            }
        }
        result.setBoolean(false);
    } else {
        p.ensureString(selection);
        if (strings.find(p.value) == strings.end()) {
            result.setBoolean(true);
        } else {
            result.setBoolean(false);
        }
    }
}
//---------------------------------------------------------------------------
string Selection::BuiltinIn::print(PlanPrinter& out)
    // Print the predicate (debugging only)
{
    string result = "in(" + probe->print(out);
    for (vector<Predicate*>::iterator iter = args.begin(), limit = args.end(); iter != limit; ++iter) {
        result += ",";
        result += (*iter)->print(out);
    }
    result += ")";
    return result;
}
//---------------------------------------------------------------------------
Selection::BuiltinNotExists::BuiltinNotExists(Operator *tree,
        std::vector<Register *> regsToLoad,
        std::vector<Register *> regsToCheck) :
    regsToLoad(regsToLoad), regsToCheck(regsToCheck), loaded(false)
{
    this->tree = std::unique_ptr<Operator>(tree);
    probe = std::unique_ptr<Predicate>(new Selection::Variable(regsToCheck[0]));
    if (regsToLoad.size() != 1)
        throw;
}
//---------------------------------------------------------------------------
void Selection::BuiltinNotExists::eval(Result& result)
    // Evaluate the predicate
{
    if (!loaded) {
        if (tree->first()) {
            do {
                set.insert(regsToLoad[0]->value);
            } while (tree->next());
        }
        loaded = true;
    }

    Result p, c;
    probe->eval(p); // The variable to be checked

    if (!set.count(p.id)) {
        result.setBoolean(true);
    } else {
        result.setBoolean(false);
    }
}
//---------------------------------------------------------------------------
string Selection::BuiltinNotExists::print(PlanPrinter& out)
    // Print the predicate (debugging only)
{
    string result = "not_exists(<subquery>)";
    return result;
}
//---------------------------------------------------------------------------
void Selection::AggrFunction::eval(Result& result)
    // Evaluate the predicate
{
    result.setId(reg->value);
    result.boolean = true;
    result.flags |= Result::booleanAvailable;
}
//---------------------------------------------------------------------------
string Selection::AggrFunction::print(PlanPrinter& out)
    // Print the predicate (debugging only)
{
    return out.formatRegister(reg);
}
//---------------------------------------------------------------------------
Selection::Selection(Operator* input, Runtime& runtime, Predicate* predicate, double expectedOutputCardinality)
    : Operator(expectedOutputCardinality), input(input), runtime(runtime), predicate(predicate)
      // Constructor
{
}
//---------------------------------------------------------------------------
Selection::~Selection()
    // Destructor
{
    delete predicate;
    delete input;
}
//---------------------------------------------------------------------------
uint64_t Selection::first()
    // Produce the first tuple
{
    observedOutputCardinality = 0;
    predicate->setSelection(this);

    // Get the first tuple
    uint64_t count = input->first();
    if (!count) return 0;

    // Match?
    if (predicate->check()) {
        observedOutputCardinality += count;
        return count;
    }

    // Get the next one
    return next();
}
//---------------------------------------------------------------------------
uint64_t Selection::next()
    // Produce the next tuple
{
    while (true) {
        // Retrieve the next tuple
        uint64_t count = input->next();
        if (!count) return 0;

        // Match?
        if (predicate->check()) {
            observedOutputCardinality += count;
            return count;
        }
    }
}
//---------------------------------------------------------------------------
void Selection::print(PlanPrinter& out)
    // Print the operator tree. Debugging only.
{
    out.beginOperator("Selection", expectedOutputCardinality, observedOutputCardinality);
    out.addGenericAnnotation(predicate->print(out));
    input->print(out);
    out.endOperator();
}
//---------------------------------------------------------------------------
void Selection::addMergeHint(Register* reg1, Register* reg2)
    // Add a merge join hint
{
    input->addMergeHint(reg1, reg2);
}
//---------------------------------------------------------------------------
void Selection::getAsyncInputCandidates(Scheduler& scheduler)
    // Register parts of the tree that can be executed asynchronous
{
    input->getAsyncInputCandidates(scheduler);
}
//---------------------------------------------------------------------------
