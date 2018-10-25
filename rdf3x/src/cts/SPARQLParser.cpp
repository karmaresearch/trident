#include "cts/parser/SPARQLParser.hpp"
#include "cts/parser/SPARQLLexer.hpp"

#include <kognac/logs.h>

#include <iostream>
#include <memory>
#include <cstdlib>
#include <set>

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
//---------------------------------------------------------------------------
SPARQLParser::ParserException::ParserException(const string& message)
    : message(message)
      // Constructor
{
}
//---------------------------------------------------------------------------
SPARQLParser::ParserException::ParserException(const char* message)
    : message(message)
      // Constructor
{
}
//---------------------------------------------------------------------------
SPARQLParser::ParserException::~ParserException()
    // Destructor
{
}
//---------------------------------------------------------------------------
SPARQLParser::Pattern::Pattern(Element subject, Element predicate, Element object)
    : subject(subject), predicate(predicate), object(object)
      // Constructor
{
}
//---------------------------------------------------------------------------
SPARQLParser::Pattern::~Pattern()
    // Destructor
{
}
//---------------------------------------------------------------------------
SPARQLParser::Filter::Filter()
    : arg1(0), arg2(0), arg3(0), arg4(0), valueArg(0), pointerToSubquery(0),
    pointerToSubPattern(0)
      // Constructor
{
}
//---------------------------------------------------------------------------
SPARQLParser::Filter::Filter(const Filter& other)
    : type(other.type), arg1(0), arg2(0), arg3(0), arg4(0), value(other.value), valueType(other.valueType), valueArg(other.valueArg), pointerToSubquery(other.pointerToSubquery), pointerToSubPattern(other.pointerToSubPattern)
      // Copy-Constructor
{
    if (other.arg1)
        arg1 = new Filter(*other.arg1);
    if (other.arg2)
        arg2 = new Filter(*other.arg2);
    if (other.arg3)
        arg3 = new Filter(*other.arg3);
    if (other.arg4)
        arg4 = new Filter(*other.arg4);
}
//---------------------------------------------------------------------------
SPARQLParser::Filter::~Filter()
    // Destructor
{
    if (arg1)
        delete arg1;
    if (arg2)
        delete arg2;
    if (arg3)
        delete arg3;
    if (arg4)
        delete arg4;
}
//---------------------------------------------------------------------------
SPARQLParser::Filter& SPARQLParser::Filter::operator=(const Filter& other)
    // Assignment
{
    if (this != &other) {
        type = other.type;
        delete arg1;
        if (other.arg1)
            arg1 = new Filter(*other.arg1);
        else
            arg1 = 0;
        delete arg2;
        if (other.arg2)
            arg2 = new Filter(*other.arg2);
        else
            arg2 = 0;
        delete arg3;
        if (other.arg3)
            arg3 = new Filter(*other.arg3);
        else
            arg3 = 0;
        if (other.arg4)
            arg4 = new Filter(*other.arg4);
        else
            arg4 = 0;
        value = other.value;
        value = other.value;
        valueType = other.valueType;
        valueArg = other.valueArg;
    }
    return *this;
}
//---------------------------------------------------------------------------
SPARQLParser::SPARQLParser(SPARQLLexer& lexer)
    : lexer(lexer), variableCount(0), namedParentVariables(NULL), projectionModifier(Modifier_None), limit(~0u), offset(0)
      // Constructor
{
}
SPARQLParser::SPARQLParser(SPARQLLexer& lexer,
        std::map<std::string, std::string> prefixes) : SPARQLParser(lexer) {
    this->prefixes = prefixes;
}

SPARQLParser::SPARQLParser(SPARQLLexer& lexer, std::map<std::string, std::string> prefixes,
        std::map<std::string, unsigned> *pv, unsigned nv) : SPARQLParser(lexer) {
    this->prefixes = prefixes;
    this->namedParentVariables = pv;
    this->variableCount = nv;
    // For now:
    this->namedVariables = *pv;
}
//---------------------------------------------------------------------------
SPARQLParser::~SPARQLParser()
    // Destructor
{
}
//---------------------------------------------------------------------------
unsigned SPARQLParser::nameVariable(const string& name)
    // Lookup or create a named variable
{
    if (namedVariables.count(name)) {
        LOG(DEBUGL) << "Found variable " << name << ", value = " << namedVariables[name];
        return namedVariables[name];
    }

    LOG(DEBUGL) << "New variable " << name << ", value = " << variableCount;
    unsigned result = variableCount++;
    namedVariables[name] = result;
    return result;
}
//---------------------------------------------------------------------------
void SPARQLParser::parsePrefix()
    // Parse the prefix part if any
{
    while (true) {
        SPARQLLexer::Token token = lexer.getNext();

        if ((token == SPARQLLexer::Identifier) && (lexer.isKeyword("prefix"))) {
            // Parse the prefix entry
            if (lexer.getNext() != SPARQLLexer::Identifier)
                throw ParserException("prefix name expected");
            string name = lexer.getTokenValue();

            SPARQLLexer::Token token = lexer.getNext();
            if (token != SPARQLLexer::Colon) {
                while (token != SPARQLLexer::Colon &&
                        lexer.getTokenValue() == "-") {
                    //We give a new name
                    token = lexer.getNext();
                    name = name + string("-") + lexer.getTokenValue();
                    token = lexer.getNext();
                }
                if (token != SPARQLLexer::Colon)
                    throw ParserException("':' expected");
            }

            if (lexer.getNext() != SPARQLLexer::IRI)
                throw ParserException("IRI expected");
            string iri = lexer.getIRIValue();

            // Register the new prefix
            if (prefixes.count(name))
                throw ParserException("duplicate prefix '" + name + "'");
            prefixes[name] = iri;
        } else {
            lexer.unget(token);
            return;
        }
    }
}
//---------------------------------------------------------------------------
string _filter2string(SPARQLParser::Filter *filter) {
    string out;
    switch (filter->type) {
        case SPARQLParser::Filter::Aggregate_count:
            out += "COUNT(";
            out += _filter2string(filter->arg1);
            out += ")";
            break;
        case SPARQLParser::Filter::Aggregate_min:
            out += "MIN(";
            out += _filter2string(filter->arg1);
            out += ")";
            break;
        case SPARQLParser::Filter::Aggregate_max:
            out += "MAX(";
            out += _filter2string(filter->arg1);
            out += ")";
            break;
        case SPARQLParser::Filter::Aggregate_avg:
            out += "AVG(";
            out += _filter2string(filter->arg1);
            out += ")";
            break;
        case SPARQLParser::Filter::Aggregate_sum:
            out += "SUM(";
            out += _filter2string(filter->arg1);
            out += ")";
            break;
        case SPARQLParser::Filter::Variable:
            out += filter->value;
            break;
        default:
            out += "UNKNOWN";
    }
    return out;
}
void SPARQLParser::parseProjection(PatternGroup& group)
    // Parse the projection
{
    // Parse the projection
    if ((lexer.getNext() != SPARQLLexer::Identifier) ||
            ((!lexer.isKeyword("select")) && (!lexer.isKeyword("construct"))))
        throw ParserException("'select' expected");

    if (lexer.isKeyword("construct")) {
        //Parse the pattern group
        SPARQLLexer::Token token = lexer.getNext();
        if (token != SPARQLLexer::LCurly) {
            //It can be only the keyword where
            if ((token != SPARQLLexer::Identifier) ||
                    (!lexer.isKeyword("where")))
                throw ParserException("'where' expected");

            if (lexer.getNext() != SPARQLLexer::LCurly)
                throw ParserException("'{' expected");
        }

        constructPatterns = PatternGroup();
        parseGroupGraphPattern(constructPatterns);

        //Now I get all variables in my pattern.
        //These are the projection variables I need
        for (std::vector<Pattern>::iterator itr = constructPatterns.patterns.begin();
                itr != constructPatterns.patterns.end(); ++itr) {
            //Get the vars from the pattern
            if (itr->subject.type == SPARQLParser::Element::Type::Variable) {
                unsigned idVar = itr->subject.id;
                bool found = false;
                for (std::vector<unsigned>::iterator itr2 = projection.begin();
                        itr2 != projection.end(); ++itr2) {
                    if (*itr2 == idVar) {
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    projection.push_back(idVar);
                }
            }

            if (itr->predicate.type == SPARQLParser::Element::Type::Variable) {
                unsigned idVar = itr->predicate.id;
                bool found = false;
                for (std::vector<unsigned>::iterator itr2 = projection.begin();
                        itr2 != projection.end(); ++itr2) {
                    if (*itr2 == idVar) {
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    projection.push_back(idVar);
                }
            }

            if (itr->object.type == SPARQLParser::Element::Type::Variable) {
                unsigned idVar = itr->object.id;
                bool found = false;
                for (std::vector<unsigned>::iterator itr2 = projection.begin();
                        itr2 != projection.end(); ++itr2) {
                    if (*itr2 == idVar) {
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    projection.push_back(idVar);
                }
            }
        }
    } else { //SELECT
        // Parse modifiers, if any
        {
            SPARQLLexer::Token token = lexer.getNext();
            if (token == SPARQLLexer::Identifier) {
                if (lexer.isKeyword("distinct")) projectionModifier = Modifier_Distinct;
                else if (lexer.isKeyword("reduced")) projectionModifier = Modifier_Reduced;
                else if (lexer.isKeyword("count")) projectionModifier = Modifier_Count;
                else if (lexer.isKeyword("duplicates")) projectionModifier = Modifier_Duplicates;
                else
                    lexer.unget(token);
            } else lexer.unget(token);
        }

        // Parse the projection clause
        bool first = true;

        //I print in the stderr because I want to parse it later on
        if (!silentOutputVars)
            std::cerr << "OUTPUT: ";

        while (true) {
            SPARQLLexer::Token token = lexer.getNext();
            if (token == SPARQLLexer::Variable) {
                if (!silentOutputVars)
                    std::cerr << lexer.getTokenValue() << "\t";
                projection.push_back(nameVariable(lexer.getTokenValue()));
            } else if (token == SPARQLLexer::Mul) {
                // We do nothing here. Empty projections will be filled with all
                // named variables after parsing
            } else if (token == SPARQLLexer::LParen) {
                lexer.unget(token);
                size_t sizeAssignments = assignments.size();
                parseAssignment(group, assignments);
                if (assignments.size() <= sizeAssignments) {
                    LOG(ERRORL) << "The parsing of the assignment did not go so well";
                    throw 10; //Exception. This should not occur
                }
                unsigned varid = assignments[sizeAssignments].outputVar.id;
                projection.push_back(varid);
                if (!silentOutputVars) {
                    for(auto &p : namedVariables) {
                        if (p.second == varid)
                            std::cerr << p.first << "\t";
                    }
                }
            } else if (token == SPARQLLexer::Identifier &&
                    (lexer.getTokenValue() != "where" && lexer.getTokenValue() != "WHERE")) {
                lexer.unget(token);
                map<string, unsigned> localVars;
                Assignment assignment;
                auto express = parseExpression(localVars);
                assignment.expression = std::shared_ptr<Filter>(express);
                Element el;
                el.type = Element::Variable;
                string stringRep = _filter2string(assignment.expression.get());
                el.id = nameVariable(stringRep);
                assignment.outputVar = el;
                //Add the assignment
                assignments.push_back(assignment);
                projection.push_back(el.id);
                if (!silentOutputVars) {
                    for(auto &p : namedVariables) {
                        if (p.second == el.id)
                            std::cerr << p.first << "\t";
                    }
                }
            } else {
                if (first)
                    throw ParserException("projection required after select");
                lexer.unget(token);
                break;
            }
            first = false;
        }
        if (!silentOutputVars)
            std::cerr << std::endl;
    }

}
//---------------------------------------------------------------------------
void SPARQLParser::parseFrom()
    // Parse the from part if any
{
    while (true) {
        SPARQLLexer::Token token = lexer.getNext();

        if ((token == SPARQLLexer::Identifier) && (lexer.isKeyword("from"))) {
            throw ParserException("from clause currently not implemented");
        } else {
            lexer.unget(token);
            return;
        }
    }
}
//---------------------------------------------------------------------------
void SPARQLParser::parseRDFLiteral(std::string& value, Element::SubType& subType, std::string& valueType)
    // Parse an RDF literal
{
    if (lexer.getNext() != SPARQLLexer::String)
        throw ParserException("literal expected");
    subType = Element::None;
    value = lexer.getLiteralValue();
    if (value.find('\\') != string::npos) {
        string v;
        v.swap(value);
        for (string::const_iterator iter = v.begin(), ilimit = v.end(); iter != ilimit; ++iter) {
            char c = (*iter);
            if (c == '\\') {
                if ((++iter) == ilimit) break;
                c = *iter;
            }
            value += c;
        }
    }

    SPARQLLexer::Token token = lexer.getNext();
    if (token == SPARQLLexer::At) {
        if (lexer.getNext() != SPARQLLexer::Identifier)
            throw ParserException("language tag expected after '@'");
        subType = Element::CustomLanguage;
        valueType = lexer.getTokenValue();
    } else if (token == SPARQLLexer::Type) {
        token = lexer.getNext();
        if (token == SPARQLLexer::Identifier) {
            string prefix = lexer.getTokenValue();
            // prefix:suffix
            SPARQLLexer::Token t = lexer.getNext();
            if (t != SPARQLLexer::Colon) {
                //There could be an hypen
                if (lexer.getTokenValue() == "-") {
                    //Read the remaining as prefix
                    lexer.getNext();
                    prefix = prefix + string("-") + lexer.getTokenValue();
                }
                if (lexer.getNext() != SPARQLLexer::Colon)
                    throw ParserException("':' expected after '" + prefix + "'");
            }

            if (!prefixes.count(prefix))
                throw ParserException("unknown prefix '" + prefix + "'");
            if (lexer.getNext() != SPARQLLexer::Identifier)
                throw ParserException("identifier expected after ':'");
            subType = Element::CustomType;
            valueType = prefixes[prefix] + lexer.getIRIValue();
            LOG(ERRORL) << "Token: valueType = " << valueType << ", value = " << value;

        } else if (token == SPARQLLexer::IRI) {
            subType = Element::CustomType;
            valueType = lexer.getIRIValue();
        } else {
            throw ParserException("type URI expected after '^^'");
        }
    } else {
        lexer.unget(token);
    }
}
//---------------------------------------------------------------------------
SPARQLParser::Filter* SPARQLParser::parseIRIrefOrFunction(std::map<std::string, unsigned>& localVars, bool mustCall)
    // Parse a "IRIrefOFunction" production
{
    // The IRI
    if (lexer.getNext() != SPARQLLexer::IRI)
        throw ParserException("IRI expected");
    unique_ptr<Filter> result(new Filter);
    result->type = Filter::IRI;
    result->value = lexer.getIRIValue();

    // Arguments?
    if (lexer.hasNext(SPARQLLexer::LParen)) {
        lexer.getNext();
        unique_ptr<Filter> call(new Filter);
        call->type = Filter::Function;
        call->arg1 = result.release();
        if (lexer.hasNext(SPARQLLexer::RParen)) {
            lexer.getNext();
        } else {
            unique_ptr<Filter> args(new Filter);
            Filter* tail = args.get();
            tail->type = Filter::ArgumentList;
            tail->arg1 = parseExpression(localVars);
            while (true) {
                if (lexer.hasNext(SPARQLLexer::Comma)) {
                    lexer.getNext();
                    tail = tail->arg2 = new Filter;
                    tail->type = Filter::ArgumentList;
                    tail->arg1 = parseExpression(localVars);
                } else {
                    if (lexer.getNext() != SPARQLLexer::RParen)
                        throw ParserException("')' expected");
                    break;
                }
            }
            call->arg2 = args.release();
        }

        result = std::move(call);
    } else if (mustCall) {
        throw ParserException("'(' expected");
    }

    return result.release();
}
//---------------------------------------------------------------------------
SPARQLParser::Filter* SPARQLParser::parseBuiltInCall(std::map<std::string, unsigned>& localVars)
    // Parse a "BuiltInCall" production
{
    if (lexer.getNext() != SPARQLLexer::Identifier)
        throw ParserException("function or aggregate name expected");

    unique_ptr<Filter> result(new Filter);
    bool aggregate = false;
    if (lexer.isKeyword("MIN")) {
        aggregate = true;
        result->type = Filter::Aggregate_min;
    } else if (lexer.isKeyword("SUM")) {
        aggregate = true;
        result->type = Filter::Aggregate_sum;
    } else if (lexer.isKeyword("MAX")) {
        aggregate = true;
        result->type = Filter::Aggregate_max;
    } else if (lexer.isKeyword("AVG")) {
        aggregate = true;
        result->type = Filter::Aggregate_avg;
    } else if (lexer.isKeyword("SAMPLE")) {
        aggregate = true;
        result->type = Filter::Aggregate_sample;
    } else if (lexer.isKeyword("COUNT")) {
        aggregate = true;
        result->type = Filter::Aggregate_count;
    } else if (lexer.isKeyword("GROUP_CONCAT")) {
        aggregate = true;
        result->type = Filter::Aggregate_group_concat;
    }
    if (aggregate) {
        // '('
        if (lexer.getNext() != SPARQLLexer::LParen) {
            throw ParserException("'(' expected");
        }
        // Optional DISTINCT
        auto tkn = lexer.getNext();
        if (lexer.isKeyword("DISTINCT")) {
            result->distinct = true;
        } else {
            lexer.unget(tkn);
        }
        bool exprDone = false;
        if (result->type == Filter::Aggregate_count) {
            // May be '*'
            auto tkn = lexer.getNext();
            if (tkn == SPARQLLexer::Mul) {
                exprDone = true;
                // arg1 remains NULL.
            } else {
                lexer.unget(tkn);
            }
        }
        if (! exprDone) {
            result->arg1 = parseExpression(localVars);
            if (result->type == Filter::Aggregate_group_concat) {
                auto tkn = lexer.getNext();
                if (tkn == SPARQLLexer::Semicolon) {
                    lexer.getNext();
                    if (! lexer.isKeyword("SEPARATOR")) {
                        throw ParserException("'Separator' expected");
                    }
                    if (lexer.getNext() != SPARQLLexer::Equal) {
                        throw ParserException("'=' expected");
                    }
                    if (lexer.getNext() != SPARQLLexer::String) {
                        throw ParserException("string expected");
                    }
                    result->value = lexer.getLiteralValue();
                } else {
                    lexer.unget(tkn);
                }
            }
        }
        // ')'
        if (lexer.getNext() != SPARQLLexer::RParen)
            throw ParserException("')' expected");
    } else if (lexer.isKeyword("STR")) {
        result->type = Filter::Builtin_str;
        result->arg1 = parseBrackettedExpression(localVars);
    } else if (lexer.isKeyword("xsd")) {
        //conversion function
        if (lexer.getNext() != SPARQLLexer::Colon) {
            throw ParserException("':' expected");
        }
        lexer.getNext();
        if (lexer.isKeyword("decimal")) {
            result->type = Filter::Builtin_xsddecimal;
            result->arg1 = parseBrackettedExpression(localVars);
        } else if (lexer.isKeyword("double")) {
            result->type = Filter::Builtin_xsddecimal;
            result->arg1 = parseBrackettedExpression(localVars);
        } else if (lexer.isKeyword("float")) {
            result->type = Filter::Builtin_xsddecimal;
            result->arg1 = parseBrackettedExpression(localVars);
        } else if (lexer.isKeyword("string")) {
            result->type = Filter::Builtin_xsdstring;
            result->arg1 = parseBrackettedExpression(localVars);
        } else {
            throw ParserException("unknown function '" + lexer.getTokenValue() + "'");
        }
    } else if (lexer.isKeyword("LANG")) {
        result->type = Filter::Builtin_lang;
        result->arg1 = parseBrackettedExpression(localVars);
    } else if (lexer.isKeyword("CONTAINS")) {
        result->type = Filter::Builtin_contains;
        if (lexer.getNext() != SPARQLLexer::LParen)
            throw ParserException("'(' expected");
        result->arg1 = parseExpression(localVars);
        if (lexer.getNext() != SPARQLLexer::Comma)
            throw ParserException("',' expected");
        result->arg2 = parseExpression(localVars);
        if (lexer.getNext() != SPARQLLexer::RParen)
            throw ParserException("')' expected");
    } else if (lexer.isKeyword("LANGMATCHES")) {
        result->type = Filter::Builtin_langmatches;
        if (lexer.getNext() != SPARQLLexer::LParen)
            throw ParserException("'(' expected");
        result->arg1 = parseExpression(localVars);
        if (lexer.getNext() != SPARQLLexer::Comma)
            throw ParserException("',' expected");
        result->arg2 = parseExpression(localVars);
        if (lexer.getNext() != SPARQLLexer::RParen)
            throw ParserException("')' expected");
    } else if (lexer.isKeyword("DATATYPE")) {
        result->type = Filter::Builtin_datatype;
        result->arg1 = parseBrackettedExpression(localVars);
    } else if (lexer.isKeyword("BOUND")) {
        result->type = Filter::Builtin_bound;
        if (lexer.getNext() != SPARQLLexer::LParen)
            throw ParserException("'(' expected");
        if (lexer.getNext() != SPARQLLexer::Variable)
            throw ParserException("variable expected as argument to BOUND");
        unique_ptr<Filter> arg(new Filter());
        arg->type = Filter::Variable;
        arg->valueArg = nameVariable(lexer.getTokenValue());
        if (lexer.getNext() != SPARQLLexer::RParen)
            throw ParserException("')' expected");
        result->arg1 = arg.release();
    } else if (lexer.isKeyword("sameTerm")) {
        result->type = Filter::Builtin_sameterm;
        if (lexer.getNext() != SPARQLLexer::LParen)
            throw ParserException("'(' expected");
        result->arg1 = parseExpression(localVars);
        if (lexer.getNext() != SPARQLLexer::Comma)
            throw ParserException("',' expected");
        result->arg2 = parseExpression(localVars);
        if (lexer.getNext() != SPARQLLexer::RParen)
            throw ParserException("')' expected");
    } else if (lexer.isKeyword("isIRI")) {
        result->type = Filter::Builtin_isiri;
        result->arg1 = parseBrackettedExpression(localVars);
    } else if (lexer.isKeyword("isURI")) {
        result->type = Filter::Builtin_isiri;
        result->arg1 = parseBrackettedExpression(localVars);
    } else if (lexer.isKeyword("isBLANK")) {
        result->type = Filter::Builtin_isblank;
        result->arg1 = parseBrackettedExpression(localVars);
    } else if (lexer.isKeyword("isLITERAL")) {
        result->type = Filter::Builtin_isliteral;
        result->arg1 = parseBrackettedExpression(localVars);
    } else if (lexer.isKeyword("REGEX")) {
        result->type = Filter::Builtin_regex;
        if (lexer.getNext() != SPARQLLexer::LParen)
            throw ParserException("'(' expected");
        result->arg1 = parseExpression(localVars);
        if (lexer.getNext() != SPARQLLexer::Comma)
            throw ParserException("',' expected");
        result->arg2 = parseExpression(localVars);
        if (lexer.hasNext(SPARQLLexer::Comma)) {
            lexer.getNext();
            result->arg3 = parseExpression(localVars);
        }
        if (lexer.getNext() != SPARQLLexer::RParen)
            throw ParserException("')' expected");
    } else if (lexer.isKeyword("REPLACE")) {
        result->type = Filter::Builtin_replace;
        if (lexer.getNext() != SPARQLLexer::LParen)
            throw ParserException("'(' expected");
        result->arg1 = parseExpression(localVars);
        if (lexer.getNext() != SPARQLLexer::Comma)
            throw ParserException("',' expected");
        result->arg2 = parseExpression(localVars);
        if (lexer.hasNext(SPARQLLexer::Comma)) {
            lexer.getNext();
            result->arg3 = parseExpression(localVars);
        }
        if (lexer.hasNext(SPARQLLexer::Comma)) {
            lexer.getNext();
            result->arg4 = parseExpression(localVars);
        }
        if (lexer.getNext() != SPARQLLexer::RParen)
            throw ParserException("')' expected");

    } else if (lexer.isKeyword("in")) {
        result->type = Filter::Builtin_in;
        if (lexer.getNext() != SPARQLLexer::LParen)
            throw ParserException("'(' expected");
        result->arg1 = parseExpression(localVars);

        if (lexer.hasNext(SPARQLLexer::RParen)) {
            lexer.getNext();
        } else {
            if (lexer.getNext() != SPARQLLexer::Comma)
                throw ParserException("',' expected");
            unique_ptr<Filter> args(new Filter);
            Filter* tail = args.get();
            tail->type = Filter::ArgumentList;
            tail->arg1 = parseExpression(localVars);
            while (true) {
                if (lexer.hasNext(SPARQLLexer::Comma)) {
                    lexer.getNext();
                    tail = tail->arg2 = new Filter;
                    tail->type = Filter::ArgumentList;
                    tail->arg1 = parseExpression(localVars);
                } else {
                    if (lexer.getNext() != SPARQLLexer::RParen)
                        throw ParserException("')' expected");
                    break;
                }
            }
            result->arg2 = args.release();
        }
    } else if (lexer.isKeyword("NOT")) {
        if (lexer.getNext() == SPARQLLexer::Identifier && lexer.isKeyword("EXISTS")) {
            result->type = Filter::Builtin_notexists;
            auto tkn1 = lexer.getNext();
            if (tkn1 != SPARQLLexer::LCurly)
                throw ParserException("'{' expected");
            auto tkn = lexer.getNext();
            if (lexer.isKeyword("select")) {
                lexer.unget(tkn);
                SPARQLParser *newSubquery = new SPARQLParser(lexer, prefixes,
                        &namedVariables, variableCount);
                newSubquery->parse(true);
                variableCount = newSubquery->variableCount;
                namedVariables = newSubquery->namedVariables;
                result->pointerToSubquery = std::shared_ptr<SPARQLParser>(newSubquery);

                if (lexer.hasNext(SPARQLLexer::RCurly)) {
                    lexer.getNext();
                } else {
                    if (lexer.getNext() != SPARQLLexer::Comma)
                        throw ParserException("',' expected");
                    unique_ptr<Filter> args(new Filter);
                    Filter* tail = args.get();
                    tail->type = Filter::ArgumentList;
                    tail->arg1 = parseExpression(localVars);
                    while (true) {
                        if (lexer.hasNext(SPARQLLexer::Comma)) {
                            lexer.getNext();
                            tail = tail->arg2 = new Filter;
                            tail->type = Filter::ArgumentList;
                            tail->arg1 = parseExpression(localVars);
                        } else {
                            if (lexer.getNext() != SPARQLLexer::RParen)
                                throw ParserException("'}' expected");
                            break;
                        }
                    }
                    result->arg2 = args.release();
                }
            } else {
                //It's a series of triple patterns
                lexer.unget(tkn);
                std::shared_ptr<PatternGroup> group = std::shared_ptr<PatternGroup>(
                        new PatternGroup);
                parseGroupGraphPattern(*group.get());
                result->pointerToSubPattern = group;
            }



        } else {
            throw ParserException("unknown function '" + lexer.getTokenValue() + "'");
        }

        /*} else if (lexer.isKeyword("NOT") && lexer.getNext() == SPARQLLexer::Identifier && lexer.isKeyword("in")) {
          result->type = Filter::Builtin_notin;
          if (lexer.getNext() != SPARQLLexer::LParen)
          throw ParserException("'(' expected");
          result->arg1 = parseExpression(localVars);

          if (lexer.hasNext(SPARQLLexer::RParen)) {
          lexer.getNext();
          } else {
          if (lexer.getNext() != SPARQLLexer::Comma)
          throw ParserException("',' expected");
          auto_ptr<Filter> args(new Filter);
          Filter* tail = args.get();
          tail->type = Filter::ArgumentList;
          tail->arg1 = parseExpression(localVars);
          while (true) {
          if (lexer.hasNext(SPARQLLexer::Comma)) {
          lexer.getNext();
          tail = tail->arg2 = new Filter;
          tail->type = Filter::ArgumentList;
          tail->arg1 = parseExpression(localVars);
          } else {
          if (lexer.getNext() != SPARQLLexer::RParen)
          throw ParserException("')' expected");
          break;
          }
          }
          result->arg2 = args.release();
          }*/
} else {
    LOG(INFOL) << "token value is " << lexer.getTokenValue();
    throw ParserException("unknown function '" + lexer.getTokenValue() + "'");
}

return result.release();
}
//---------------------------------------------------------------------------
SPARQLParser::Filter* SPARQLParser::parsePrimaryExpression(map<string, unsigned>& localVars)
    // Parse a "PrimaryExpression" production
{
    SPARQLLexer::Token token = lexer.getNext();

    if (token == SPARQLLexer::LParen) {
        lexer.unget(token);
        return parseBrackettedExpression(localVars);
    }
    if (token == SPARQLLexer::Identifier) {
        //It could be the prefix of a IRI
        if (prefixes.find(lexer.getTokenValue()) != prefixes.end()) {
            std::string prefix = lexer.getTokenValue();
            if (prefix == "xsd") {
                // Hack .... --Ceriel
                lexer.unget(token);
                return parseBuiltInCall(localVars);
            }
            SPARQLLexer::Token dotToken = lexer.getNext();
            if (dotToken != SPARQLLexer::Token::Colon) {
                throw ParserException("Expected :");
            }
            lexer.getNext();
            unique_ptr<Filter> result(new Filter);
            result->type = Filter::IRI;
            result->value = prefixes.find(prefix)->second + lexer.getTokenValue();
            return result.release();
        } else {
            if (lexer.isKeyword("true")) {
                unique_ptr<Filter> result(new Filter);
                result->type = Filter::Literal;
                result->value = "true";
                result->valueType = "http://www.w3.org/2001/XMLSchema#boolean";
                result->valueArg = Element::CustomType;
                return result.release();
            } else if (lexer.isKeyword("false")) {
                unique_ptr<Filter> result(new Filter);
                result->type = Filter::Literal;
                result->value = "false";
                result->valueType = "http://www.w3.org/2001/XMLSchema#boolean";
                result->valueArg = Element::CustomType;
                return result.release();
            }
            lexer.unget(token);
            return parseBuiltInCall(localVars);
        }
    }
    if (token == SPARQLLexer::IRI) {
        lexer.unget(token);
        return parseIRIrefOrFunction(localVars, false);
    }
    if (token == SPARQLLexer::String) {
        lexer.unget(token);
        unique_ptr<Filter> result(new Filter);
        result->type = Filter::Literal;
        Element::SubType type;
        parseRDFLiteral(result->value, type, result->valueType);
        result->valueArg = type;
        return result.release();
    }
    if (token == SPARQLLexer::Integer) {
        unique_ptr<Filter> result(new Filter);
        result->type = Filter::Literal;
        result->value = lexer.getTokenValue();
        result->valueType = "http://www.w3.org/2001/XMLSchema#integer";
        result->valueArg = Element::CustomType;
        return result.release();
    }
    if (token == SPARQLLexer::Decimal) {
        unique_ptr<Filter> result(new Filter);
        result->type = Filter::Literal;
        result->value = lexer.getTokenValue();
        result->valueType = "http://www.w3.org/2001/XMLSchema#decimal";
        result->valueArg = Element::CustomType;
        return result.release();
    }
    if (token == SPARQLLexer::Double) {
        unique_ptr<Filter> result(new Filter);
        result->type = Filter::Literal;
        result->value = lexer.getTokenValue();
        result->valueType = "http://www.w3.org/2001/XMLSchema#double";
        result->valueArg = Element::CustomType;
        return result.release();
    }
    if (token == SPARQLLexer::Variable) {
        unique_ptr<Filter> result(new Filter);
        result->type = Filter::Variable;
        result->value = lexer.getTokenValue();
        result->valueArg = nameVariable(result->value);
        return result.release();
    }
    throw ParserException("syntax error in primary expression");
}
//---------------------------------------------------------------------------
SPARQLParser::Filter* SPARQLParser::parseUnaryExpression(map<string, unsigned>& localVars)
    // Parse a "UnaryExpression" production
{
    SPARQLLexer::Token token = lexer.getNext();

    if (token == SPARQLLexer::Not) {
        unique_ptr<Filter> result(new Filter);
        result->type = Filter::Not;
        result->arg1 = parsePrimaryExpression(localVars);
        return result.release();
    } else if (token == SPARQLLexer::Plus) {
        unique_ptr<Filter> result(new Filter);
        result->type = Filter::UnaryPlus;
        result->arg1 = parsePrimaryExpression(localVars);
        return result.release();
    } else if (token == SPARQLLexer::Minus) {
        unique_ptr<Filter> result(new Filter);
        result->type = Filter::UnaryMinus;
        result->arg1 = parsePrimaryExpression(localVars);
        return result.release();
    } else {
        lexer.unget(token);
        return parsePrimaryExpression(localVars);
    }
}
//---------------------------------------------------------------------------
SPARQLParser::Filter* SPARQLParser::parseMultiplicativeExpression(map<string, unsigned>& localVars)
    // Parse a "MultiplicativeExpression" production
{
    unique_ptr<Filter> result(parseUnaryExpression(localVars));

    // op *
    while (true) {
        SPARQLLexer::Token token = lexer.getNext();
        if ((token == SPARQLLexer::Mul) || (token == SPARQLLexer::Div)) {
            unique_ptr<Filter> right(parseUnaryExpression(localVars));

            unique_ptr<Filter> newEntry(new Filter);
            switch (token) {
                case SPARQLLexer::Mul:
                    newEntry->type = Filter::Mul;
                    break;
                case SPARQLLexer::Div:
                    newEntry->type = Filter::Div;
                    break;
                default:
                    throw; // cannot happen
            }
            newEntry->arg1 = result.release();
            newEntry->arg2 = right.release();
            result = std::move(newEntry);
        } else {
            lexer.unget(token);
            break;
        }
    }
    return result.release();
}
//---------------------------------------------------------------------------
SPARQLParser::Filter* SPARQLParser::parseAdditiveExpression(map<string, unsigned>& localVars)
    // Parse a "AdditiveExpression" production
{
    unique_ptr<Filter> result(parseMultiplicativeExpression(localVars));

    // op *
    while (true) {
        SPARQLLexer::Token token = lexer.getNext();
        if ((token == SPARQLLexer::Plus) || (token == SPARQLLexer::Minus)) {
            unique_ptr<Filter> right(parseMultiplicativeExpression(localVars));

            unique_ptr<Filter> newEntry(new Filter);
            switch (token) {
                case SPARQLLexer::Plus:
                    newEntry->type = Filter::Plus;
                    break;
                case SPARQLLexer::Minus:
                    newEntry->type = Filter::Minus;
                    break;
                default:
                    throw; // cannot happen
            }
            newEntry->arg1 = result.release();
            newEntry->arg2 = right.release();
            result = std::move(newEntry);
        } else {
            lexer.unget(token);
            break;
        }
    }
    return result.release();
}
//---------------------------------------------------------------------------
SPARQLParser::Filter* SPARQLParser::parseNumericExpression(map<string, unsigned>& localVars)
    // Parse a "NumericExpression" production
{
    return parseAdditiveExpression(localVars);
}
//---------------------------------------------------------------------------
SPARQLParser::Filter* SPARQLParser::parseRelationalExpression(map<string, unsigned>& localVars)
    // Parse a "RelationalExpression" production
{
    unique_ptr<Filter> result(parseNumericExpression(localVars));

    // op *
    while (true) {
        SPARQLLexer::Token token = lexer.getNext();
        if ((token == SPARQLLexer::Equal) || (token == SPARQLLexer::NotEqual) || (token == SPARQLLexer::Less) || (token == SPARQLLexer::LessOrEqual) || (token == SPARQLLexer::Greater) || (token == SPARQLLexer::GreaterOrEqual)) {
            unique_ptr<Filter> right(parseNumericExpression(localVars));

            unique_ptr<Filter> newEntry(new Filter);
            switch (token) {
                case SPARQLLexer::Equal:
                    newEntry->type = Filter::Equal;
                    break;
                case SPARQLLexer::NotEqual:
                    newEntry->type = Filter::NotEqual;
                    break;
                case SPARQLLexer::Less:
                    newEntry->type = Filter::Less;
                    break;
                case SPARQLLexer::LessOrEqual:
                    newEntry->type = Filter::LessOrEqual;
                    break;
                case SPARQLLexer::Greater:
                    newEntry->type = Filter::Greater;
                    break;
                case SPARQLLexer::GreaterOrEqual:
                    newEntry->type = Filter::GreaterOrEqual;
                    break;
                default:
                    throw; // cannot happen
            }
            newEntry->arg1 = result.release();
            newEntry->arg2 = right.release();
            result = std::move(newEntry);
        } else {
            //Can be IN or NOT IN
            if (token == SPARQLLexer::Identifier) {
                if (lexer.isKeyword("IN")) {
                    throw ParserException("To be implemented");
                } else if (lexer.isKeyword("NOT")) {
                    SPARQLLexer::Token t2 = lexer.getNext();
                    if (t2 == SPARQLLexer::Identifier && lexer.isKeyword("IN")) {
                        unique_ptr<Filter> newEntry(new Filter);
                        newEntry->type = Filter::Builtin_notin;
                        if (lexer.getNext() != SPARQLLexer::LParen)
                            throw ParserException("'(' expected");

                        PatternGroup group;
                        if (lexer.hasNext(SPARQLLexer::RParen)) {
                            lexer.getNext();
                        } else {
                            //Element element = parsePatternElement(group, localVars);
                            //result->value = element.value;
                            //if (lexer.getNext() != SPARQLLexer::Comma)
                            //    throw ParserException("',' expected");
                            unique_ptr<Filter> args(new Filter);
                            Filter* tail = args.get();
                            tail->type = Filter::ArgumentList;
                            Element element = parsePatternElement(group, localVars);
                            tail->value = element.value;
                            while (true) {
                                if (lexer.hasNext(SPARQLLexer::Comma)) {
                                    lexer.getNext();
                                    tail = tail->arg1 = new Filter;
                                    tail->type = Filter::ArgumentList;
                                    Element element = parsePatternElement(group, localVars);
                                    tail->value = element.value;
                                } else {
                                    if (lexer.getNext() != SPARQLLexer::RParen)
                                        throw ParserException("')' expected");
                                    break;
                                }
                            }
                            newEntry->arg1 = result.release();
                            newEntry->arg2 = args.release();
                            result = std::move(newEntry);
                            break;
                        }
                    } else {
                        lexer.unget(t2);
                        lexer.unget(token);
                        break;
                    }
                } else {
                    lexer.unget(token);
                    break;
                }
            } else {
                lexer.unget(token);
                break;
            }
        }
    }
    return result.release();
}
//---------------------------------------------------------------------------
SPARQLParser::Filter* SPARQLParser::parseValueLogical(map<string, unsigned>& localVars)
    // Parse a "ValueLogical" production
{
    return parseRelationalExpression(localVars);
}
//---------------------------------------------------------------------------
SPARQLParser::Filter* SPARQLParser::parseConditionalAndExpression(map<string, unsigned>& localVars)
    // Parse a "ConditionalAndExpression" production
{
    unique_ptr<Filter> result(parseValueLogical(localVars));

    // && *
    while (lexer.hasNext(SPARQLLexer::And)) {
        if (lexer.getNext() != SPARQLLexer::And)
            throw ParserException("'&&' expected");
        unique_ptr<Filter> right(parseValueLogical(localVars));

        unique_ptr<Filter> newEntry(new Filter);
        newEntry->type = Filter::And;
        newEntry->arg1 = result.release();
        newEntry->arg2 = right.release();

        result = std::move(newEntry);
    }
    return result.release();
}
//---------------------------------------------------------------------------
SPARQLParser::Filter* SPARQLParser::parseConditionalOrExpression(map<string, unsigned>& localVars)
    // Parse a "ConditionalOrExpression" production
{
    unique_ptr<Filter> result(parseConditionalAndExpression(localVars));

    // || *
    while (lexer.hasNext(SPARQLLexer::Or)) {
        if (lexer.getNext() != SPARQLLexer::Or)
            throw ParserException("'||' expected");
        unique_ptr<Filter> right(parseConditionalAndExpression(localVars));

        unique_ptr<Filter> newEntry(new Filter);
        newEntry->type = Filter::Or;
        newEntry->arg1 = result.release();
        newEntry->arg2 = right.release();

        result = std::move(newEntry);
    }
    return result.release();
}
//---------------------------------------------------------------------------
SPARQLParser::Filter* SPARQLParser::parseExpression(map<string, unsigned>& localVars)
    // Parse a "Expression" production
{
    return parseConditionalOrExpression(localVars);
}
//---------------------------------------------------------------------------
SPARQLParser::Filter* SPARQLParser::parseBrackettedExpression(map<string, unsigned>& localVars)
    // Parse a "BrackettedExpression" production
{
    // '('
    if (lexer.getNext() != SPARQLLexer::LParen)
        throw ParserException("'(' expected");

    // Expression
    unique_ptr<Filter> result(parseExpression(localVars));

    // ')'
    if (lexer.getNext() != SPARQLLexer::RParen)
        throw ParserException("')' expected");

    return result.release();
}
//---------------------------------------------------------------------------
SPARQLParser::Filter* SPARQLParser::parseConstraint(map<string, unsigned>& localVars)
    // Parse a "Constraint" production
{
    // Check possible productions
    if (lexer.hasNext(SPARQLLexer::LParen))
        return parseBrackettedExpression(localVars);
    if (lexer.hasNext(SPARQLLexer::Identifier))
        return parseBuiltInCall(localVars);
    if (lexer.hasNext(SPARQLLexer::IRI))
        return parseIRIrefOrFunction(localVars, true);

    // Report an error
    throw ParserException("filter constraint expected");
}
//---------------------------------------------------------------------------
void SPARQLParser::parseMinus(PatternGroup & group)
    // Parse a filter condition
{
    //Can be a list of patterns or a subselect query
    SPARQLLexer::Token token = lexer.getNext();
    if (token == SPARQLLexer::LCurly) {
        PatternGroup subgroup;
        parseGroupGraphPattern(subgroup);
        group.minuses.push_back(subgroup);
    } else {
        throw ParserException("expected {");
    }

}
//---------------------------------------------------------------------------
void SPARQLParser::parseFilter(PatternGroup & group, map<string, unsigned>& localVars)
    // Parse a filter condition
{
    Filter* entry = parseConstraint(localVars);
    group.filters.push_back(*entry);

    if (lexer.hasNext(SPARQLLexer::Dot)) {
        lexer.getNext();
    }

    delete entry;
}
//---------------------------------------------------------------------------
SPARQLParser::Element SPARQLParser::parseBlankNode(PatternGroup & group, map<string, unsigned>& localVars)
    // Parse blank node patterns
{
    // The subject is a blank node
    Element subject;
    subject.type = Element::Variable;
    subject.id = variableCount++;

    // Parse the the remaining part of the pattern
    SPARQLParser::Element predicate = parsePatternElement(group, localVars);
    SPARQLParser::Element object = parsePatternElement(group, localVars);
    group.patterns.push_back(Pattern(subject, predicate, object));

    // Check for the tail
    while (true) {
        SPARQLLexer::Token token = lexer.getNext();
        if (token == SPARQLLexer::Semicolon) {
            predicate = parsePatternElement(group, localVars);
            object = parsePatternElement(group, localVars);
            group.patterns.push_back(Pattern(subject, predicate, object));
            continue;
        } else if (token == SPARQLLexer::Comma) {
            object = parsePatternElement(group, localVars);
            group.patterns.push_back(Pattern(subject, predicate, object));
            continue;
        } else if (token == SPARQLLexer::Dot) {
            return subject;
        } else if (token == SPARQLLexer::RBracket) {
            lexer.unget(token);
            return subject;
        } else if (token == SPARQLLexer::Identifier) {
            if (!lexer.isKeyword("filter"))
                throw ParserException("'filter' expected, not " + lexer.getTokenValue());
            parseFilter(group, localVars);
            continue;
        } else {
            // Error while parsing, let out caller handle it
            lexer.unget(token);
            return subject;
        }
    }
}
//---------------------------------------------------------------------------
SPARQLParser::Element SPARQLParser::parsePatternElement(PatternGroup & group, map<string, unsigned>& localVars)
    // Parse an entry in a pattern
{
    Element result;
    SPARQLLexer::Token token = lexer.getNext();
    if (token == SPARQLLexer::Variable) {
        result.type = Element::Variable;
        result.id = nameVariable(lexer.getTokenValue());
    } else if (token == SPARQLLexer::String) {
        result.type = Element::Literal;
        lexer.unget(token);
        parseRDFLiteral(result.value, result.subType, result.subTypeValue);
    } else if (token == SPARQLLexer::IRI) {
        result.type = Element::IRI;
        result.value = lexer.getIRIValue();
    } else if (token == SPARQLLexer::Anon) {
        result.type = Element::Variable;
        result.id = variableCount++;
    } else if (token == SPARQLLexer::LBracket) {
        result = parseBlankNode(group, localVars);
        if (lexer.getNext() != SPARQLLexer::RBracket)
            throw ParserException("']' expected");
    } else if (token == SPARQLLexer::Underscore) {
        // _:variable
        if (lexer.getNext() != SPARQLLexer::Colon)
            throw ParserException("':' expected");
        if (lexer.getNext() != SPARQLLexer::Identifier)
            throw ParserException("identifier expected after ':'");
        result.type = Element::Variable;
        if (localVars.count(lexer.getTokenValue()))
            result.id = localVars[lexer.getTokenValue()];
        else
            result.id = localVars[lexer.getTokenValue()] = variableCount++;
    } else if (token == SPARQLLexer::Colon) {
        // :identifier. Should reference the base
        if (lexer.getNext() != SPARQLLexer::Identifier)
            throw ParserException("identifier expected after ':'");
        result.type = Element::IRI;
        result.value = lexer.getTokenValue();
    } else if (token == SPARQLLexer::Identifier) {
        string prefix = lexer.getTokenValue();
        // Handle the keyword 'a'
        if (prefix == "a") {
            result.type = Element::IRI;
            result.value = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
        } else {
            // prefix:suffix
            SPARQLLexer::Token t = lexer.getNext();
            if (t != SPARQLLexer::Colon) {
                //There could be an hypen
                if (lexer.getTokenValue() == "-") {
                    //Read the remaining as prefix
                    lexer.getNext();
                    prefix = prefix + string("-") + lexer.getTokenValue();
                }
                if (lexer.getNext() != SPARQLLexer::Colon)
                    throw ParserException("':' expected after '" + prefix + "'");
            }

            if (!prefixes.count(prefix))
                throw ParserException("unknown prefix '" + prefix + "'");
            if (lexer.getNext() != SPARQLLexer::Identifier)
                throw ParserException("identifier expected after ':'");
            result.type = Element::IRI;
            result.value = prefixes[prefix] + lexer.getIRIValue();
        }
    } else {
        throw ParserException("invalid pattern element");
    }
    return result;
}
//---------------------------------------------------------------------------
void SPARQLParser::parseGraphPattern(PatternGroup & group)
    // Parse a graph pattern
{
    map<string, unsigned> localVars;

    // Parse the first pattern
    Element subject = parsePatternElement(group, localVars);
    Element predicate = parsePatternElement(group, localVars);
    Element object = parsePatternElement(group, localVars);
    group.patterns.push_back(Pattern(subject, predicate, object));

    // Check for the tail
    while (true) {
        SPARQLLexer::Token token = lexer.getNext();
        if (token == SPARQLLexer::Semicolon) {
            predicate = parsePatternElement(group, localVars);
            object = parsePatternElement(group, localVars);
            group.patterns.push_back(Pattern(subject, predicate, object));
            continue;
        } else if (token == SPARQLLexer::Comma) {
            object = parsePatternElement(group, localVars);
            group.patterns.push_back(Pattern(subject, predicate, object));
            continue;
        } else if (token == SPARQLLexer::Dot) {
            return;
        } else if (token == SPARQLLexer::RCurly) {
            lexer.unget(token);
            return;
        } else if (token == SPARQLLexer::Identifier) {
            if (lexer.isKeyword("optional")) {
                lexer.getNext();
                PatternGroup optionalGroup;
                parseGroupGraphPattern(optionalGroup);
                group.optional.push_back(optionalGroup);

                //Remove a potential trailing .
                if (lexer.hasNext(SPARQLLexer::Token::Dot)) {
                    lexer.getNext();
                }
            } else if (lexer.isKeyword("filter")) {
                parseFilter(group, localVars);
            } else if (lexer.isKeyword("bind")) {
                parseAssignment(group, group.assignments);
            } else if (lexer.isKeyword("values")) {
                parseValues(group);
            }
            else if (token == SPARQLLexer::Identifier
                    && lexer.isKeyword("minus")) {
                parseMinus(group);
            } else {
                throw ParserException("Unexpected: " + lexer.getTokenValue());
            }
        } else {
            // Error while parsing, let our caller handle it
            lexer.unget(token);
            return;
        }
    }
}
//---------------------------------------------------------------------------
void SPARQLParser::parseGroupGraphPattern(PatternGroup & group)
    // Parse a group of patterns
{
    while (true) {
        SPARQLLexer::Token token = lexer.getNext();

        if (lexer.isKeyword("select")) {
            lexer.unget(token);

            SPARQLParser *newSubquery = new SPARQLParser(lexer, prefixes, &namedVariables, variableCount);
            newSubquery->parse(true);
            group.subqueries.push_back(std::shared_ptr<SPARQLParser>(newSubquery));
            variableCount = newSubquery->variableCount;
            namedVariables = newSubquery->namedVariables;

            //The last token should be the RCurly
            if (lexer.getNext() != SPARQLLexer::RCurly) {
                throw ParserException("'}' expected");
            }
            return;

        } else if (lexer.isKeyword("optional")) {
            lexer.getNext();
            PatternGroup optionalGroup;
            parseGroupGraphPattern(optionalGroup);
            group.optional.push_back(optionalGroup);

            //Remove a potential trailing .
            if (lexer.hasNext(SPARQLLexer::Token::Dot)) {
                lexer.getNext();
            }
        } else if (token == SPARQLLexer::LCurly) {

            // Parse the group
            PatternGroup newGroup;
            parseGroupGraphPattern(newGroup);

            // Union statement?
            token = lexer.getNext();
            if ((token == SPARQLLexer::Identifier) && (lexer.isKeyword("union"))) {
                group.unions.push_back(vector<PatternGroup>());
                vector<PatternGroup>& currentUnion = group.unions.back();
                currentUnion.push_back(newGroup);
                while (true) {
                    if (lexer.getNext() != SPARQLLexer::LCurly)
                        throw ParserException("'{' expected");
                    PatternGroup subGroup;
                    parseGroupGraphPattern(subGroup);
                    currentUnion.push_back(subGroup);

                    // Another union?
                    token = lexer.getNext();
                    if ((token == SPARQLLexer::Identifier) && (lexer.isKeyword("union")))
                        continue;
                    break;
                }
            } else {
                // No, simply merge it
                group.patterns.insert(group.patterns.end(), newGroup.patterns.begin(), newGroup.patterns.end());
                group.filters.insert(group.filters.end(), newGroup.filters.begin(), newGroup.filters.end());
                group.optional.insert(group.optional.end(), newGroup.optional.begin(), newGroup.optional.end());
                group.unions.insert(group.unions.end(), newGroup.unions.begin(), newGroup.unions.end());
                group.subqueries.insert(group.subqueries.end(), newGroup.subqueries.begin(), newGroup.subqueries.end());
            }

            if (token != SPARQLLexer::Dot)
                lexer.unget(token);

        } else if ((token == SPARQLLexer::IRI) ||
                (token == SPARQLLexer::Variable) ||
                (token == SPARQLLexer::Identifier) ||
                (token == SPARQLLexer::String) ||
                (token == SPARQLLexer::Underscore) ||
                (token == SPARQLLexer::Colon) ||
                (token == SPARQLLexer::LBracket) ||
                (token == SPARQLLexer::Anon)) {
            // Distinguish filter conditions
            if ((token == SPARQLLexer::Identifier)
                    && (lexer.isKeyword("filter"))) {
                map<string, unsigned> localVars;
                parseFilter(group, localVars);
            } else if (token == SPARQLLexer::Identifier
                    && lexer.isKeyword("bind")) {
                parseAssignment(group, group.assignments);
            } else if (token == SPARQLLexer::Identifier
                    && lexer.isKeyword("optional")) {
                // Parser Optional
                group.optional.push_back(PatternGroup());
                PatternGroup& optionalGroup = group.optional.back();
                parseGroupGraphPattern(optionalGroup);
            } else if (token == SPARQLLexer::Identifier
                    && lexer.isKeyword("minus")) {
                parseMinus(group);
            } else {
                lexer.unget(token);
                parseGraphPattern(group);
            }
        } else if (token == SPARQLLexer::RCurly) {
            break;
        } else {
            throw ParserException("'}' expected");
        }
    }
}
//---------------------------------------------------------------------------
void SPARQLParser::parseValues(PatternGroup & group) {
    //Parse the variable(s)
    map<string, unsigned> localVars;
    std::vector<unsigned> variables;
    std::vector<SPARQLParser::Element> values;
    SPARQLLexer::Token token = lexer.getNext();
    if (token == SPARQLLexer::Variable) {
        //Parse only one variable
        variables.push_back(nameVariable(lexer.getTokenValue()));
    } else {
        if (token != SPARQLLexer::LParen) {
            throw ParserException("Expected (");
        }
        token = lexer.getNext();
        while (token != SPARQLLexer::RParen) {
            if (token != SPARQLLexer::Variable) {
                throw ParserException("Expected a variable");
            }
            variables.push_back(nameVariable(lexer.getTokenValue()));
            token = lexer.getNext();
        }
    }
    //Parse the bindings
    token = lexer.getNext();
    if (token == SPARQLLexer::LCurly) {
        //Read the values
        SPARQLLexer::Token token = lexer.getNext();
        while (token != SPARQLLexer::RCurly) {
            if (token == SPARQLLexer::LParen) {
                //Expect a RParen
                unsigned id = 0;
                while (id < variables.size()) {
                    SPARQLParser::Element e = parsePatternElement(group, localVars);
                    values.push_back(e);
                    id += 1;
                }
                token = lexer.getNext();
                if (token != SPARQLLexer::RParen) {
                    throw ParserException("Expected )");
                }
                token = lexer.getNext();
            } else {
                lexer.unget(token);
                SPARQLParser::Element e = parsePatternElement(group, localVars);
                values.push_back(e);
                token = lexer.getNext();
            }
        }
    } else {
        throw ParserException("'{' expected");
    }
    group.values.push_back(SPARQLParser::PatternGroup::ValueBindings(variables, values));
}
//---------------------------------------------------------------------------
void SPARQLParser::parseAssignment(PatternGroup & group,
        std::vector<Assignment> &assignments) {
    map<string, unsigned> localVars;

    Assignment assignment;

    if (lexer.getNext() != SPARQLLexer::LParen) {
        throw ParserException("Expected (");
    }

    assignment.expression = std::shared_ptr<Filter>(parseExpression(localVars));

    if (!lexer.isKeyword("AS")) {
        throw ParserException("Expected AS");
    }
    lexer.getNext();

    assignment.outputVar = parsePatternElement(group, localVars);

    if (assignment.outputVar.type != Element::Variable) {
        throw ParserException("The expression should be assigned to a variable");
    }

    if (lexer.getNext() != SPARQLLexer::RParen) {
        throw ParserException("Expected )");
    }

    if (lexer.hasNext(SPARQLLexer::Dot)) {
        lexer.getNext(); //Ignore final point
    }

    assignments.push_back(assignment);
}
//---------------------------------------------------------------------------
void SPARQLParser::parseWhere()
    // Parse the where part if any
{
    SPARQLLexer::Token token = lexer.getNext();
    if (token != SPARQLLexer::LCurly) {
        //It can be only the keyword where
        if ((token != SPARQLLexer::Identifier) ||
                (!lexer.isKeyword("where")))
            throw ParserException("'where' expected");

        if (lexer.getNext() != SPARQLLexer::LCurly)
            throw ParserException("'{' expected");
    }

    patterns = PatternGroup();
    parseGroupGraphPattern(patterns);


}
//---------------------------------------------------------------------------
void SPARQLParser::parseGroupBy(std::map<std::string, unsigned>& localVars)
    // Parse the group by part if any
{
    SPARQLLexer::Token token = lexer.getNext();
    if ((token != SPARQLLexer::Identifier) || (!lexer.isKeyword("group"))) {
        lexer.unget(token);
        return;
    }
    if ((lexer.getNext() != SPARQLLexer::Identifier) || (!lexer.isKeyword("by")))
        throw ParserException("'by' expected");

    while(true) {
        GroupBy g;
        g.id = ~0u;
        g.expr = NULL;
        token = lexer.getNext();
        if (token == SPARQLLexer::Identifier) {
            if (lexer.isKeyword("having") || lexer.isKeyword("order") || lexer.isKeyword("limit")
                    || lexer.isKeyword("offset")) {
                lexer.unget(token);
                break;
            }
            lexer.unget(token);
            g.expr = parseBuiltInCall(localVars);
        } else if (token == SPARQLLexer::IRI) {
            lexer.unget(token);
            g.expr = SPARQLParser::parseIRIrefOrFunction(localVars, false);
        } else if (token == SPARQLLexer::Variable) {
            g.id = nameVariable(lexer.getTokenValue());
        } else if (token == SPARQLLexer::LParen) {
            g.expr = parseExpression(localVars);
            token = lexer.getNext();
            if (lexer.isKeyword("AS")) {
                lexer.getNext();
                if (token != SPARQLLexer::Variable) {
                    throw ParserException("variable expected after AS");
                }
                g.id = nameVariable(lexer.getTokenValue());
            }
            if (lexer.getNext() != SPARQLLexer::RParen) {
                throw ParserException("')' expected");
            }
        } else {
            lexer.unget(token);
            break;
        }
        groupBy.push_back(g);
    }
}
//---------------------------------------------------------------------------
void SPARQLParser::parseHaving(std::map<std::string, unsigned>& localVars)
    // Parse the having part if any
{
    SPARQLLexer::Token token = lexer.getNext();
    if ((token != SPARQLLexer::Identifier) || (!lexer.isKeyword("having"))) {
        lexer.unget(token);
        return;
    }
    while(true) {
        SPARQLParser::Filter* constraint = parseConstraint(localVars);
        having.push_back(constraint);
        token = lexer.getNext();
        lexer.unget(token);
        if (token != SPARQLLexer::LParen && token != SPARQLLexer::Identifier && token != SPARQLLexer::IRI) {
            break;
        }
    }
}
//---------------------------------------------------------------------------
void SPARQLParser::parseOrderBy(std::map<std::string, unsigned>& localVars)
    // Parse the order by part if any
{
    SPARQLLexer::Token token = lexer.getNext();
    if ((token != SPARQLLexer::Identifier) || (!lexer.isKeyword("order"))) {
        lexer.unget(token);
        return;
    }
    if ((lexer.getNext() != SPARQLLexer::Identifier) || (!lexer.isKeyword("by")))
        throw ParserException("'by' expected");

    while (true) {
        token = lexer.getNext();
        if (token == SPARQLLexer::Identifier) {
            if (lexer.isKeyword("limit") || lexer.isKeyword("offset")) {
                lexer.unget(token);
                break;
            }
            if (lexer.isKeyword("asc") || lexer.isKeyword("desc")) {
                Order o;
                o.descending = lexer.isKeyword("desc");
                o.expr = parseBrackettedExpression(localVars);
                order.push_back(o);
            } else if (lexer.isKeyword("count")) {
                Order o;
                o.id = ~0u;
                o.descending = false;
                order.push_back(o);
            } else {
                lexer.unget(token);
		Order o;
		o.descending = false;
		o.expr = parseConstraint(localVars);
		order.push_back(o);
            }
        } else if (token == SPARQLLexer::Variable) {
            Order o;
            o.id = nameVariable(lexer.getTokenValue());
            o.descending = false;
            order.push_back(o);
        } else if (token == SPARQLLexer::Eof) {
            lexer.unget(token);
            return;
        } else {
            throw ParserException("variable expected in order-by clause");
        }
    }
}
//---------------------------------------------------------------------------
void SPARQLParser::parseLimitOffset()
    // Parse the limit/offset part if any
{
    bool hadLimit = false;
    bool hadOffset = false;

    for (;;) {
	SPARQLLexer::Token token = lexer.getNext();
	if ((token == SPARQLLexer::Identifier) && (lexer.isKeyword("limit") || lexer.isKeyword("offset"))) {
	    if (lexer.isKeyword("limit")) {
		if (hadLimit) {
		    throw ParserException("limit specified twice");
		}
		hadLimit = true;
		if (lexer.getNext() != SPARQLLexer::Integer)
		    throw ParserException("number expected after 'limit'");
		limit = atoi(lexer.getTokenValue().c_str());
		if (limit <= 0)
		    throw ParserException("invalid limit specifier");
	    } else {
		if (hadOffset) {
		    throw ParserException("offset specified twice");
		}
		if (lexer.getNext() != SPARQLLexer::Integer)
		    throw ParserException("number expected after 'offset'");
		offset = atoi(lexer.getTokenValue().c_str());
		if (offset < 0)
		    throw ParserException("invalid offset specifier");
	    }
	} else {
	    lexer.unget(token);
	    return;
	}
	if (hadLimit && hadOffset) {
	    return;
	}
    }
}
//---------------------------------------------------------------------------
void SPARQLParser::parse(bool multiQuery, bool silentOutputVars)
    // Parse the input
{
    this->silentOutputVars = silentOutputVars;

    // Parse the prefix part
    parsePrefix();

    // Parse the projection
    parseProjection(patterns);

    // Parse the from clause
    parseFrom();

    // Parse the where clause
    parseWhere();

    // Parse the group by clause
    parseGroupBy(namedVariables);

    // Parse the having clause
    parseHaving(namedVariables);

    // Parse the order by clause
    parseOrderBy(namedVariables);

    // Parse the limit/offset clauses
    parseLimitOffset();

    // Check that the input is done
    if ((!multiQuery) && (lexer.getNext() != SPARQLLexer::Eof))
        throw ParserException("syntax error");

    // Fixup empty projections (i.e. *)
    if (!projection.size()) {
        for (map<string, unsigned>::const_iterator iter = namedVariables.begin(), ilimit = namedVariables.end(); iter != ilimit; ++iter)
            projection.push_back((*iter).second);
    }
}
//---------------------------------------------------------------------------
string SPARQLParser::getVariableName(unsigned id) const
// Get the name of a variable
{
    for (map<string, unsigned>::const_iterator iter = namedVariables.begin(), ilimit = namedVariables.end(); iter != ilimit; ++iter)
        if ((*iter).second == id)
            return (*iter).first;
    return "";
}
//---------------------------------------------------------------------------
unsigned SPARQLParser::getVarCount() const {
    return variableCount;
}
