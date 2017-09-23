#include <cts/semana/SemanticAnalysis.hpp>
#include <cts/parser/SPARQLParser.hpp>
#include <cts/infra/QueryGraph.hpp>
#include <rts/runtime/QueryDict.hpp>

#include <infra/util/Type.hpp>
#include <dblayer.hpp>

/*#include "rts/database/Database.hpp"
#include "rts/runtime/DifferentialIndex.hpp"
#include "rts/segment/DictionarySegment.hpp"
#include "rts/segment/FactsSegment.hpp"*/

#include <set>
#include <cassert>
#include <cstdlib>
#include <iostream>
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
/// ID used for table functions
static const char tableFunctionId[] = "http://www.mpi-inf.mpg.de/rdf3x/tableFunction";
static bool encodeNotExistsFilter(SemanticAnalysis *myself, QueryGraph::Filter::Type type, DBLayer& dict, DifferentialIndex* diffIndex, const SPARQLParser::PatternGroup& group, const SPARQLParser::Filter& input, QueryGraph::Filter& output, QueryDict &tempDict);
//---------------------------------------------------------------------------
SemanticAnalysis::SemanticException::SemanticException(const std::string& message)
    : message(message)
      // Constructor
{
}
//---------------------------------------------------------------------------
SemanticAnalysis::SemanticException::SemanticException(const char* message)
    : message(message)
      // Constructor
{
}
//---------------------------------------------------------------------------
SemanticAnalysis::SemanticException::~SemanticException()
    // Destructor
{
}
//---------------------------------------------------------------------------
SemanticAnalysis::SemanticAnalysis(DBLayer& dict, QueryDict &tempDict)
    : dict(dict), /*diffIndex(0),*/ tempDict(tempDict)
      // Constructor
{
}
//---------------------------------------------------------------------------
/*SemanticAnalysis::SemanticAnalysis(DifferentialIndex& diffIndex, QueryDict &tempDict)
  : dict(diffIndex.getDatabase().getDictionary()), diffIndex(&diffIndex) , tempDict(tempDict)
// Constructor
{
}*/
//---------------------------------------------------------------------------
static bool lookup(DBLayer& dict, DifferentialIndex* diffIndex, const std::string& text, ::Type::ID type, unsigned subType, uint64_t& id)
    // Perform a dictionary lookup
{
    if (dict.lookup(text, type, subType, id))
        return true;
    /*if ((diffIndex) && (diffIndex->lookup(text, type, subType, id)))
      return true;*/
    return false;
}
//---------------------------------------------------------------------------
static bool encode(DBLayer& dict, DifferentialIndex* diffIndex, const SPARQLParser::Element& element, uint64_t& id, bool& constant)
    // Encode an element for the query graph
{
    switch (element.type) {
        case SPARQLParser::Element::Variable:
            id = element.id;
            constant = false;
            return true;
        case SPARQLParser::Element::Literal:
            if (element.subType == SPARQLParser::Element::None) {
                if (lookup(dict, diffIndex, element.value, Type::Literal, 0, id)) {
                    constant = true;
                    return true;
                } else return false;
            } else if (element.subType == SPARQLParser::Element::CustomLanguage) {
                uint64_t languageId;
                if (!lookup(dict, diffIndex, element.subTypeValue, Type::Literal, 0, languageId))
                    return false;
                if (lookup(dict, diffIndex, element.value, Type::CustomLanguage, languageId, id)) {
                    constant = true;
                    return true;
                } else return false;
            } else if (element.subType == SPARQLParser::Element::CustomType) {
                Type::ID type;
                uint64_t subType = 0;
                if (element.subTypeValue == "http://www.w3.org/2001/XMLSchema#string") {
                    type = Type::String;
                } else if (element.subTypeValue == "http://www.w3.org/2001/XMLSchema#integer") {
                    type = Type::Integer;
                } else if (element.subTypeValue == "http://www.w3.org/2001/XMLSchema#date") {
                    type = Type::Date;
                } else if (element.subTypeValue == "http://www.w3.org/2001/XMLSchema#decimal") {
                    type = Type::Decimal;
                } else if (element.subTypeValue == "http://www.w3.org/2001/XMLSchema#double") {
                    type = Type::Double;
                } else if (element.subTypeValue == "http://www.w3.org/2001/XMLSchema#boolean") {
                    type = Type::Boolean;
                } else {
                    if (!lookup(dict, diffIndex, element.subTypeValue, Type::URI, 0, subType))
                        return false;
                    type = Type::CustomType;
                }
                if (lookup(dict, diffIndex, element.value, type, subType, id)) {
                    constant = true;
                    return true;
                } else return false;
            } else {
                return false;
            }
        case SPARQLParser::Element::IRI:
            if (lookup(dict, diffIndex, element.value, Type::URI, 0, id)) {
                constant = true;
                return true;
            } else return false;
    }
    return false;
}
//---------------------------------------------------------------------------
static bool binds(const SPARQLParser::PatternGroup& group, uint64_t id)
    // Check if a variable is bound in a pattern group
{
    for (std::vector<SPARQLParser::Pattern>::const_iterator iter = group.patterns.begin(), limit = group.patterns.end(); iter != limit; ++iter)
        if ((((*iter).subject.type == SPARQLParser::Element::Variable) && ((*iter).subject.id == id)) ||
                (((*iter).predicate.type == SPARQLParser::Element::Variable) && ((*iter).predicate.id == id)) ||
                (((*iter).object.type == SPARQLParser::Element::Variable) && ((*iter).object.id == id)))
            return true;
    for (std::vector<SPARQLParser::PatternGroup>::const_iterator iter = group.optional.begin(), limit = group.optional.end(); iter != limit; ++iter)
        if (binds(*iter, id))
            return true;
    for (std::vector<std::vector<SPARQLParser::PatternGroup> >::const_iterator iter = group.unions.begin(), limit = group.unions.end(); iter != limit; ++iter)
        for (std::vector<SPARQLParser::PatternGroup>::const_iterator iter2 = (*iter).begin(), limit2 = (*iter).end(); iter2 != limit2; ++iter2)
            if (binds(*iter2, id))
                return true;
    for (std::vector<SPARQLParser::Assignment>::const_iterator iter = group.assignments.begin();
            iter != group.assignments.end(); ++iter) {
        if (iter->outputVar.id == id)
            return true;
    }
    return false;
}
//---------------------------------------------------------------------------
static bool encodeFilter(SemanticAnalysis *myself, DBLayer& dict, DifferentialIndex* diffIndex, const SPARQLParser::PatternGroup& group, const SPARQLParser::Filter& input, QueryGraph::Filter& output, QueryDict &tempDict);
static bool encodeFilter(DBLayer& dict, DifferentialIndex* diffIndex, const SPARQLParser::PatternGroup& group, const SPARQLParser::Filter& input, QueryGraph::Filter& output, QueryDict &tempDict) {
    return encodeFilter(NULL, dict, diffIndex, group, input, output, tempDict);
}
//---------------------------------------------------------------------------
static bool encodeUnaryFilter(QueryGraph::Filter::Type type, DBLayer& dict, DifferentialIndex* diffIndex, const SPARQLParser::PatternGroup& group, const SPARQLParser::Filter& input, QueryGraph::Filter& output, QueryDict &tempDict)
    // Encode a unary filter element
{
    output.type = type;
    output.arg1 = new QueryGraph::Filter();
    return encodeFilter(dict, diffIndex, group, *input.arg1, *output.arg1, tempDict);
}
//---------------------------------------------------------------------------
static bool encodeBinaryFilter(QueryGraph::Filter::Type type, DBLayer& dict, DifferentialIndex* diffIndex, const SPARQLParser::PatternGroup& group, const SPARQLParser::Filter& input, QueryGraph::Filter& output, QueryDict &tempDict)
    // Encode a binary filter element
{
    output.type = type;
    output.value = input.value;
    if (input.arg1) {
        output.arg1 = new QueryGraph::Filter();
        if (!encodeFilter(dict, diffIndex, group, *input.arg1, *output.arg1, tempDict)) {
            return false;
        }
    }
    if (input.arg2) {
        output.arg2 = new QueryGraph::Filter();
        if (!encodeFilter(dict, diffIndex, group, *input.arg2, *output.arg2, tempDict))
            return false;
    }
    return true;
}
//---------------------------------------------------------------------------
static bool encodeTernaryFilter(QueryGraph::Filter::Type type, DBLayer& dict, DifferentialIndex* diffIndex, const SPARQLParser::PatternGroup& group, const SPARQLParser::Filter& input, QueryGraph::Filter& output, QueryDict &tempDict)
    // Encode a ternary filter element
{
    output.type = type;
    output.arg1 = new QueryGraph::Filter();
    output.arg2 = new QueryGraph::Filter();
    output.arg3 = (input.arg3) ? (new QueryGraph::Filter()) : 0;
    return encodeFilter(dict, diffIndex, group, *input.arg1, *output.arg1, tempDict) && encodeFilter(dict, diffIndex, group, *input.arg2, *output.arg2, tempDict) && ((!input.arg3) || encodeFilter(dict, diffIndex, group, *input.arg3, *output.arg3, tempDict));
}
//---------------------------------------------------------------------------
static bool encodeQuadernaryFilter(QueryGraph::Filter::Type type, DBLayer& dict, DifferentialIndex* diffIndex, const SPARQLParser::PatternGroup& group, const SPARQLParser::Filter& input, QueryGraph::Filter& output, QueryDict &tempDict)
    // Encode a quad filter element
{
    output.type = type;
    output.arg1 = new QueryGraph::Filter();
    output.arg2 = new QueryGraph::Filter();
    output.arg3 = (input.arg3) ? (new QueryGraph::Filter()) : 0;
    output.arg4 = (input.arg4) ? (new QueryGraph::Filter()) : 0;
    bool response = encodeFilter(dict, diffIndex, group, *input.arg1, *output.arg1, tempDict) && encodeFilter(dict, diffIndex, group, *input.arg2, *output.arg2, tempDict) && ((!input.arg3) || encodeFilter(dict, diffIndex, group, *input.arg3, *output.arg3, tempDict)) && ((!input.arg4) || encodeFilter(dict, diffIndex, group, *input.arg4, *output.arg4, tempDict));
    return response;
}
//---------------------------------------------------------------------------
static bool encodeFilter(SemanticAnalysis *myself, DBLayer& dict, DifferentialIndex* diffIndex, const SPARQLParser::PatternGroup& group, const SPARQLParser::Filter& input, QueryGraph::Filter& output, QueryDict &tempDict)
    // Encode an element for the query graph
{
    switch (input.type) {
        case SPARQLParser::Filter::Or:
            return encodeBinaryFilter(QueryGraph::Filter::Or, dict, diffIndex, group, input, output, tempDict);
        case SPARQLParser::Filter::And:
            return encodeBinaryFilter(QueryGraph::Filter::And, dict, diffIndex, group, input, output, tempDict);
        case SPARQLParser::Filter::Equal:
            return encodeBinaryFilter(QueryGraph::Filter::Equal, dict, diffIndex, group, input, output, tempDict);
        case SPARQLParser::Filter::NotEqual:
            return encodeBinaryFilter(QueryGraph::Filter::NotEqual, dict, diffIndex, group, input, output, tempDict);
        case SPARQLParser::Filter::Less:
            return encodeBinaryFilter(QueryGraph::Filter::Less, dict, diffIndex, group, input, output, tempDict);
        case SPARQLParser::Filter::LessOrEqual:
            return encodeBinaryFilter(QueryGraph::Filter::LessOrEqual, dict, diffIndex, group, input, output, tempDict);
        case SPARQLParser::Filter::Greater:
            return encodeBinaryFilter(QueryGraph::Filter::Greater, dict, diffIndex, group, input, output, tempDict);
        case SPARQLParser::Filter::GreaterOrEqual:
            return encodeBinaryFilter(QueryGraph::Filter::GreaterOrEqual, dict, diffIndex, group, input, output, tempDict);
        case SPARQLParser::Filter::Plus:
            return encodeBinaryFilter(QueryGraph::Filter::Plus, dict, diffIndex, group, input, output, tempDict);
        case SPARQLParser::Filter::Minus:
            return encodeBinaryFilter(QueryGraph::Filter::Minus, dict, diffIndex, group, input, output, tempDict);
        case SPARQLParser::Filter::Mul:
            return encodeBinaryFilter(QueryGraph::Filter::Mul, dict, diffIndex, group, input, output, tempDict);
        case SPARQLParser::Filter::Div:
            return encodeBinaryFilter(QueryGraph::Filter::Div, dict, diffIndex, group, input, output, tempDict);
        case SPARQLParser::Filter::Not:
            return encodeUnaryFilter(QueryGraph::Filter::Not, dict, diffIndex, group, input, output, tempDict);
        case SPARQLParser::Filter::UnaryPlus:
            return encodeUnaryFilter(QueryGraph::Filter::UnaryPlus, dict, diffIndex, group, input, output, tempDict);
        case SPARQLParser::Filter::UnaryMinus:
            return encodeUnaryFilter(QueryGraph::Filter::UnaryMinus, dict, diffIndex, group, input, output, tempDict);
        case SPARQLParser::Filter::Literal: {
                                                SPARQLParser::Element e;
                                                e.type = SPARQLParser::Element::Literal;
                                                e.subType = static_cast<SPARQLParser::Element::SubType>(input.valueArg);
                                                e.subTypeValue = input.valueType;
                                                e.value = input.value;
                                                uint64_t id;
                                                bool constant;
                                                if (encode(dict, diffIndex, e, id, constant)) {
                                                    output.type = QueryGraph::Filter::Literal;
                                                    output.id = id;
                                                    output.value = input.value;
                                                } else {
                                                    output.type = QueryGraph::Filter::Literal;
                                                    output.id = ~0u;
                                                    output.value = input.value;
                                                    //In this case the literal is completely new (and is not present in the KB.)
                                                    output.valueid = tempDict.add(output.value);
                                                }
                                            }
                                            return true;
        case SPARQLParser::Filter::Variable:
                                            if (binds(group, input.valueArg)) {
                                                output.type = QueryGraph::Filter::Variable;
                                                output.id = input.valueArg;
                                            } else {
                                                output.type = QueryGraph::Filter::Null;
                                            }
                                            return true;
        case SPARQLParser::Filter::IRI: {
                                            SPARQLParser::Element e;
                                            e.type = SPARQLParser::Element::IRI;
                                            e.subType = static_cast<SPARQLParser::Element::SubType>(input.valueArg);
                                            e.subTypeValue = input.valueType;
                                            e.value = input.value;
                                            uint64_t id;
                                            bool constant;
                                            if (encode(dict, diffIndex, e, id, constant)) {
                                                output.type = QueryGraph::Filter::IRI;
                                                output.id = id;
                                                output.value = input.value;
                                            } else {
                                                output.type = QueryGraph::Filter::IRI;
                                                output.id = ~0u;
                                                output.value = input.value;
                                            }
                                        }
                                        return true;
        case SPARQLParser::Filter::Function:
                                        if (input.arg1->value == tableFunctionId)
                                            throw SemanticAnalysis::SemanticException(std::string("<") + tableFunctionId + "> calls must be placed in seperate filter clauses");
                                        return encodeBinaryFilter(QueryGraph::Filter::Function, dict, diffIndex, group, input, output, tempDict);
        case SPARQLParser::Filter::ArgumentList:
                                        return encodeBinaryFilter(QueryGraph::Filter::ArgumentList, dict, diffIndex, group, input, output, tempDict);
        case SPARQLParser::Filter::Builtin_str:
                                        return encodeUnaryFilter(QueryGraph::Filter::Builtin_str, dict, diffIndex, group, input, output, tempDict);
        case SPARQLParser::Filter::Builtin_lang:
                                        return encodeUnaryFilter(QueryGraph::Filter::Builtin_lang, dict, diffIndex, group, input, output, tempDict);
        case SPARQLParser::Filter::Builtin_langmatches:
                                        return encodeBinaryFilter(QueryGraph::Filter::Builtin_langmatches, dict, diffIndex, group, input, output, tempDict);
        case SPARQLParser::Filter::Builtin_datatype:
                                        return encodeUnaryFilter(QueryGraph::Filter::Builtin_datatype, dict, diffIndex, group, input, output, tempDict);
        case SPARQLParser::Filter::Builtin_bound:
                                        return encodeUnaryFilter(QueryGraph::Filter::Builtin_bound, dict, diffIndex, group, input, output, tempDict);
        case SPARQLParser::Filter::Builtin_sameterm:
                                        return encodeBinaryFilter(QueryGraph::Filter::Builtin_sameterm, dict, diffIndex, group, input, output, tempDict);
        case SPARQLParser::Filter::Builtin_isiri:
                                        return encodeUnaryFilter(QueryGraph::Filter::Builtin_isiri, dict, diffIndex, group, input, output, tempDict);
        case SPARQLParser::Filter::Builtin_isblank:
                                        return encodeUnaryFilter(QueryGraph::Filter::Builtin_isblank, dict, diffIndex, group, input, output, tempDict);
        case SPARQLParser::Filter::Builtin_isliteral:
                                        return encodeUnaryFilter(QueryGraph::Filter::Builtin_isliteral, dict, diffIndex, group, input, output, tempDict);
        case SPARQLParser::Filter::Builtin_regex:
                                        return encodeTernaryFilter(QueryGraph::Filter::Builtin_regex, dict, diffIndex, group, input, output, tempDict);
        case SPARQLParser::Filter::Builtin_replace:
                                        return encodeQuadernaryFilter(QueryGraph::Filter::Builtin_replace, dict, diffIndex, group, input, output, tempDict);
        case SPARQLParser::Filter::Builtin_in:
                                        return encodeBinaryFilter(QueryGraph::Filter::Builtin_in, dict, diffIndex, group, input, output, tempDict);
        case SPARQLParser::Filter::Builtin_notin:
                                        return encodeBinaryFilter(QueryGraph::Filter::Builtin_notin, dict, diffIndex, group, input, output, tempDict);
        case SPARQLParser::Filter::Builtin_contains:
                                        return encodeBinaryFilter(QueryGraph::Filter::Builtin_contains, dict, diffIndex, group, input, output, tempDict);
        case SPARQLParser::Filter::Builtin_xsddecimal:
                                        return encodeUnaryFilter(QueryGraph::Filter::Builtin_xsddecimal, dict, diffIndex, group, input, output, tempDict);
        case SPARQLParser::Filter::Builtin_notexists:
                                        return encodeNotExistsFilter(myself, QueryGraph::Filter::Builtin_notexists, dict, diffIndex, group, input, output, tempDict);
    }
    return false; // XXX cannot happen
}
//---------------------------------------------------------------------------
static bool encodeFilter(SemanticAnalysis *myself, DBLayer& dict, DifferentialIndex* diffIndex, const SPARQLParser::PatternGroup& group, const SPARQLParser::Filter& input, QueryGraph::SubQuery& output, QueryDict &tempQueryDict)
    // Encode an element for the query graph
{
    // Handle and separately to be more flexible
    if (input.type == SPARQLParser::Filter::And) {
        if (!encodeFilter(myself, dict, diffIndex, group, *input.arg1, output, tempQueryDict))
            return false;
        if (!encodeFilter(myself, dict, diffIndex, group, *input.arg2, output, tempQueryDict))
            return false;
        return true;
    }

    // Encode recursively
    output.filters.push_back(QueryGraph::Filter());
    return encodeFilter(myself, dict, diffIndex, group, input, output.filters.back(), tempQueryDict);
}
//---------------------------------------------------------------------------
static bool encodeAssignment(SemanticAnalysis *myself, DBLayer& dict, DifferentialIndex* diffIndex,
        const SPARQLParser::PatternGroup& group,
        const SPARQLParser::Assignment &input, QueryGraph::SubQuery& output, QueryDict &tempQueryDict) {
    output.tableFunctions.resize(output.tableFunctions.size() + 1);
    QueryGraph::TableFunction& func = output.tableFunctions.back();
    func.name = "BIND";
    encodeFilter(myself, dict, diffIndex, group, *input.expression, output, tempQueryDict);

    //Copy the filter
    func.associatedFilter = std::shared_ptr<QueryGraph::Filter>(new QueryGraph::Filter(output.filters.back()));
    output.filters.pop_back();
    std::set<std::pair<uint64_t, bool> > allVars = func.associatedFilter->allIdVarsAndLiterals();
    for (std::set<std::pair<uint64_t, bool> >::iterator itr = allVars.begin();
            itr != allVars.end(); ++itr) {
        QueryGraph::TableFunction::Argument arg;
        if (itr->second) { //It's a variable
            arg.id = itr->first;
            arg.idvalue = 0;
        } else { //It's a literal
            arg.id = ~0u;
            arg.idvalue = itr->first;
        }
        func.input.push_back(arg);
    }

    // Set up the output
    func.output.resize(1);
    func.output.back() = input.outputVar.id;
    return true;
}

//---------------------------------------------------------------------------
static void encodeTableFunction(const SPARQLParser::PatternGroup& /*group*/, const SPARQLParser::Filter& input, QueryGraph::SubQuery& output)
    // Produce a table function call
{
    // Collect all arguments
    std::vector<SPARQLParser::Filter*> args;
    for (SPARQLParser::Filter* iter = input.arg2; iter; iter = iter->arg2)
        args.push_back(iter->arg1);

    // Check the call
    if ((args.size() < 2) || (args[0]->type != SPARQLParser::Filter::Literal) || (args[1]->type != SPARQLParser::Filter::Literal))
        throw SemanticAnalysis::SemanticException("malformed table function call");
    unsigned inputArgs = std::atoi(args[1]->value.c_str());
    if ((inputArgs + 2) >= args.size())
        throw SemanticAnalysis::SemanticException("too few arguments to table function");
    for (unsigned index = 0; index < inputArgs; index++)
        if ((args[2 + index]->type != SPARQLParser::Filter::Literal) && (args[2 + index]->type != SPARQLParser::Filter::IRI) && (args[2 + index]->type != SPARQLParser::Filter::Variable))
            throw SemanticAnalysis::SemanticException("table function arguments must be literals or variables");
    for (unsigned index = 2 + inputArgs; index < args.size(); index++)
        if (args[index]->type != SPARQLParser::Filter::Variable)
            throw SemanticAnalysis::SemanticException("table function output must consist of variables");

    // Translate it
    output.tableFunctions.resize(output.tableFunctions.size() + 1);
    QueryGraph::TableFunction& func = output.tableFunctions.back();
    func.name = args[0]->value;
    func.input.resize(inputArgs);
    func.output.resize(args.size() - 2 - inputArgs);
    for (unsigned index = 0; index < inputArgs; index++) {
        if (args[index + 2]->type == SPARQLParser::Filter::Variable) {
            func.input[index].id = args[index + 2]->valueArg;
        } else {
            func.input[index].id = ~0u;
            if (args[index + 2]->type == SPARQLParser::Filter::IRI)
                func.input[index].value = "<" + args[index + 2]->value + ">";
            else
                func.input[index].value = "\"" + args[index + 2]->value + "\"";
        }
    }
    for (unsigned index = 2 + inputArgs; index < args.size(); index++)
        func.output[index - 2 - inputArgs] = args[index]->valueArg;
}
//---------------------------------------------------------------------------
static bool transformSubquery(SemanticAnalysis *myself, DBLayer& dict, DifferentialIndex* diffIndex, const SPARQLParser::PatternGroup& group, QueryGraph::SubQuery& output, QueryDict &tempQueryDict)
    // Transform a subquery
{
    // Encode all patterns
    for (std::vector<SPARQLParser::Pattern>::const_iterator iter = group.patterns.begin(), limit = group.patterns.end(); iter != limit; ++iter) {
        // Encode the entries
        QueryGraph::Node node;
        node.constSubject = node.constPredicate = node.constObject = false;
        if ((!encode(dict, diffIndex, (*iter).subject, node.subject, node.constSubject)) ||
                (!encode(dict, diffIndex, (*iter).predicate, node.predicate, node.constPredicate)) ||
                (!encode(dict, diffIndex, (*iter).object, node.object, node.constObject))) {
            // A constant could not be resolved. This will produce an empty result
            return false;
        }
        output.nodes.push_back(node);
    }

    // Encode all VALUES patterns
    for( auto iter : group.values) {
        QueryGraph::ValuesNode n;
        n.variables = iter.variables;
        for (auto value : iter.values) {
            bool constV;
            uint64_t v;
            if ((!encode(dict, diffIndex, value, v, constV))) {
                return false;
            }
            n.values.push_back(v);
        }
        output.valueNodes.push_back(n);
    }

    //Encode possible assignments as special table functions
    for (std::vector<SPARQLParser::Assignment>::const_iterator iter = group.assignments.begin();
            iter != group.assignments.end(); ++iter) {
        if (!encodeAssignment(myself, dict, diffIndex, group, *iter, output, tempQueryDict)) {
            return false;
        }
    }

    // Encode the filter conditions
    for (std::vector<SPARQLParser::Filter>::const_iterator iter = group.filters.begin(), limit = group.filters.end(); iter != limit; ++iter) {
        if (((*iter).type == SPARQLParser::Filter::Function) && ((*iter).arg1->value == tableFunctionId)) {
            encodeTableFunction(group, *iter, output);
            continue;
        }
        if (!encodeFilter(myself, dict, diffIndex, group, *iter, output, tempQueryDict)) {
            // The filter variable is not bound. This will produce an empty result
            return false;
        }
    }

    // Encode all optional parts
    for (std::vector<SPARQLParser::PatternGroup>::const_iterator iter = group.optional.begin(), limit = group.optional.end(); iter != limit; ++iter) {
        QueryGraph::SubQuery subQuery;
        if (!transformSubquery(myself, dict, diffIndex, *iter, subQuery, tempQueryDict)) {
            // Known to produce an empty result, skip it
            continue;
        }
        output.optional.push_back(subQuery);
    }

    // Encode all union parts
    for (std::vector<std::vector<SPARQLParser::PatternGroup> >::const_iterator iter = group.unions.begin(), limit = group.unions.end(); iter != limit; ++iter) {
        std::vector<QueryGraph::SubQuery> unionParts;
        for (std::vector<SPARQLParser::PatternGroup>::const_iterator iter2 = (*iter).begin(), limit2 = (*iter).end(); iter2 != limit2; ++iter2) {
            QueryGraph::SubQuery subQuery;
            if (!transformSubquery(myself, dict, diffIndex, *iter2, subQuery, tempQueryDict)) {
                // Known to produce an empty result, skip it
                continue;
            }
            unionParts.push_back(subQuery);
        }
        // Empty union?
        if (unionParts.empty())
            return false;
        output.unions.push_back(unionParts);
    }

    // Encode entire subselects
    for (auto itr = group.subqueries.begin();
            itr != group.subqueries.end(); ++itr) {
        //Create a child querygraph
        std::shared_ptr<QueryGraph> subquery(new QueryGraph());
        myself->transform(**itr, *subquery);
        output.subqueries.push_back(subquery);
        if (subquery->knownEmpty()) {
            return false;
        }
    }

    // Encode minuses
    for (auto it : group.minuses) {
        std::shared_ptr<QueryGraph> q(new QueryGraph());
        if (!transformSubquery(myself, dict, diffIndex, it, q->getQuery(), tempQueryDict)) {
            continue;
        }
        output.minuses.push_back(q);
    }

    return true;
}
//---------------------------------------------------------------------------
static bool encodeNotExistsFilter(SemanticAnalysis *myself, QueryGraph::Filter::Type type, DBLayer& dict, DifferentialIndex* diffIndex, const SPARQLParser::PatternGroup& group, const SPARQLParser::Filter& input, QueryGraph::Filter& output, QueryDict &tempDict)
    // Encode a filter for not exists
{
    output.type = type;
    if (myself == NULL) {
        throw SemanticAnalysis::SemanticException("Must implement it");
    }
    if (input.pointerToSubquery != NULL) {
        output.subquery = std::shared_ptr<QueryGraph>(new QueryGraph());
        myself->transform(*input.pointerToSubquery.get(), *output.subquery.get());
        return true;
    } else { //It's a pattern
        output.subpattern = std::shared_ptr<QueryGraph::SubQuery>(new QueryGraph::SubQuery());
        bool res = transformSubquery(myself, dict, NULL, *input.pointerToSubPattern.get(),
                *output.subpattern.get(), tempDict);
        return res;
    }
}
//---------------------------------------------------------------------------
void SemanticAnalysis::transform(const SPARQLParser& input, QueryGraph& output)
    // Perform the transformation
{
    output.clear();

    if (!transformSubquery(this, dict, /*diffIndex,*/NULL, input.getPatterns(), output.getQuery(), tempDict)) {
        // A constant could not be resolved. This will produce an empty result
        output.markAsKnownEmpty();
        return;
    }

    // Compute the edges
    output.constructEdges();

    // Add the projection entry
    for (SPARQLParser::projection_iterator iter = input.projectionBegin(), limit = input.projectionEnd(); iter != limit; ++iter)
        output.addProjection(*iter);

    // Set the duplicate handling
    switch (input.getProjectionModifier()) {
        case SPARQLParser::Modifier_None:
            output.setDuplicateHandling(QueryGraph::AllDuplicates);
            break;
        case SPARQLParser::Modifier_Distinct:
            output.setDuplicateHandling(QueryGraph::NoDuplicates);
            break;
        case SPARQLParser::Modifier_Reduced:
            output.setDuplicateHandling(QueryGraph::ReducedDuplicates);
            break;
        case SPARQLParser::Modifier_Count:
            output.setDuplicateHandling(QueryGraph::CountDuplicates);
            break;
        case SPARQLParser::Modifier_Duplicates:
            output.setDuplicateHandling(QueryGraph::ShowDuplicates);
            break;
    }

    // Order by clause
    for (SPARQLParser::order_iterator iter = input.orderBegin(), limit = input.orderEnd(); iter != limit; ++iter) {
        QueryGraph::Order o;
        if (~(*iter).id) {
            if (!binds(input.getPatterns(), (*iter).id))
                continue;
            o.id = (*iter).id;
        } else {
            o.id = ~0u;
        }
        o.descending = (*iter).descending;
        output.addOrder(o);
    }

    // Set the limit
    output.setLimit(input.getLimit());
}
//---------------------------------------------------------------------------
