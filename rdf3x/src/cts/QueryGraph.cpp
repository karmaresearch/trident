#include "cts/infra/QueryGraph.hpp"
#include <set>
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
using namespace std;
//---------------------------------------------------------------------------
bool QueryGraph::Node::canJoin(const Node& other) const
// Is there an implicit join edge to another node?
{
    // Extract variables
    unsigned v11 = 0, v12 = 0, v13 = 0;
    if (!constSubject) v11 = subject + 1;
    if (!constPredicate) v12 = predicate + 1;
    if (!constObject) v13 = object + 1;
    unsigned v21 = 0, v22 = 0, v23 = 0;
    if (!other.constSubject) v21 = other.subject + 1;
    if (!other.constPredicate) v22 = other.predicate + 1;
    if (!other.constObject) v23 = other.object + 1;

    // Do they have a variable in common?
    bool canJoin = false;
    if (v11 && v21 && (v11 == v21)) canJoin = true;
    if (v11 && v22 && (v11 == v22)) canJoin = true;
    if (v11 && v23 && (v11 == v23)) canJoin = true;
    if (v12 && v21 && (v12 == v21)) canJoin = true;
    if (v12 && v22 && (v12 == v22)) canJoin = true;
    if (v12 && v23 && (v12 == v23)) canJoin = true;
    if (v13 && v21 && (v13 == v21)) canJoin = true;
    if (v13 && v22 && (v13 == v22)) canJoin = true;
    if (v13 && v23 && (v13 == v23)) canJoin = true;

    return canJoin;
}
//---------------------------------------------------------------------------
QueryGraph::Edge::Edge(uint64_t from, uint64_t to, const vector<unsigned>& common)
    : from(from), to(to), common(common)
      // Constructor
{
}
//---------------------------------------------------------------------------
QueryGraph::Edge::~Edge()
    // Destructor
{
}
//---------------------------------------------------------------------------
QueryGraph::Filter::Filter()
    : arg1(0), arg2(0), arg3(0), arg4(0), id(~0u), subquery(NULL), subpattern(NULL)
      // Constructor
{
}
//---------------------------------------------------------------------------
QueryGraph::Filter::Filter(const Filter& other)
    : type(other.type), arg1(other.arg1 ? new Filter(*other.arg1) : 0), arg2(other.arg2 ? new Filter(*other.arg2) : 0), arg3(other.arg3 ? new Filter(*other.arg3) : 0), arg4(other.arg4 ? new Filter(*other.arg4) : 0), id(other.id), value(other.value),
    valueid(other.valueid), subquery(other.subquery), subpattern(other.subpattern)
    // Copy-Constructor
{
}
//---------------------------------------------------------------------------
QueryGraph::Filter::~Filter()
    // Destructor
{
    delete arg1;
    delete arg2;
    delete arg3;
    delete arg4;
}
//---------------------------------------------------------------------------
QueryGraph::Filter& QueryGraph::Filter::operator=(const Filter& other)
    // Assignment
{
    if ((&other) != this) {
        type = other.type;
        delete arg1;
        arg1 = other.arg1 ? new Filter(*other.arg1) : 0;
        delete arg2;
        arg2 = other.arg2 ? new Filter(*other.arg2) : 0;
        delete arg3;
        arg3 = other.arg3 ? new Filter(*other.arg3) : 0;
        delete arg4;
        arg4 = other.arg4 ? new Filter(*other.arg4) : 0;
        id = other.id;
        value = other.value;
        valueid = other.valueid;
    }
    return *this;
}
//---------------------------------------------------------------------------
std::set<std::pair<uint64_t, bool> > QueryGraph::Filter::allIdVarsAndLiterals() const {
    // A variable?
    std::set<std::pair<uint64_t, bool> > set;
    if (type == Variable) {
        set.insert(std::make_pair(id, true));
        return set;
    } else if (type == Literal) {
        set.insert(std::make_pair(valueid, false));
        return set;
    } else if (type == Builtin_aggr) {
        set.insert(std::make_pair(id, true));
    }

    // Check the input
    if (arg1) {
        std::set<std::pair<uint64_t, bool> > set1 = arg1->allIdVarsAndLiterals();
        for (std::set<std::pair<uint64_t, bool> >::iterator itr = set1.begin(); itr != set1.end(); ++itr)
            set.insert(*itr);
    }

    if (arg2) {
        std::set<std::pair<uint64_t, bool> > set2 = arg2->allIdVarsAndLiterals();
        for (std::set<std::pair<uint64_t, bool> >::iterator itr = set2.begin(); itr != set2.end(); ++itr)
            set.insert(*itr);
    }

    if (arg3) {
        std::set<std::pair<uint64_t, bool> > set3 = arg3->allIdVarsAndLiterals();
        for (std::set<std::pair<uint64_t, bool> >::iterator itr = set3.begin(); itr != set3.end(); ++itr)
            set.insert(*itr);
    }

    if (arg4) {
        std::set<std::pair<uint64_t, bool> > set4 = arg4->allIdVarsAndLiterals();
        for (std::set<std::pair<uint64_t, bool> >::iterator itr = set4.begin(); itr != set4.end(); ++itr)
            set.insert(*itr);
    }

    return set;
}
//---------------------------------------------------------------------------
bool QueryGraph::Filter::isApplicable(const std::set<unsigned>& variables) const
// Could be applied?
{
    // A variable?
    if (type == Variable)
        return variables.count(id);

    // Check the input
    if (arg1 && (!arg1->isApplicable(variables))) return false;
    if (arg2 && (!arg2->isApplicable(variables))) return false;
    if (arg3 && (!arg3->isApplicable(variables))) return false;
    if (subquery) {
        //Check whether the subquery mentions all variables
        for(auto idx = subquery->projectionBegin();
                idx != subquery->projectionEnd();
                ++idx) {
            if (!variables.count(*idx)) {
                return false;
            }
        }
    }
    return true;
}
//---------------------------------------------------------------------------
QueryGraph::QueryGraph(unsigned varcount)
    : duplicateHandling(AllDuplicates), limit(~0u), knownEmptyResult(false),
    hdl(varcount)
      // Constructor
{
}
//---------------------------------------------------------------------------
QueryGraph::~QueryGraph()
    // Destructor
{
}
//---------------------------------------------------------------------------
void QueryGraph::clear()
    // Clear the graph
{
    query = SubQuery();
    duplicateHandling = AllDuplicates;
    knownEmptyResult = false;
}
//---------------------------------------------------------------------------
static bool intersects(const set<unsigned>& a, const set<unsigned>& b, vector<unsigned>& common)
    // Check if two sets overlap
{
    common.clear();
    set<unsigned>::const_iterator ia, la, ib, lb;
    if (a.size() < b.size()) {
        if (a.empty())
            return false;
        ia = a.begin();
        la = a.end();
        ib = b.lower_bound(*ia);
        lb = b.end();
    } else {
        if (b.empty())
            return false;
        ib = b.begin();
        lb = b.end();
        ia = a.lower_bound(*ib);
        la = a.end();
    }
    bool result = false;
    while ((ia != la) && (ib != lb)) {
        unsigned va = *ia, vb = *ib;
        if (va < vb) {
            ++ia;
        } else if (va > vb) {
            ++ib;
        } else {
            result = true;
            common.push_back(*ia);
            ++ia;
            ++ib;
        }
    }
    return result;
}
//---------------------------------------------------------------------------
static void constructEdges(QueryGraph::SubQuery& subQuery, set<unsigned>& bindings)
    // Construct the edges for a specfic subquery
{
    // Collect all variable bindings
    vector<set<unsigned> > patternBindings, optionalBindings, unionBindings, subqueryBindings, valueBindings;
    patternBindings.resize(subQuery.nodes.size());
    for (unsigned index = 0, limit = patternBindings.size(); index < limit; ++index) {
        const QueryGraph::Node& n = subQuery.nodes[index];
        if (!n.constSubject) {
            patternBindings[index].insert(n.subject);
            bindings.insert(n.subject);
        }
        if (!n.constPredicate) {
            patternBindings[index].insert(n.predicate);
            bindings.insert(n.predicate);
        }
        if (!n.constObject) {
            patternBindings[index].insert(n.object);
            bindings.insert(n.object);
        }
    }
    optionalBindings.resize(subQuery.optional.size());
    for (unsigned index = 0, limit = optionalBindings.size(); index < limit; ++index) {
        constructEdges(subQuery.optional[index], optionalBindings[index]);
        bindings.insert(optionalBindings[index].begin(), optionalBindings[index].end());
    }
    unionBindings.resize(subQuery.unions.size());
    for (unsigned index = 0, limit = unionBindings.size(); index < limit; ++index) {
        for (vector<QueryGraph::SubQuery>::iterator iter = subQuery.unions[index].begin(), limit = subQuery.unions[index].end(); iter != limit; ++iter)
            constructEdges(*iter, unionBindings[index]);
        bindings.insert(unionBindings[index].begin(), unionBindings[index].end());
    }
    // Add the projection of the subqueries.
    subqueryBindings.resize(subQuery.subqueries.size());
    for (unsigned index = 0, limit = subqueryBindings.size(); index < limit; ++index) {
        subqueryBindings[index].insert(subQuery.subqueries[index]->projectionBegin(), subQuery.subqueries[index]->projectionEnd());
        bindings.insert(subqueryBindings[index].begin(), subqueryBindings[index].end());
    }
    valueBindings.resize(subQuery.valueNodes.size());
    for (unsigned index = 0, limit = valueBindings.size(); index < limit; ++index) {
        for (auto v : subQuery.valueNodes[index].variables) {
            valueBindings[index].insert(v);
            bindings.insert(v);
        }
    }

    // Derive all edges
    subQuery.edges.clear();
    unsigned optionalOfs = patternBindings.size(), unionOfs = optionalOfs + optionalBindings.size();
    unsigned subqueryOfs = unionOfs + unionBindings.size();
    unsigned valueOfs = subqueryOfs + subqueryBindings.size();
    vector<unsigned> common;
    for (unsigned index = 0, limit = patternBindings.size(); index < limit; ++index) {
        for (unsigned index2 = index + 1; index2 < limit; index2++)
            if (intersects(patternBindings[index], patternBindings[index2], common))
                subQuery.edges.push_back(QueryGraph::Edge(index, index2, common));
        for (unsigned index2 = 0, limit2 = optionalBindings.size(); index2 < limit2; index2++)
            if (intersects(patternBindings[index], optionalBindings[index2], common))
                subQuery.edges.push_back(QueryGraph::Edge(index, optionalOfs + index2, common));
        for (unsigned index2 = 0, limit2 = unionBindings.size(); index2 < limit2; index2++)
            if (intersects(patternBindings[index], unionBindings[index2], common))
                subQuery.edges.push_back(QueryGraph::Edge(index, unionOfs + index2, common));
        for (unsigned index2 = 0, limit2 = subqueryBindings.size(); index2 < limit2; index2++)
            if (intersects(patternBindings[index], subqueryBindings[index2], common))
                subQuery.edges.push_back(QueryGraph::Edge(index, subqueryOfs + index2, common));
        for (unsigned index2 = 0, limit2 = valueBindings.size(); index2 < limit2; index2++)
            if (intersects(patternBindings[index], valueBindings[index2], common))
                subQuery.edges.push_back(QueryGraph::Edge(index, valueOfs + index2, common));
    }
    for (unsigned index = 0, limit = optionalBindings.size(); index < limit; ++index) {
        for (unsigned index2 = index + 1; index2 < limit; index2++)
            if (intersects(optionalBindings[index], optionalBindings[index2], common))
                subQuery.edges.push_back(QueryGraph::Edge(optionalOfs + index, optionalOfs + index2, common));
        for (unsigned index2 = 0, limit2 = unionBindings.size(); index2 < limit2; index2++)
            if (intersects(optionalBindings[index], unionBindings[index2], common))
                subQuery.edges.push_back(QueryGraph::Edge(optionalOfs + index, unionOfs + index2, common));
        for (unsigned index2 = 0, limit2 = subqueryBindings.size(); index2 < limit2; index2++)
            if (intersects(optionalBindings[index], subqueryBindings[index2], common))
                subQuery.edges.push_back(QueryGraph::Edge(optionalOfs + index, subqueryOfs + index2, common));
        for (unsigned index2 = 0, limit2 = valueBindings.size(); index2 < limit2; index2++)
            if (intersects(optionalBindings[index], valueBindings[index2], common))
                subQuery.edges.push_back(QueryGraph::Edge(optionalOfs + index, valueOfs + index2, common));
    }
    for (unsigned index = 0, limit = unionBindings.size(); index < limit; ++index) {
        for (unsigned index2 = index + 1; index2 < limit; index2++)
            if (intersects(unionBindings[index], unionBindings[index2], common))
                subQuery.edges.push_back(QueryGraph::Edge(unionOfs + index, unionOfs + index2, common));
        for (unsigned index2 = 0, limit2 = subqueryBindings.size(); index2 < limit2; index2++)
            if (intersects(unionBindings[index], subqueryBindings[index2], common))
                subQuery.edges.push_back(QueryGraph::Edge(unionOfs + index, subqueryOfs + index2, common));
        for (unsigned index2 = 0, limit2 = valueBindings.size(); index2 < limit2; index2++)
            if (intersects(unionBindings[index], valueBindings[index2], common))
                subQuery.edges.push_back(QueryGraph::Edge(unionOfs + index, valueOfs + index2, common));
    }
    for (unsigned index = 0, limit = subqueryBindings.size(); index < limit; index++) {
        for (unsigned index2 = index + 1, limit2 = subqueryBindings.size(); index2 < limit2; index2++)
            if (intersects(subqueryBindings[index], subqueryBindings[index2], common))
                subQuery.edges.push_back(QueryGraph::Edge(subqueryOfs + index, subqueryOfs + index2, common));
        for (unsigned index2 = 0, limit2 = valueBindings.size(); index2 < limit2; index2++)
            if (intersects(subqueryBindings[index], valueBindings[index2], common))
                subQuery.edges.push_back(QueryGraph::Edge(subqueryOfs + index, valueOfs + index2, common));
    }
}
//---------------------------------------------------------------------------
void QueryGraph::constructEdges()
    // Construct the edges
{
    set<unsigned> bindings;
    ::constructEdges(query, bindings);
}
//---------------------------------------------------------------------------
