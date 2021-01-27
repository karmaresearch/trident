#include <cts/plangen/PlanGen.hpp>
#include <cts/plangen/Costs.hpp>

/*#include "cts/codegen/CodeGen.hpp"
#include "rts/segment/AggregatedFactsSegment.hpp"
#include "rts/segment/FullyAggregatedFactsSegment.hpp"
#include "rts/segment/FactsSegment.hpp"
#include "rts/segment/ExactStatisticsSegment.hpp"*/

#include <map>
#include <set>
#include <algorithm>
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
// XXX integrate DPhyper-based optimization, query path statistics
//---------------------------------------------------------------------------
/// Description for a join
struct PlanGen::JoinDescription {
    /// Sides of the join
    BitSet left, right;
    /// Required ordering
    unsigned ordering;
    /// Selectivity
    double selectivity;
    /// Table function
    const QueryGraph::TableFunction* tableFunction;
};
//---------------------------------------------------------------------------
PlanGen::PlanGen() : plans(new PlanContainer())
                     // Constructor
{
}
//---------------------------------------------------------------------------
PlanGen::PlanGen(std::shared_ptr<PlanContainer> plans) : plans(plans)
                                                         // Constructor
{
}
//---------------------------------------------------------------------------
PlanGen::~PlanGen()
    // Destructor
{
}
//---------------------------------------------------------------------------
void PlanGen::addPlan(Problem* problem, Plan* plan)
    // Add a plan to a subproblem
{
    // Check for dominance
    if (~plan->ordering) {
        Plan* last = 0;
        for (Plan* iter = problem->plans, *next; iter; iter = next) {
            next = iter->next;
            if (iter->ordering == plan->ordering) {
                // Dominated by existing plan?
                if (iter->costs <= plan->costs) {
                    plans->free(plan);
                    return;
                }
                // No, remove the existing plan
                if (last)
                    last->next = iter->next;
                else
                    problem->plans = iter->next;
                plans->free(iter);
            } else if ((!~iter->ordering) && (iter->costs >= plan->costs)) {
                // Dominated by new plan
                if (last)
                    last->next = iter->next;
                else
                    problem->plans = iter->next;
                plans->free(iter);
            } else last = iter;
        }
    } else {
        Plan* last = 0;
        for (Plan* iter = problem->plans, *next; iter; iter = next) {
            next = iter->next;
            // Dominated by existing plan?
            if (iter->costs <= plan->costs) {
                plans->free(plan);
                return;
            }
            // Dominates existing plan?
            if (iter->ordering == plan->ordering) {
                if (last)
                    last->next = iter->next;
                else
                    problem->plans = iter->next;
                plans->free(iter);
            } else last = iter;
        }
    }

    // Add the plan to the problem set
    plan->next = problem->plans;
    problem->plans = plan;
}
Plan *PlanGen::buildFilterPlan(const QueryGraph::Filter *filter) {
    Plan* plan;
    if (filter->subquery) {
        //Temporary set the subquery as full query
        const QueryGraph *prevQ = fullQuery;
        fullQuery = filter->subquery.get();
        plan = translate_int(filter->subquery->getQuery(), *filter->subquery.get(), false);
        fullQuery = prevQ;
    } else if (filter->subpattern) { //pattern
        QueryGraph q(0);
        plan = translate_int(*filter->subpattern.get(), q, false);
    } else {
        throw; //should never happen
    }
    return plan;
}
//---------------------------------------------------------------------------
Plan *PlanGen::attachFiltersToPlan(QueryGraph::Filter *filter, Plan *plan) {
    //Decompose the filter in many component
    Plan* p2 = plans->alloc();
    double cost1 = plan->costs + Costs::filter(plan->cardinality);
    p2->op = Plan::Filter;
    p2->costs = cost1;
    p2->opArg = filter->id;
    p2->left = plan;

    Plan *filterPlan = NULL;
    //If not exist, then there is a subgraph to build
    if (filter->type == QueryGraph::Filter::Builtin_notexists) {
        filterPlan = buildFilterPlan(filter);
    }
    FilterArgs *fa = plans->allocFilterArgs();
    fa->filter = filter;
    fa->plan = filterPlan;
    p2->right = reinterpret_cast<Plan*>(fa);

    p2->next = 0;
    p2->cardinality = plan->cardinality * 0.5;
    p2->ordering = plan->ordering;
    return p2;
}
//---------------------------------------------------------------------------
Plan* PlanGen::buildFilters(const QueryGraph::SubQuery& query, Plan* plan, uint64_t value1, uint64_t value2, uint64_t value3)
    // Apply filters to index scans
{
    // Collect variables
    set<unsigned> orderingOnly, allAttributes;
    if (~(plan->ordering))
        orderingOnly.insert(plan->ordering);
    allAttributes.insert(value1);
    allAttributes.insert(value2);
    allAttributes.insert(value3);

    // Apply a filter on the ordering first
    for (vector<QueryGraph::Filter>::const_iterator iter = query.filters.begin(), limit = query.filters.end(); iter != limit; ++iter)
        if ((*iter).isApplicable(orderingOnly)) {
            Plan* p2 = plans->alloc();
            double cost1 = plan->costs + Costs::filter(plan->cardinality);
            p2->op = Plan::Filter;
            p2->costs = cost1;
            p2->opArg = (*iter).id;
            p2->left = plan;

            Plan *filterPlan = NULL;
            //If not exist, then there is a subgraph to build
            if (iter->type == QueryGraph::Filter::Builtin_notexists) {
                filterPlan = buildFilterPlan(&*iter);
            }
            FilterArgs *fa = plans->allocFilterArgs();
            fa->filter = &*iter;
            fa->plan = filterPlan;
            p2->right = reinterpret_cast<Plan*>(fa);

            p2->next = 0;
            p2->cardinality = plan->cardinality * 0.5;
            p2->ordering = plan->ordering;
            plan = p2;
        }
    // Apply all other applicable filters
    for (vector<QueryGraph::Filter>::const_iterator iter = query.filters.begin(), limit = query.filters.end(); iter != limit; ++iter)
        if ((*iter).isApplicable(allAttributes) && (!(*iter).isApplicable(orderingOnly))) {
            Plan* p2 = plans->alloc();
            p2->op = Plan::Filter;
            p2->opArg = (*iter).id;
            p2->left = plan;

            Plan *filterPlan = NULL;
            //If not exist, then there is a subgraph to build
            if (iter->type == QueryGraph::Filter::Builtin_notexists) {
                filterPlan = buildFilterPlan(&*iter);
            }
            FilterArgs *fa = plans->allocFilterArgs();
            fa->filter = &*iter;
            fa->plan = filterPlan;
            p2->right = reinterpret_cast<Plan*>(fa);

            p2->next = 0;
            p2->cardinality = plan->cardinality * 0.5;
            p2->costs = plan->costs + Costs::filter(plan->cardinality);
            p2->ordering = plan->ordering;
            plan = p2;
        }
    return plan;
}
//---------------------------------------------------------------------------
static void normalizePattern(DBLayer::DataOrder order, uint64_t& c1, uint64_t& c2, uint64_t& c3)
    // Extract subject/predicate/object order
{
    uint64_t s = UINT64_MAX, p = UINT64_MAX, o = UINT64_MAX;
    switch (order) {
        case DBLayer::Order_No_Order_SPO:
        case DBLayer::Order_Subject_Predicate_Object:
            s = c1;
            p = c2;
            o = c3;
            break;
        case DBLayer::Order_No_Order_SOP:
        case DBLayer::Order_Subject_Object_Predicate:
            s = c1;
            o = c2;
            p = c3;
            break;
        case DBLayer::Order_No_Order_OPS:
        case DBLayer::Order_Object_Predicate_Subject:
            o = c1;
            p = c2;
            s = c3;
            break;
        case DBLayer::Order_No_Order_OSP:
        case DBLayer::Order_Object_Subject_Predicate:
            o = c1;
            s = c2;
            p = c3;
            break;
        case DBLayer::Order_No_Order_PSO:
        case DBLayer::Order_Predicate_Subject_Object:
            p = c1;
            s = c2;
            o = c3;
            break;
        case DBLayer::Order_No_Order_POS:
        case DBLayer::Order_Predicate_Object_Subject:
            p = c1;
            o = c2;
            s = c3;
            break;
    }
    c1 = s;
    c2 = p;
    c3 = o;
}
//---------------------------------------------------------------------------
static uint64_t getCardinality(DBLayer& db, DBLayer::DataOrder order, uint64_t c1, uint64_t c2, uint64_t c3)
    // Estimate the cardinality of a predicate
{
    normalizePattern(order, c1, c2, c3);
    return db.getCardinality(c1, c2, c3);
}
//---------------------------------------------------------------------------
void PlanGen::buildIndexScan(const QueryGraph::SubQuery& query, DBLayer::DataOrder order, Problem* result, uint64_t value1, uint64_t value1C, uint64_t value2, uint64_t value2C, uint64_t value3, uint64_t value3C)
    // Build an index scan
{
    // Initialize a new plan
    Plan* plan = plans->alloc();
    plan->op = Plan::IndexScan;
    plan->opArg = order;
    plan->left = 0;
    plan->right = 0;
    plan->next = 0;

    // Compute the statistics
    uint64_t scanned;
    plan->cardinality = scanned = getCardinality(*db, order, value1C, value2C, value3C);
    if (!~value1) {
        if (!~value2) {
            plan->ordering = value3;
        } else {
            scanned = getCardinality(*db, order, value1C, value2C, UINT64_MAX);
            plan->ordering = value2;
        }
    } else {
        scanned = getCardinality(*db, order, value1C, UINT64_MAX, UINT64_MAX);
        plan->ordering = value1;
    }
    /*unsigned pages = 1 + static_cast<unsigned>(db->getFacts(order).getPages() * (static_cast<double>(scanned) / static_cast<double>(db->getFacts(order).getCardinality())));
      plan->costs = Costs::seekBtree() + Costs::scan(pages);*/

    if (order == DBLayer::DataOrder::Order_No_Order_SPO ||
            order == DBLayer::DataOrder::Order_No_Order_SOP ||
            order == DBLayer::DataOrder::Order_No_Order_OPS ||
            order == DBLayer::DataOrder::Order_No_Order_OSP ||
            order == DBLayer::DataOrder::Order_No_Order_POS ||
            order == DBLayer::DataOrder::Order_No_Order_PSO)
        plan->ordering = ~0u;

    plan->costs = db->getScanCost(order, value1, value1C,
            value2, value2C,
            value3, value3C);

    // Apply filters
    plan = buildFilters(query, plan, value1, value2, value3);

    // And store it
    addPlan(result, plan);
}
//---------------------------------------------------------------------------
void PlanGen::buildAggregatedIndexScan(const QueryGraph::SubQuery& query, DBLayer::DataOrder order, Problem* result, uint64_t value1, uint64_t value1C, uint64_t value2, uint64_t value2C)
    // Build an aggregated index scan
{
    // Refuse placing constants at the end
    // They should be pruned out anyway, but sometimes are not due to misestimations
    if ((!~value2) && (~value1))
        return;

    // Initialize a new plan
    Plan* plan = plans->alloc();
    plan->op = Plan::AggregatedIndexScan;
    plan->opArg = order;
    plan->left = 0;
    plan->right = 0;
    plan->next = 0;

    // Compute the statistics
    uint64_t scanned = getCardinality(*db, order, value1C, value2C, UINT64_MAX);
    uint64_t fullSize = db->getCardinality();
    if (scanned > fullSize)
        scanned = fullSize;
    if (!scanned) scanned = 1;
    plan->cardinality = scanned;
    if (!~value1) {
        plan->ordering = value2;
    } else {
        plan->ordering = value1;
    }

    /*unsigned pages = 1 + static_cast<unsigned>(db->getAggregatedFacts(order).getPages() * (static_cast<double>(scanned) / static_cast<double>(fullSize)));
      plan->costs = Costs::seekBtree() + Costs::scan(pages);*/
    plan->costs = db->getScanCost(order, value1, value1C, value2, value2C);

    if (order == DBLayer::DataOrder::Order_No_Order_SPO ||
            order == DBLayer::DataOrder::Order_No_Order_SOP ||
            order == DBLayer::DataOrder::Order_No_Order_OPS ||
            order == DBLayer::DataOrder::Order_No_Order_OSP ||
            order == DBLayer::DataOrder::Order_No_Order_POS ||
            order == DBLayer::DataOrder::Order_No_Order_PSO)
        plan->ordering = ~0u;


    // Apply filters
    plan = buildFilters(query, plan, value1, value2, UINT64_MAX);

    // And store it
    addPlan(result, plan);
}
//---------------------------------------------------------------------------
void PlanGen::buildFullyAggregatedIndexScan(const QueryGraph::SubQuery& query, DBLayer::DataOrder order, Problem* result, uint64_t value1, uint64_t value1C)
    // Build an fully aggregated index scan
{
    // Initialize a new plan
    Plan* plan = plans->alloc();
    plan->op = Plan::FullyAggregatedIndexScan;
    plan->opArg = order;
    plan->left = 0;
    plan->right = 0;
    plan->next = 0;

    // Compute the statistics
    uint64_t scanned = getCardinality(*db, order, value1C, UINT64_MAX, UINT64_MAX);
    uint64_t fullSize = db->getCardinality();
    if (scanned > fullSize)
        scanned = fullSize;
    if (!scanned) scanned = 1;
    plan->cardinality = scanned;
    if (!~value1) {
        plan->ordering = ~0u;
    } else {
        plan->ordering = value1;
    }
    /*unsigned pages = 1 + static_cast<unsigned>(db->getFullyAggregatedFacts(order).getPages() * (static_cast<double>(scanned) / static_cast<double>(fullSize)));
      plan->costs = Costs::seekBtree() + Costs::scan(pages);*/
    plan->costs = db->getScanCost(order, value1, value1C);

    if (order == DBLayer::DataOrder::Order_No_Order_SPO ||
            order == DBLayer::DataOrder::Order_No_Order_SOP ||
            order == DBLayer::DataOrder::Order_No_Order_OPS ||
            order == DBLayer::DataOrder::Order_No_Order_OSP ||
            order == DBLayer::DataOrder::Order_No_Order_POS ||
            order == DBLayer::DataOrder::Order_No_Order_PSO)
        plan->ordering = ~0u;

    // Apply filters
    plan = buildFilters(query, plan, value1, UINT64_MAX, UINT64_MAX);

    // And store it
    addPlan(result, plan);
}
//---------------------------------------------------------------------------
static bool isUnused(const QueryGraph::Filter* filter, unsigned val)
    // Check if a variable is unused
{
    if (!filter)
        return true;
    if (filter->type == QueryGraph::Filter::Variable)
        if (filter->id == val)
            return false;
    return isUnused(filter->arg1, val) && isUnused(filter->arg2, val) && isUnused(filter->arg3, val);
}
//---------------------------------------------------------------------------
static bool isUnused(const QueryGraph::TableFunction* function, uint64_t val) {
    //Check all variables in input
    for (std::vector<QueryGraph::TableFunction::Argument>::const_iterator itr = function->input.begin(); itr != function->input.end();
            ++itr) {
        if (itr->id == val)
            return false;
    }
    return true;
}

//---------------------------------------------------------------------------
static bool isUnused(const QueryGraph::SubQuery& query, const QueryGraph::Node& node, uint64_t val)
    // Check if a variable is unused outside its primary pattern
{
    for (vector<QueryGraph::Filter>::const_iterator iter = query.filters.begin(), limit = query.filters.end(); iter != limit; ++iter)
        if (!isUnused(&(*iter), val))
            return false;

    for (vector<QueryGraph::TableFunction>::const_iterator iter = query.tableFunctions.begin(),
            limit = query.tableFunctions.end(); iter != limit; ++iter)
        if (!isUnused(&(*iter), val))
            return false;

    for (vector<QueryGraph::Node>::const_iterator iter = query.nodes.begin(), limit = query.nodes.end(); iter != limit; ++iter) {
        const QueryGraph::Node& n = *iter;
        if ((&n) == (&node))
            continue;
        if ((!n.constSubject) && (val == n.subject)) return false;
        if ((!n.constPredicate) && (val == n.predicate)) return false;
        if ((!n.constObject) && (val == n.object)) return false;
    }
    for(auto iter : query.valueNodes) {
        for(auto v : iter.variables) {
            if (v == val)
                return false;
        }
    }
    for (vector<QueryGraph::SubQuery>::const_iterator iter = query.optional.begin(), limit = query.optional.end(); iter != limit; ++iter)
        if (!isUnused(*iter, node, val))
            return false;
    for (vector<vector<QueryGraph::SubQuery> >::const_iterator iter = query.unions.begin(), limit = query.unions.end(); iter != limit; ++iter)
        for (vector<QueryGraph::SubQuery>::const_iterator iter2 = (*iter).begin(), limit2 = (*iter).end(); iter2 != limit2; ++iter2)
            if (!isUnused(*iter2, node, val))
                return false;
    for (std::vector<std::shared_ptr<QueryGraph>>::const_iterator itr = query.subqueries.begin(); itr != query.subqueries.end(); ++itr)
        if (! isUnused((*itr)->getQuery(), node, val))
            return false;
    return true;
}
//---------------------------------------------------------------------------
static bool isUnused(const QueryGraph& query, const QueryGraph::Node& node, unsigned val)
    // Check if a variable is unused outside its primary pattern
{
    for (QueryGraph::projection_iterator iter = query.projectionBegin(), limit = query.projectionEnd(); iter != limit; ++iter)
        if ((*iter) == val)
            return false;

    for (QueryGraph::order_iterator iter = query.orderBegin(); iter != query.orderEnd();
            ++iter) {
        if (iter->id == val) {
            return false;
        }
    }

    //Check if the variable is used in some assignments
    for (const auto &tableFunction : query.c_getGlobalAssignments()) {
        for (const auto &arg : tableFunction.input) {
            if (arg.id == val) {
                return false;
            } else {
                //Arg.id could be a variable associated to an aggregated function.
                //In this case, I should check the arguments of the aggregate
                //function
                const auto &ahdl = query.c_getAggredateHandler();
                const auto &assignments = ahdl.getInputOutputVars();
                for (const auto &outputVar : assignments.first) {
                    if (outputVar == val) {
                        return false;
                    }
                }
            }
        }
    }


    return isUnused(query.getQuery(), node, val);
}
//---------------------------------------------------------------------------
PlanGen::Problem* PlanGen::buildValue(const QueryGraph::SubQuery& query,
        const QueryGraph::ValuesNode& node, uint64_t id)
{
    //Create a new problem
    Problem* result = problems.alloc();
    result->next = 0;
    result->relations = BitSet();
    result->relations.set(id);

    //Create a new plan
    Plan* plan = plans->alloc();
    plan->op = Plan::ValuesScan;
    plan->opArg = 0;
    plan->left = reinterpret_cast<Plan*>(const_cast<QueryGraph::ValuesNode*>(&node));
    plan->right = 0;
    plan->next = 0;
    plan->ordering = ~0u;

    if (node.variables.size() > 0) {
        plan->cardinality = node.values.size() / node.variables.size();
        plan->costs = plan->cardinality;
    } else {
        plan->cardinality = plan->costs = 0;
    }

    result->plans = plan;
    return result;
}
//---------------------------------------------------------------------------
PlanGen::Problem* PlanGen::buildScan(const QueryGraph::SubQuery& query, const QueryGraph::Node& node, uint64_t id)
    // Generate base table accesses
{
    // Create new problem instance
    Problem* result = problems.alloc();
    result->next = 0;
    result->plans = 0;
    result->relations = BitSet();
    result->relations.set(id);

    // Check which parts of the pattern are unused
    bool unusedSubject = (!node.constSubject) && isUnused(*fullQuery, node, node.subject);
    bool unusedPredicate = (!node.constPredicate) && isUnused(*fullQuery, node, node.predicate);
    bool unusedObject = (!node.constObject) && isUnused(*fullQuery, node, node.object);

    // Lookup variables
    uint64_t s = node.constSubject ? UINT64_MAX : node.subject, p = node.constPredicate ? UINT64_MAX : node.predicate, o = node.constObject ? UINT64_MAX : node.object;
    uint64_t sc = node.constSubject ? node.subject : UINT64_MAX, pc = node.constPredicate ? node.predicate : UINT64_MAX, oc = node.constObject ? node.object : UINT64_MAX;

    // Build all relevant scans
    if ((unusedSubject + unusedPredicate + unusedObject) >= 2) {
        if (!unusedSubject) {
            buildFullyAggregatedIndexScan(query, DBLayer::Order_Subject_Predicate_Object, result, s, sc);
            buildFullyAggregatedIndexScan(query, DBLayer::Order_No_Order_SPO, result, s, sc);
        } else if (!unusedObject) {
            buildFullyAggregatedIndexScan(query, DBLayer::Order_Object_Subject_Predicate, result, o, oc);
            buildFullyAggregatedIndexScan(query, DBLayer::Order_No_Order_OSP, result, o, oc);
        } else {
            buildFullyAggregatedIndexScan(query, DBLayer::Order_Predicate_Subject_Object, result, p, pc);
            buildFullyAggregatedIndexScan(query, DBLayer::Order_No_Order_PSO, result, p, pc);
        }
    } else {
        if (unusedObject) {
            buildAggregatedIndexScan(query, DBLayer::Order_Subject_Predicate_Object, result, s, sc, p, pc);
            buildAggregatedIndexScan(query, DBLayer::Order_No_Order_SPO, result, s, sc, p, pc);
        } else {
            buildIndexScan(query, DBLayer::Order_Subject_Predicate_Object, result, s, sc, p, pc, o, oc);
            buildIndexScan(query, DBLayer::Order_No_Order_SPO, result, s, sc, p, pc, o, oc);
        }
        if (unusedPredicate) {
            buildAggregatedIndexScan(query, DBLayer::Order_Subject_Object_Predicate, result, s, sc, o, oc);
            buildAggregatedIndexScan(query, DBLayer::Order_No_Order_SOP, result, s, sc, o, oc);
        } else {
            buildIndexScan(query, DBLayer::Order_Subject_Object_Predicate, result, s, sc, o, oc, p, pc);
            buildIndexScan(query, DBLayer::Order_No_Order_SOP, result, s, sc, o, oc, p, pc);
        }
        if (unusedSubject) {
            buildAggregatedIndexScan(query, DBLayer::Order_Object_Predicate_Subject, result, o, oc, p, pc);
            buildAggregatedIndexScan(query, DBLayer::Order_No_Order_OPS, result, o, oc, p, pc);
        } else {
            buildIndexScan(query, DBLayer::Order_Object_Predicate_Subject, result, o, oc, p, pc, s, sc);
            buildIndexScan(query, DBLayer::Order_No_Order_OPS, result, o, oc, p, pc, s, sc);
        }
        if (unusedPredicate) {
            buildAggregatedIndexScan(query, DBLayer::Order_Object_Subject_Predicate, result, o, oc, s, sc);
            buildAggregatedIndexScan(query, DBLayer::Order_No_Order_OSP, result, o, oc, s, sc);
        } else {
            buildIndexScan(query, DBLayer::Order_Object_Subject_Predicate, result, o, oc, s, sc, p, pc);
            buildIndexScan(query, DBLayer::Order_No_Order_OSP, result, o, oc, s, sc, p, pc);
        }
        if (unusedObject) {
            buildAggregatedIndexScan(query, DBLayer::Order_Predicate_Subject_Object, result, p, pc, s, sc);
            buildAggregatedIndexScan(query, DBLayer::Order_No_Order_PSO, result, p, pc, s, sc);
        } else {
            buildIndexScan(query, DBLayer::Order_Predicate_Subject_Object, result, p, pc, s, sc, o, oc);
            buildIndexScan(query, DBLayer::Order_No_Order_PSO, result, p, pc, s, sc, o, oc);
        }
        if (unusedSubject) {
            buildAggregatedIndexScan(query, DBLayer::Order_Predicate_Object_Subject, result, p, pc, o, oc);
            buildAggregatedIndexScan(query, DBLayer::Order_No_Order_POS, result, p, pc, o, oc);
        } else {
            buildIndexScan(query, DBLayer::Order_Predicate_Object_Subject, result, p, pc, o, oc, s, sc);
            buildIndexScan(query, DBLayer::Order_No_Order_POS, result, p, pc, o, oc, s, sc);
        }
    }

    // Update the child pointers as info for the code generation
    for (Plan* iter = result->plans; iter; iter = iter->next) {
        Plan* iter2 = iter;
        while (iter2->op == Plan::Filter)
            iter2 = iter2->left;
        iter2->left = static_cast<Plan*>(0) + id;
        iter2->right = reinterpret_cast<Plan*>(const_cast<QueryGraph::Node*>(&node));
    }

    return result;
}
//---------------------------------------------------------------------------
PlanGen::JoinDescription PlanGen::buildJoinInfo(const QueryGraph::SubQuery& query, const QueryGraph::Edge& edge)
    // Build the informaion about a join
{
    // Fill in the relations involved
    JoinDescription result;
    result.left.set(edge.from);
    result.right.set(edge.to);

    // Compute the join selectivity
    if (edge.from < query.nodes.size() && edge.to < query.nodes.size()) {
        const QueryGraph::Node& l = query.nodes[edge.from], &r = query.nodes[edge.to];
        result.selectivity = db->getJoinSelectivity(l.constSubject, l.subject, l.constPredicate, l.predicate, l.constObject, l.object, r.constSubject, r.subject, r.constPredicate, r.predicate, r.constObject, r.object);
    } else {
        //I cannot calculate the join selectivity because there is a nested query involved
        //One solution is to get the cardinality of the nested query.
        //Another is to do some rough approximations
        //I choose the second one.
        //
        result.selectivity = -1;
    }

    // Look up suitable orderings
    if (!edge.common.empty()) {
        result.ordering = edge.common.front(); // XXX multiple orderings possible
    } else {
        // Cross product
        result.ordering = (~0u) - 1;
        result.selectivity = -1;
    }
    result.tableFunction = 0;

    return result;
}
//---------------------------------------------------------------------------
PlanGen::Problem* PlanGen::buildOptional(const QueryGraph::SubQuery& query,
        const QueryGraph &entirePlan,
        uint64_t id, bool completeEstimate)
    // Generate an optional part
{
    // Solve the subproblem
    Plan* p = translate_int(query, entirePlan, completeEstimate);
    Plan *tmp = p;
    while (tmp) {
        tmp->optional = true;
        tmp = tmp->next;
    }

    // Create new problem instance
    Problem* result = problems.alloc();
    result->next = 0;
    result->plans = p;
    result->relations = BitSet();
    result->relations.set(id);

    return result;
}
//---------------------------------------------------------------------------
static void collectVariables(const QueryGraph::Filter* filter, set<unsigned>& vars, const void* except)
    // Collect all variables used in a filter
{
    if ((!filter) || (filter == except))
        return;
    if (filter->type == QueryGraph::Filter::Variable)
        vars.insert(filter->id);
    collectVariables(filter->arg1, vars, except);
    collectVariables(filter->arg2, vars, except);
    collectVariables(filter->arg3, vars, except);
}
//---------------------------------------------------------------------------
static void collectVariables(const QueryGraph::SubQuery& query, set<unsigned>& vars, const void* except)
    // Collect all variables used in a subquery
{
    for (vector<QueryGraph::Filter>::const_iterator iter = query.filters.begin(), limit = query.filters.end(); iter != limit; ++iter)
        collectVariables(&(*iter), vars, except);
    for (vector<QueryGraph::Node>::const_iterator iter = query.nodes.begin(), limit = query.nodes.end(); iter != limit; ++iter) {
        const QueryGraph::Node& n = *iter;
        if (except == (&n))
            continue;
        if (!n.constSubject) vars.insert(n.subject);
        if (!n.constPredicate) vars.insert(n.predicate);
        if (!n.constObject) vars.insert(n.object);
    }
    for (vector<QueryGraph::SubQuery>::const_iterator iter = query.optional.begin(), limit = query.optional.end(); iter != limit; ++iter)
        if (except != (&(*iter)))
            collectVariables(*iter, vars, except);
    for (vector<vector<QueryGraph::SubQuery> >::const_iterator iter = query.unions.begin(), limit = query.unions.end(); iter != limit; ++iter)
        if (except != (&(*iter)))
            for (vector<QueryGraph::SubQuery>::const_iterator iter2 = (*iter).begin(), limit2 = (*iter).end(); iter2 != limit2; ++iter2)
                if (except != (&(*iter2)))
                    collectVariables(*iter2, vars, except);
}
//---------------------------------------------------------------------------
static void collectVariables(const QueryGraph& query, set<unsigned>& vars, const void* except)
    // Collect all variables used in a query
{
    for (QueryGraph::projection_iterator iter = query.projectionBegin(), limit = query.projectionEnd(); iter != limit; ++iter)
        vars.insert(*iter);
    if (except != (&(query.getQuery())))
        collectVariables(query.getQuery(), vars, except);
}
//---------------------------------------------------------------------------
static Plan* findOrdering(Plan* root, unsigned ordering)
    // Find a plan with a specific ordering
{
    for (; root; root = root->next)
        if (root->ordering == ordering)
            return root;
    return 0;
}
//---------------------------------------------------------------------------
PlanGen::Problem* PlanGen::buildUnion(const vector<QueryGraph::SubQuery>& query,
        const QueryGraph &entirePlan,
        uint64_t id, bool completeEstimate)
    // Generate a union part
{
    // Solve the subproblems
    vector<Plan*> parts, solutions;
    for (unsigned index = 0; index < query.size(); index++) {
        Plan* p = translate_int(query[index], entirePlan, completeEstimate), *bp = p;
        for (Plan* iter = p; iter; iter = iter->next)
            if (iter->costs < bp->costs)
                bp = iter;
        parts.push_back(bp);
        solutions.push_back(p);
    }

    // Compute statistics
    Plan::card_t card = 0;
    Plan::cost_t costs = 0;
    for (vector<Plan*>::const_iterator iter = parts.begin(), limit = parts.end(); iter != limit; ++iter) {
        card += (*iter)->cardinality;
        costs += (*iter)->costs;
    }

    // Create a new problem instance
    Problem* result = problems.alloc();
    result->next = 0;
    result->plans = 0;
    result->relations = BitSet();
    result->relations.set(id);

    // And create a union operator
    Plan* last = plans->alloc();
    last->op = Plan::Union;
    last->opArg = 0;
    last->left = parts[0];
    last->right = parts.size() > 1 ? parts[1] : NULL;
    last->cardinality = card;
    last->costs = costs;
    last->ordering = ~0u;
    last->next = 0;
    result->plans = last;
    for (unsigned index = 2; index < parts.size(); index++) {
        Plan* nextPlan = plans->alloc();
        nextPlan->left = last->right;
        last->right = nextPlan;
        last = nextPlan;
        last->op = Plan::Union;
        last->opArg = 0;
        last->right = parts[index];
        last->cardinality = card;
        last->costs = costs;
        last->ordering = ~0u;
        last->next = 0;
    }

    // Could we also use a merge union?
    set<unsigned> otherVars, unionVars;
    vector<unsigned> commonVars;
    collectVariables(*fullQuery, otherVars, &query);
    for (vector<QueryGraph::SubQuery>::const_iterator iter = query.begin(), limit = query.end(); iter != limit; ++iter)
        collectVariables(*iter, unionVars, 0);
    set_intersection(otherVars.begin(), otherVars.end(), unionVars.begin(), unionVars.end(), back_inserter(commonVars));
    if (commonVars.size() == 1) {
        unsigned resultVar = commonVars[0];
        // Can we get all plans sorted in this way?
        bool canMerge = true;
        costs = 0;
        for (vector<Plan*>::const_iterator iter = solutions.begin(), limit = solutions.end(); iter != limit; ++iter) {
            Plan* p;
            if ((p = findOrdering(*iter, resultVar)) == 0) {
                canMerge = false;
                break;
            }
            costs += p->costs;
        }
        // Yes, build the plan
        if (canMerge) {
            Plan* last = plans->alloc();
            last->op = Plan::MergeUnion;
            last->opArg = 0;
            last->left = findOrdering(solutions[0], resultVar);
            last->right = findOrdering(solutions[1], resultVar);
            last->cardinality = card;
            last->costs = costs;
            last->ordering = resultVar;
            last->next = 0;
            result->plans->next = last;
            for (unsigned index = 2; index < solutions.size(); index++) {
                Plan* nextPlan = plans->alloc();
                nextPlan->left = last->right;
                last->right = nextPlan;
                last = nextPlan;
                last->op = Plan::MergeUnion;
                last->opArg = 0;
                last->right = findOrdering(solutions[index], resultVar);
                last->cardinality = card;
                last->costs = costs;
                last->ordering = resultVar;
                last->next = 0;
            }
        }
    }

    return result;
}
//---------------------------------------------------------------------------
PlanGen::Problem* PlanGen::buildTableFunction(const QueryGraph::TableFunction& /*function*/, uint64_t id)
    // Generate a table function access
{
    // Create new problem instance
    Problem* result = problems.alloc();
    result->next = 0;
    result->plans = 0;
    result->relations = BitSet();
    result->relations.set(id);

    return result;
}
//---------------------------------------------------------------------------
static void findFilters(Plan* plan, set<const QueryGraph::Filter*>& filters)
    // Find all filters already applied in a plan
{
    switch (plan->op) {
        case Plan::Union:
        case Plan::MergeUnion:
            // A nested subquery starts here, stop
            break;
        case Plan::IndexScan:
        case Plan::AggregatedIndexScan:
        case Plan::FullyAggregatedIndexScan:
        case Plan::Singleton:
            // We reached a leaf.
            break;
        case Plan::Filter:
            filters.insert(reinterpret_cast<FilterArgs*>(plan->right)->filter);
            findFilters(plan->left, filters);
            break;
        case Plan::NestedLoopJoin:
        case Plan::MergeJoin:
        case Plan::HashJoin:
        case Plan::CartProd:
            findFilters(plan->left, filters);
            findFilters(plan->right, filters);
            break;
        case Plan::HashGroupify:
        case Plan::TableFunction:
            findFilters(plan->left, filters);
            break;
        case Plan::Subselect:
            break;
        case Plan::ValuesScan:
            break;
        case Plan::Minus:
            findFilters(plan->left, filters);
            break;
        case Plan::Having:
        case Plan::GroupBy:
        case Plan::Aggregates:
            findFilters(plan->left, filters);
            break;
    }
}
//---------------------------------------------------------------------------
Plan* PlanGen::translate_int(const QueryGraph::SubQuery& query,
        const QueryGraph &entirePlan,
        bool completeEstimate)

    // Translate a query into an operator tree
{
    bool singletonNeeded = (!(query.nodes.size() +
                query.optional.size() +
                query.unions.size())) && query.tableFunctions.size();

    // Check if we could handle the query
    if ((query.nodes.size() + query.optional.size() + query.unions.size() +
                query.subqueries.size() + query.tableFunctions.size() +
                singletonNeeded) > BitSet::maxWidth)
        return 0;

    //Is it a subselect?
    std::vector<Plan*> subqueryPlans;
    for (std::vector<std::shared_ptr<QueryGraph>>::const_iterator itr = query.subqueries.begin(); itr != query.subqueries.end(); ++itr) {
        PlanGen p(plans);
        p.init(db, *itr->get());
        Plan* childPlan = p.translate_int((*itr)->getQuery(), *itr->get(), completeEstimate);
        Plan* plan = plans->alloc();
        plan->op = Plan::Subselect;
        plan->left = childPlan;
        plan->right = reinterpret_cast<Plan*>(
                const_cast<QueryGraph*>(itr->get()));
        //Little tweak: if there is a limit, then this limits the cardinality
        plan->cardinality = childPlan->cardinality;
        if (plan->cardinality > (*itr)->getLimit()) {
            plan->cardinality = (*itr)->getLimit();
        }
        subqueryPlans.push_back(plan);
    }

    // Seed the DP table with scans
    vector<Problem*> dpTable;
    dpTable.resize(query.nodes.size() + query.optional.size() +
            query.unions.size() + query.subqueries.size() +
            query.tableFunctions.size() + query.valueNodes.size() +
            singletonNeeded);

    Problem* last = 0;
    unsigned id = 0;
    for (vector<QueryGraph::Node>::const_iterator iter = query.nodes.begin(), limit = query.nodes.end(); iter != limit; ++iter, ++id) {
        Problem* p;
        p = buildScan(query, *iter, id);
        if (last)
            last->next = p;
        else
            dpTable[0] = p;
        last = p;
    }
    for(auto &iter : query.valueNodes) {
        Problem* p;
        p = buildValue(query, iter, id++);
        if (last)
            last->next = p;
        else
            dpTable[0] = p;
        last = p;
    }
    for (vector<QueryGraph::SubQuery>::const_iterator iter = query.optional.begin(), limit = query.optional.end(); iter != limit; ++iter, ++id) {
        Problem* p = buildOptional(*iter, entirePlan, id, completeEstimate);
        if (last)
            last->next = p;
        else
            dpTable[0] = p;
        last = p;
    }
    for (vector<vector<QueryGraph::SubQuery> >::const_iterator iter = query.unions.begin(), limit = query.unions.end(); iter != limit; ++iter, ++id) {
        Problem* p = buildUnion(*iter, entirePlan, id, completeEstimate);
        if (last)
            last->next = p;
        else
            dpTable[0] = p;
        last = p;
    }

    for (unsigned i = 0; i < subqueryPlans.size(); ++i, ++id) {
        Plan *subqueryPlan = subqueryPlans[i];
        // Create new problem instance
        Problem* p = problems.alloc();
        p->next = 0;
        p->plans = subqueryPlan;
        p->relations = BitSet();
        p->relations.set(id);
        if (last)
            last->next = p;
        else
            dpTable[0] = p;
        last = p;
    }

    unsigned functionIds = id;
    for (vector<QueryGraph::TableFunction>::const_iterator iter = query.tableFunctions.begin(), limit = query.tableFunctions.end(); iter != limit; ++iter, ++id) {
        Problem* p = buildTableFunction(*iter, id);
        if (last)
            last->next = p;
        else
            dpTable[0] = p;
        last = p;
    }
    unsigned singletonId = id;
    if (singletonNeeded) {
        Plan* plan = plans->alloc();
        plan->op = Plan::Singleton;
        plan->opArg = 0;
        plan->left = 0;
        plan->right = 0;
        plan->next = 0;
        plan->cardinality = 1;
        plan->ordering = ~0u;
        plan->costs = 0;

        Problem* problem = problems.alloc();
        problem->next = 0;
        problem->plans = plan;
        problem->relations = BitSet();
        problem->relations.set(id);
        if (last)
            last->next = problem;
        else
            dpTable[0] = problem;
        last = problem;
    }

    // Construct the join info
    vector<JoinDescription> joins;

    for (vector<QueryGraph::Edge>::const_iterator iter = query.edges.begin(), limit = query.edges.end(); iter != limit; ++iter)
        joins.push_back(buildJoinInfo(query, *iter));

    id = functionIds;
    for (vector<QueryGraph::TableFunction>::const_iterator iter = query.tableFunctions.begin(), limit = query.tableFunctions.end(); iter != limit; ++iter, ++id) {
        JoinDescription join;
        set<unsigned> input;
        for (vector<QueryGraph::TableFunction::Argument>::const_iterator iter2 = (*iter).input.begin(), limit2 = (*iter).input.end(); iter2 != limit2; ++iter2)
            if (~(*iter2).id)
                input.insert((*iter2).id);
        for (unsigned index = 0; index < query.nodes.size(); index++) {
            uint64_t s = query.nodes[index].constSubject ? INT64_MAX : query.nodes[index].subject;
            uint64_t p = query.nodes[index].constPredicate ? INT64_MAX : query.nodes[index].predicate;
            uint64_t o = query.nodes[index].constObject ? INT64_MAX : query.nodes[index].object;
            if (input.count(s) || input.count(p) || input.count(o) || input.empty())
                join.left.set(index);
        }
        if (singletonNeeded && input.empty())
            join.left.set(singletonId);
        for (unsigned index = 0; index < query.tableFunctions.size(); index++) {
            bool found = false;
            for (vector<unsigned>::const_iterator iter = query.tableFunctions[index].output.begin(), limit = query.tableFunctions[index].output.end(); iter != limit; ++iter)
                if (input.count(*iter)) {
                    found = true;
                    break;
                }
            if (found)
                join.left.set(functionIds + index);
        }
        join.right.set(id);
        join.ordering = ~0u;
        join.selectivity = 1;
        join.tableFunction = &(*iter);
        joins.push_back(join);
    }

    // Build larger join trees
    vector<unsigned> joinOrderings;
    for (unsigned index = 1; index < dpTable.size(); index++) {
        map<BitSet, Problem*> lookup;
        for (unsigned index2 = 0; index2 < index; index2++) {
            for (Problem* iter = dpTable[index2]; iter; iter = iter->next) {
                BitSet leftRel = iter->relations;
                for (Problem* iter2 = dpTable[index - index2 - 1]; iter2; iter2 = iter2->next) {
                    // Overlapping subproblem?
                    BitSet rightRel = iter2->relations;
                    if (leftRel.overlapsWith(rightRel))
                        continue;

                    // Investigate all join candidates
                    Problem* problem = 0;
                    double selectivity = 1;
                    for (vector<JoinDescription>::const_iterator iter3 = joins.begin(), limit3 = joins.end(); iter3 != limit3; ++iter3)
                        if (((*iter3).left.subsetOf(leftRel)) && ((*iter3).right.subsetOf(rightRel))) {
                            if (!iter->plans)
                                break;
                            // We can join it...
                            BitSet relations = leftRel.unionWith(rightRel);
                            if (lookup.count(relations)) {
                                problem = lookup[relations];
                            } else {
                                lookup[relations] = problem = problems.alloc();
                                problem->relations = relations;
                                problem->plans = 0;
                                problem->next = dpTable[index];
                                dpTable[index] = problem;
                            }
                            // Table function call?
                            if ((*iter3).tableFunction) {
                                for (Plan* leftPlan = iter->plans; leftPlan; leftPlan = leftPlan->next) {
                                    Plan* p = plans->alloc();
                                    p->op = Plan::TableFunction;
                                    p->opArg = 0;
                                    p->right = reinterpret_cast<Plan*>(const_cast<QueryGraph::TableFunction*>((*iter3).tableFunction));

                                    //Left plan must be enriched with the filtering operations
                                    if (iter3->tableFunction->associatedFilter != NULL) {
                                        p->left = attachFiltersToPlan(iter3->tableFunction->associatedFilter.get(), leftPlan);
                                    } else {
                                        p->left = leftPlan;
                                    }

                                    p->next = 0;
                                    p->cardinality = leftPlan->cardinality;
                                    p->costs = leftPlan->costs + Costs::tableFunction(leftPlan->cardinality);
                                    p->ordering = leftPlan->ordering;
                                    addPlan(problem, p);
                                }

                                problem = 0;
                                break;
                            }
                            // Collect selectivities and join order candidates
                            joinOrderings.clear();
                            joinOrderings.push_back((*iter3).ordering);
                            selectivity = (*iter3).selectivity;
                            for (++iter3; iter3 != limit3; ++iter3) {
                                joinOrderings.push_back((*iter3).ordering);
                                // selectivity*=(*iter3).selectivity;
                            }
                            break;
                        }
                    if (!problem)
                        continue;

                    // Combine phyiscal plans
                    for (Plan* leftPlan = iter->plans; leftPlan; leftPlan = leftPlan->next) {
                        for (Plan* rightPlan = iter2->plans; rightPlan; rightPlan = rightPlan->next) {
                            // Try a merge joins
                            if (leftPlan->ordering == rightPlan->ordering) {
                                for (vector<unsigned>::const_iterator iter = joinOrderings.begin(), limit = joinOrderings.end(); iter != limit; ++iter) {
                                    if (leftPlan->ordering == (*iter)) {
                                        Plan* p = plans->alloc();
                                        p->op = Plan::MergeJoin;
                                        p->opArg = *iter;
                                        p->left = leftPlan;
                                        p->right = rightPlan;
                                        p->next = 0;
                                        if ((p->cardinality = leftPlan->cardinality * rightPlan->cardinality * selectivity) < 1) p->cardinality = 1;
                                        p->costs = leftPlan->costs + rightPlan->costs + Costs::mergeJoin(leftPlan->cardinality, rightPlan->cardinality);
                                        p->ordering = leftPlan->ordering;
                                        addPlan(problem, p);
                                        break;
                                    }
                                }
                            }
                            // Try a hash join
                            if (selectivity >= 0) {
                                Plan* p = plans->alloc();
                                p->op = Plan::HashJoin;
                                p->opArg = 0;
                                p->left = leftPlan;
                                p->right = rightPlan;
                                p->next = 0;
                                if ((p->cardinality = leftPlan->cardinality * rightPlan->cardinality * selectivity) < 1) p->cardinality = 1;
                                p->costs = leftPlan->costs + rightPlan->costs + Costs::hashJoin(leftPlan->cardinality, rightPlan->cardinality);
                                p->ordering = ~0u;
                                addPlan(problem, p);
                                // Second order
                                p = plans->alloc();
                                p->op = Plan::HashJoin;
                                p->opArg = 0;
                                p->left = rightPlan;
                                p->right = leftPlan;
                                p->next = 0;
                                if ((p->cardinality = leftPlan->cardinality * rightPlan->cardinality * selectivity) < 1) p->cardinality = 1;
                                p->costs = leftPlan->costs + rightPlan->costs + Costs::hashJoin(rightPlan->cardinality, leftPlan->cardinality);
                                p->ordering = ~0u;
                                addPlan(problem, p);
                            } else {
                                // Nested loop join
                                Plan* p = plans->alloc();
                                p->op = Plan::NestedLoopJoin;
                                p->opArg = 0;
                                p->left = leftPlan;
                                p->right = rightPlan;
                                p->next = 0;
                                if ((p->cardinality = leftPlan->cardinality * rightPlan->cardinality) < 1) p->cardinality = 1;
                                p->costs = leftPlan->costs + rightPlan->costs + leftPlan->cardinality * rightPlan->costs;
                                p->ordering = leftPlan->ordering;
                                addPlan(problem, p);
                            }
                        }
                    }
                }
            }
        }
        if (dpTable[index] == NULL) { //No join was found. Yet, I need to include some problems that are not considered. Use cartesian product.
            for (unsigned index2 = 0; index2 < index && !dpTable[index]; index2++) {
                for (Problem* iter = dpTable[index2]; iter && !dpTable[index]; iter = iter->next) {
                    BitSet leftRel = iter->relations;
                    for (Problem* iter2 = dpTable[index - index2 - 1]; iter2; iter2 = iter2->next) {
                        BitSet rightRel = iter2->relations;
                        if (leftRel.overlapsWith(rightRel))
                            continue;
                        Problem* problem = 0;
                        BitSet relations = leftRel.unionWith(rightRel);
                        lookup[relations] = problem = problems.alloc();
                        problem->relations = relations;
                        problem->plans = 0;
                        problem->next = dpTable[index];
                        dpTable[index] = problem;

                        //Create a plan for the two
                        for (Plan* leftPlan = iter->plans; leftPlan; leftPlan = leftPlan->next) {
                            for (Plan* rightPlan = iter2->plans; rightPlan; rightPlan = rightPlan->next) {
                                Plan* p = plans->alloc();
                                p->op = Plan::CartProd;
                                p->opArg = 0;
                                p->left = leftPlan;
                                p->right = rightPlan;
                                p->next = 0;
                                p->cardinality = leftPlan->cardinality * rightPlan->cardinality;
                                p->costs = (leftPlan->costs + leftPlan->cardinality) * (rightPlan->costs + rightPlan->cardinality);
                                p->ordering = ~0u;
                                addPlan(problem, p);
                            }
                        }
                        break;
                    }
                }
            }
        }
    }

    // Extract the bestplan
    if (dpTable.empty())
        return 0;
    if (dpTable.back() == NULL) {
        cerr << "Something went wrong...";
        throw 10;
    }
    Plan* plan =  dpTable.back()->plans;

    // Add all remaining filters
    set<const QueryGraph::Filter*> appliedFilters;
    findFilters(plan, appliedFilters);
    for (vector<QueryGraph::Filter>::const_iterator iter = query.filters.begin(),
            limit = query.filters.end(); iter != limit; ++iter)
        if (!appliedFilters.count(&(*iter))) {
            Plan* p = plans->alloc();
            p->op = Plan::Filter;
            p->opArg = 0;
            p->left = plan;

            Plan *filterPlan = NULL;
            //If not exist, then there is a subgraph to build
            if (iter->type == QueryGraph::Filter::Builtin_notexists) {
                filterPlan = buildFilterPlan(&*iter);
            }
            FilterArgs *fa = plans->allocFilterArgs();
            fa->filter = &*iter;
            fa->plan = filterPlan;
            p->right = reinterpret_cast<Plan*>(fa);

            p->next = 0;
            p->cardinality = plan->cardinality; // XXX real computation
            p->costs = plan->costs;
            p->ordering = plan->ordering;
            plan = p;
        }

    //Is there a minus
    for (const auto &itr : query.minuses) {
        Plan* subqueryPlan = translate_int(itr->getQuery(), *itr, completeEstimate);
        subqueryPlan->subquery = itr;
        Plan* p = plans->alloc();
        p->op = Plan::Minus;
        p->left = plan;
        p->right = subqueryPlan;
        p->cardinality = plan->cardinality;
        plan = p;
    }

    //GroupBy
    bool addedGroupBy = false;
    if (!entirePlan.getGroupBy().empty()) {
        //Add an operator that groups the variables in some groups
        Plan* p = plans->alloc();
        p->op = Plan::GroupBy;
        p->opArg = entirePlan.c_getAggredateHandler().empty();
        p->left = plan;
        p->right = (Plan*)&entirePlan.getGroupBy();
        p->next = 0;
        p->cardinality = plan->cardinality;
        p->costs = plan->costs;
        p->ordering = ~0u;
        addedGroupBy = true;
        plan = p;
    }

    //Aggregates
    if (!entirePlan.c_getAggredateHandler().empty()) {
        if (!addedGroupBy) {
            Plan* p = plans->alloc();
            p->op = Plan::GroupBy;
            p->opArg = 0;
            p->left = plan;
            p->right = NULL;
            p->next = 0;
            p->cardinality = plan->cardinality;
            p->costs = plan->costs;
            p->ordering = ~0u;
            addedGroupBy = true;
            plan = p;
        }

        Plan* p = plans->alloc();
        p->op = Plan::Aggregates;
        p->opArg = 0;
        p->left = plan;
        p->right = (Plan*)&entirePlan;
        p->next = 0;
        p->cardinality = plan->cardinality;
        p->costs = plan->costs;
        p->ordering = ~0u;
        plan = p;
    }

    //Having
    if (!entirePlan.getHavings().empty()) {
        if (!addedGroupBy) {
            Plan* p = plans->alloc();
            p->op = Plan::GroupBy;
            p->opArg = entirePlan.c_getAggredateHandler().empty();
            p->left = plan;
            p->right = NULL;
            p->next = 0;
            p->cardinality = plan->cardinality;
            p->costs = plan->costs;
            p->ordering = ~0u;
            addedGroupBy = true;
            plan = p;
        }

        Plan* p = plans->alloc();
        p->op = Plan::Having;
        p->opArg = 0;
        p->left = plan;
        p->right = (Plan*)&entirePlan.getHavings();
        p->next = 0;
        p->cardinality = plan->cardinality;
        p->costs = plan->costs;
        p->ordering = ~0u;
        plan = p;
    }

    //Global functions (e.g., AS)
    if (!entirePlan.c_getGlobalAssignments().empty()) {
        for(const auto &t: entirePlan.c_getGlobalAssignments()) {
            Plan* p = plans->alloc();
            p->op = Plan::TableFunction;
            p->opArg = 0;
            p->right = reinterpret_cast<Plan*>(
                    const_cast<QueryGraph::TableFunction*>(&t));
            //Left plan must be enriched with the filtering operations
            if (t.associatedFilter != NULL) {
                p->left = attachFiltersToPlan(
                        t.associatedFilter.get(),
                        plan);
            } else {
                p->left = plan;
            }
            p->next = 0;
            p->cardinality = plan->cardinality;
            p->costs = plan->costs + Costs::tableFunction(plan->cardinality);
            p->ordering = plan->ordering;
            plan = p;
        }
    }

    // Return the complete plan
    return plan;
}
//---------------------------------------------------------------------------
/*Plan* PlanGen::translate_noopt(DBLayer& db, const QueryGraph& query,
  const std::vector<int> &optimalOrder) {
  fullQuery = &query;
  unsigned id = 0;
  auto q = query.getQuery();
  std::vector<Problem*> problems(optimalOrder.size());
  for (int i = 0; i < optimalOrder.size(); ++i) {
  QueryGraph::Node n = q.nodes[optimalOrder[i]];
  Problem* p = buildScan(q, n, id);
  problems[i] = p;
  }

// Construct the join info
vector<JoinDescription> joins;
for (vector<QueryGraph::Edge>::const_iterator iter = q.edges.begin(), limit = q.edges.end(); iter != limit; ++iter)
joins.push_back(buildJoinInfo(q, *iter));

for (unsigned index = 1; index < problems.size(); index++) {
//Must decide which join to make...
Problem* problem = this->problems.alloc();
for (Plan *p1 = problems[index - 1]->plans; p1; p1 = p1->next) {
for (Plan *p2 = problems[index]->plans; p2; p2 = p2->next) {
Plan* p = plans.alloc();
p->left = p1;
p->right = p2;
p->next = 0;
if ((p->cardinality = p1->cardinality * p2->cardinality) < 1) p->cardinality = 1;
if (p1->ordering == p2->ordering) {
p->op = Plan::MergeJoin;
p->opArg = p1->ordering;
p->costs = p1->costs + p2->costs + Costs::mergeJoin(p1->cardinality, p2->cardinality);
p->ordering = p1->ordering;
} else {
p->op = Plan::HashJoin;
if (p2->cardinality < p1->cardinality) {
p->left = p2;
p->right = p1;
}
p->opArg = 0;
p->costs = p->left->costs + p->right->costs + Costs::hashJoin(p->left->cardinality, p->right->cardinality);
p->ordering = ~0u;
}
addPlan(problem, p);
}
}
problems[index] = problem;
}

//Get the best plan...
Plan *finalPlan = NULL;
if (problems.back()) {
Problem *p = problems.back();
//Get the plan with the smallest cost
for (Plan *plan = p->plans; plan; plan = plan->next) {
if (!finalPlan || plan->costs < finalPlan->costs) {
finalPlan = plan;
}
}
}
return finalPlan;

}*/
//---------------------------------------------------------------------------
void PlanGen::init(DBLayer* db, const QueryGraph& query) {
    // Reset the plan generator
    problems.freeAll();
    this->db = db;
    fullQuery = &query;
}
//---------------------------------------------------------------------------
Plan* PlanGen::translate(DBLayer& db, const QueryGraph& query, bool completeEstimate)
    // Translate a query into an operator tree
{
    // Reset the plan generator
    plans->clear();
    problems.freeAll();
    this->db = &db;
    fullQuery = &query;

    // Retrieve the base plan
    Plan* plan = translate_int(query.getQuery(), query, completeEstimate);
    if (!plan)
        return 0;
    Plan* best = 0;
    for (Plan* iter = plan; iter; iter = iter->next)
        if ((!best) || (iter->costs < best->costs) || ((iter->costs == best->costs) && (iter->cardinality < best->cardinality)))
            best = iter;
    if (!best)
        return 0;

    // Aggregate, if required
    if ((query.getDuplicateHandling() == QueryGraph::CountDuplicates) || (query.getDuplicateHandling() == QueryGraph::NoDuplicates) || (query.getDuplicateHandling() == QueryGraph::ShowDuplicates)) {
        Plan* p = plans->alloc();
        p->op = Plan::HashGroupify;
        p->opArg = 0;
        p->left = best;
        p->right = 0;
        p->next = 0;
        p->cardinality = best->cardinality; // not correct, be we do not use this value anyway
        p->costs = best->costs; // the same here
        p->ordering = ~0u;
        best = p;
    }

    return best;
}
//---------------------------------------------------------------------------
