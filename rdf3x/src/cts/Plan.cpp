#include "cts/plangen/Plan.hpp"
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
PlanContainer::PlanContainer()
    // Constructor
{
}
//---------------------------------------------------------------------------
PlanContainer::~PlanContainer()
    // Destructor
{
}
//---------------------------------------------------------------------------
void PlanContainer::clear()
    // Release all plans
{
    pool.freeAll();
    poolFilterArgs.freeAll();
}
void PlanContainer::free(Plan *p)
{
    pool.free(p);
}
void PlanContainer::freeFilterArgs(FilterArgs *p)
{
    poolFilterArgs.free(p);
}
//---------------------------------------------------------------------------
void Plan::print(unsigned indent) const
// Print the plan
{
    for (unsigned index = 0; index < indent; index++)
        cout << ' ';
    switch (op) {
        case IndexScan:
            cout << "IndexScan";
            break;
        case AggregatedIndexScan:
            cout << "AggregatedIndexScan";
            break;
        case FullyAggregatedIndexScan:
            cout << "FullyAggregatedIndexScan";
            break;
        case NestedLoopJoin:
            cout << "NestedLoopJoin";
            break;
        case MergeJoin:
            cout << "MergeJoin";
            break;
        case HashJoin:
            cout << "HashJoin";
            break;
        case CartProd:
            cout << "CartesianProduct";
            break;
        case HashGroupify:
            cout << "HashGroupify";
            break;
        case Filter:
            cout << "Filter";
            break;
        case Union:
            cout << "Union";
            break;
        case MergeUnion:
            cout << "MergeUnion";
            break;
        case TableFunction:
            cout << "TableFunction";
            break;
        case Singleton:
            cout << "Singleton";
            break;
        case Subselect:
            cout << "Subselect";
            break;
        case Minus:
            cout << "Minus";
	    break;
        case ValuesScan:
            cout << "Values";
	    break;
	case GroupBy:
            cout << "GroupBy";
	    break;
	case Having:
            cout << "Having";
	    break;
	case Aggregates:
            cout << "Aggregates";
	    break;
    }
    cout << " cardinality=" << cardinality << " costs=" << costs << endl;
    switch (op) {
        case IndexScan:
            break;
        case AggregatedIndexScan:
            break;
        case FullyAggregatedIndexScan:
            break;
        case NestedLoopJoin:
        case MergeJoin:
        case HashJoin:
        case CartProd:
            left->print(indent + 1);
            right->print(indent + 1);
            break;
        case HashGroupify:
        case Filter:
            left->print(indent + 1);
            break;
        case Union:
        case Subselect:
            left->print(indent + 1);
            break;
        case Minus:
            left->print(indent + 1);
            right->print(indent + 1);
            break;
        case MergeUnion:
            left->print(indent + 1);
            right->print(indent + 1);
            break;
        case TableFunction:
            left->print(indent + 1);
            break;
        case Singleton:
            break;
        case ValuesScan:
            break;
	case GroupBy:
	case Having:
	case Aggregates:
	    break;
    }
}
//---------------------------------------------------------------------------
