#include "rts/operator/Sort.hpp"
#include "infra/util/Type.hpp"
#include "rts/operator/PlanPrinter.hpp"
#include "rts/runtime/Runtime.hpp"
#include <trident/kb/dictmgmt.h>

#include <kognac/consts.h>
#include <algorithm>
//---------------------------------------------------------------------------
// RDF-3X
// (c) 2009 Thomas Neumann. Web site: http://www.mpi-inf.mpg.de/~neumann/rdf3x
//
// This work is licensed under the Creative Commons
// Attribution-Noncommercial-Share Alike 3.0 Unported License. To view a copy
// of this license, visit http://creativecommons.org/licenses/by-nc-sa/3.0/
// or send a letter to Creative Commons, 171 Second Street, Suite 300,
// San Francisco, California, 94105, USA.
//---------------------------------------------------------------------------
using namespace std;
//---------------------------------------------------------------------------
/// Comparator
class Sort::Sorter {
private:
    /// The dictionary
    DBLayer& dict;
    /// The sort order
    const vector<Order>& order;
    std::unique_ptr<char> buffer1;
    std::unique_ptr<char> buffer2;

public:
    /// Constructor
    Sorter(const Sorter &c) : dict(c.dict), order(c.order), buffer1(new char[MAX_TERM_SIZE]), buffer2(new char[MAX_TERM_SIZE]) {}

    Sorter(DBLayer& dict, const vector<Order>& order) : dict(dict), order(order), buffer1(new char[MAX_TERM_SIZE]), buffer2(new char[MAX_TERM_SIZE]) {}

    /// Compare
    bool operator()(const Tuple* a, const Tuple* b);
};
//---------------------------------------------------------------------------
bool Sort::Sorter::operator()(const Tuple* a, const Tuple* b)
// Compare
{
    for (vector<Order>::const_iterator iter = order.begin(), limit = order.end(); iter != limit; ++iter) {
        uint64_t slot = (*iter).slot;
        if (~slot) {
            // Access values
            uint64_t v1, v2;
            if ((*iter).descending) {
                v1 = b->values[slot];
                v2 = a->values[slot];
            } else {
                v1 = a->values[slot];
                v2 = b->values[slot];
            }

            // Equal?
            if (v1 == v2) continue;

            // Null values
            if (!~v1) return true;
            if (!~v2) return false;

	    if (DictMgmt::isnumeric(v1)) {
		if (! DictMgmt::isnumeric(v2)) {
		    throw 10;
		}
		uint64_t t1 = DictMgmt::getType(v1);
		uint64_t t2 = DictMgmt::getType(v2);
		int retval = DictMgmt::compare(t1, v1, t2, v2);
		if (retval == 0) {
		    continue;
		}
		return retval < 0;
	    } else if (DictMgmt::isnumeric(v2)) {
		throw 10;
	    }

            // Load the strings
            const char* start1, *stop1, *start2, *stop2;
            size_t len1, len2;
            Type::ID type1, type2;
            unsigned subType1, subType2;
            if (!dict.lookupById(v1, buffer1.get(), len1, type1, subType1)) continue;
            if (!dict.lookupById(v2, buffer2.get(), len2, type2, subType2)) continue;
            start1 = buffer1.get();
            start2 = buffer2.get();
            stop1 = start1 + len1;
            stop2 = start2 + len2;

            // Compare
            if (type1 < type2) return true;
            if (type1 > type2) return false;
            if (Type::hasSubType(type1)) {
                if (subType1 < subType2) return true;
                if (subType1 > subType2) return false;
            }
            int c = memcmp(start1, start2, min(stop1 - start1, stop2 - start2));
            if (c < 0) return true;
            if (c > 0) return false;
            if ((stop1 - start1) < (stop2 - start2)) return true;
            if ((stop1 - start1) > (stop2 - start2)) return false;

            // Tie breaker. Should not be necessary...
            if (v1 < v2) return true;
            if (v1 > v2) return false;
        } else {
            // Sort by count
            if ((*iter).descending) {
                if (a->count > b->count) return true;
                if (a->count < b->count) return false;
            } else {
                if (a->count < b->count) return true;
                if (a->count > b->count) return false;
            }
        }
    }
    return false;
}
//---------------------------------------------------------------------------
Sort::Sort(DBLayer& db, Operator* input, const vector<Register*>& values, const vector<pair<Register*, bool> >& registerOrder, double expectedOutputCardinality)
    : Operator(expectedOutputCardinality), values(values), input(input), tuplesPool(values.size() * sizeof(uint64_t)), dict(db)
      // Constructor
{
    for (vector<pair<Register*, bool> >::const_iterator iter = registerOrder.begin(), limit = registerOrder.end(); iter != limit; ++iter) {
        if (!(*iter).first) {
            Order o;
            o.slot = ~0u;
            o.descending = (*iter).second;
            order.push_back(o);
        } else {
            uint64_t slot = ~0u;
            for (uint64_t index = 0; index < values.size(); index++)
                if ((*iter).first == values[index]) {
                    slot = index;
                    break;
                }
            if (~slot) {
                Order o;
                o.slot = slot;
                o.descending = (*iter).second;
                order.push_back(o);
            }
        }
    }
}
//---------------------------------------------------------------------------
Sort::~Sort()
// Destructor
{
    delete input;
}
//---------------------------------------------------------------------------
uint64_t Sort::first()
// Produce the first tuple
{
    observedOutputCardinality = 0;

    // Collect the input
    tuples.clear();
    tuplesPool.freeAll();
    for (uint64_t count = input->first(); count; count = input->next()) {
        Tuple* t = tuplesPool.alloc();
        t->count = count;
        for (uint64_t index = 0, limit = values.size(); index < limit; index++) {
            t->values[index] = values[index]->value;
        }
        tuples.push_back(t);
    }

    // Sort it
    sort(tuples.begin(), tuples.end(), Sorter(dict, order));

    // Return the first one
    tuplesIter = tuples.begin();
    return next();
}
//---------------------------------------------------------------------------
uint64_t Sort::next()
// Produce the next tuple
{
    // End of input
    if (tuplesIter == tuples.end())
        return 0;

    // Produce the next tuple
    const Tuple& t = **tuplesIter;
    for (uint64_t index = 0, limit = values.size(); index < limit; index++)
        values[index]->value = t.values[index];
    uint64_t count = t.count;
    ++tuplesIter;

    observedOutputCardinality += count;
    return count;
}
//---------------------------------------------------------------------------
void Sort::print(PlanPrinter& out)
// Print the operator tree. Debugging only.
{
    out.beginOperator("Sort", expectedOutputCardinality, observedOutputCardinality);
    string o = "[";
    for (uint64_t index = 0, limit = order.size(); index < limit; index++) {
        if (index) o += " ";
        if (~order[index].slot)
            o += out.formatRegister(values[order[index].slot]);
        else
            o += "count";
        if (order[index].descending)
            o += " desc";
    }
    o += "]";
    out.addGenericAnnotation(o);
    out.addMaterializationAnnotation(values);
    input->print(out);
    out.endOperator();
}
//---------------------------------------------------------------------------
void Sort::addMergeHint(Register* /*reg1*/, Register* /*reg2*/)
// Add a merge join hint
{
    // Do not propagate as we break the pipeline
}
//---------------------------------------------------------------------------
void Sort::getAsyncInputCandidates(Scheduler& scheduler)
// Register parts of the tree that can be executed asynchronous
{
    input->getAsyncInputCandidates(scheduler);
}
//---------------------------------------------------------------------------
