#include "rts/operator/FullyAggregatedIndexScan.hpp"
#include "rts/operator/PlanPrinter.hpp"
#include "rts/runtime/Runtime.hpp"
#include <cassert>
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
/// Implementation
class FullyAggregatedIndexScan::Scan : public FullyAggregatedIndexScan {
public:
    /// Constructor
    Scan(DBLayer& db, DBLayer::DataOrder order, Register* value1, bool bound1, double expectedOutputCardinality) : FullyAggregatedIndexScan(db, order, value1, bound1, expectedOutputCardinality) {}

    /// First tuple
    uint64_t first();
    /// Next tuple
    uint64_t next();
};
//---------------------------------------------------------------------------
/// Implementation
class FullyAggregatedIndexScan::ScanPrefix1 : public FullyAggregatedIndexScan {
private:
    /// The stop condition
    uint64_t stop1;

public:
    /// Constructor
    ScanPrefix1(DBLayer& db, DBLayer::DataOrder order, Register* value1, bool bound1, double expectedOutputCardinality) : FullyAggregatedIndexScan(db, order, value1, bound1, expectedOutputCardinality) {}

    /// First tuple
    uint64_t first();
    /// Next tuple
    uint64_t next();
};
//---------------------------------------------------------------------------
FullyAggregatedIndexScan::FullyAggregatedHint::FullyAggregatedHint(FullyAggregatedIndexScan& scan)
    : scan(scan)
      // Constructor
{
}
//---------------------------------------------------------------------------
FullyAggregatedIndexScan::FullyAggregatedHint::~FullyAggregatedHint()
// Destructor
{
}
//---------------------------------------------------------------------------
void FullyAggregatedIndexScan::FullyAggregatedHint::next(uint64_t& value1)
// Scanning hint
{
    // First value
    if (scan.bound1) {
        uint64_t v = scan.value1->value;
        if ((~v) && (v > value1)) {
            value1 = v;
        }
    }
    for (std::vector<Register*>::const_iterator iter = scan.merge1.begin(), limit = scan.merge1.end(); iter != limit; ++iter) {
        uint64_t v = (*iter)->value;
        if ((~v) && (v > value1)) {
            value1 = v;
        }
    }
    if (scan.value1->domain) {
        uint64_t v = scan.value1->domain->nextCandidate(value1);
        if (v > value1) {
            value1 = v;
        }
    }
}
//---------------------------------------------------------------------------
FullyAggregatedIndexScan::FullyAggregatedIndexScan(DBLayer& db, DBLayer::DataOrder order, Register* value1, bool bound1, double expectedOutputCardinality)
    : Operator(expectedOutputCardinality), value1(value1), bound1(bound1),/*facts(db.getFullyAggregatedFacts(order)),*/order(order), scan(db.getScan(order, DBLayer::AGGR_SKIP_2LAST, &hint)), hint(*this)
      /*,scan(disableSkipping?0:&hint),hint(*this)*/
      // Constructor
{
}
//---------------------------------------------------------------------------
FullyAggregatedIndexScan::~FullyAggregatedIndexScan()
// Destructor
{
}
//---------------------------------------------------------------------------
void FullyAggregatedIndexScan::print(PlanPrinter& out)
// Print the operator tree. Debugging only.
{
    const char* scanType = "";
    switch (order) {
    case DBLayer::Order_No_Order_SPO:
        scanType = "NoOrder_S";
        break;
    case DBLayer::Order_No_Order_SOP:
        scanType = "NoOrder_S";
        break;
    case DBLayer::Order_No_Order_POS:
        scanType = "NoOrder_P";
        break;
    case DBLayer::Order_No_Order_PSO:
        scanType = "NoOrder_P";
        break;
    case DBLayer::Order_No_Order_OPS:
        scanType = "NoOrder_O";
        break;
    case DBLayer::Order_No_Order_OSP:
        scanType = "NoOrder_O";
        break;
    case DBLayer::Order_Subject_Predicate_Object:
        scanType = "Subject";
        break;
    case DBLayer::Order_Subject_Object_Predicate:
        scanType = "Subject";
        break;
    case DBLayer::Order_Object_Predicate_Subject:
        scanType = "Object";
        break;
    case DBLayer::Order_Object_Subject_Predicate:
        scanType = "Object";
        break;
    case DBLayer::Order_Predicate_Subject_Object:
        scanType = "Predicate";
        break;
    case DBLayer::Order_Predicate_Object_Subject:
        scanType = "Predicate";
        break;
    }
    out.beginOperator("FullyAggregatedIndexScan", expectedOutputCardinality, observedOutputCardinality);
    out.addArgumentAnnotation(scanType);
    out.addScanAnnotation(value1, bound1);
    out.endOperator();
}
//---------------------------------------------------------------------------
static void handleHints(Register* reg1, Register* reg2, Register* result, std::vector<Register*>& merges)
// Add hints
{
    bool has1 = false, has2 = false;
    for (std::vector<Register*>::const_iterator iter = merges.begin(), limit = merges.end(); iter != limit; ++iter) {
        if ((*iter) == reg1) has1 = true;
        if ((*iter) == reg2) has2 = true;
    }
    if (reg1 == result) has1 = true;
    if (reg2 == result) has2 = true;

    if (has1 && (!has2)) merges.push_back(reg2);
    if (has2 && (!has1)) merges.push_back(reg1);
}
//---------------------------------------------------------------------------
void FullyAggregatedIndexScan::addMergeHint(Register* reg1, Register* reg2)
// Add a merge join hint
{
    handleHints(reg1, reg2, value1, merge1);
}
//---------------------------------------------------------------------------
void FullyAggregatedIndexScan::getAsyncInputCandidates(Scheduler& /*scheduler*/)
// Register parts of the tree that can be executed asynchronous
{
}
//---------------------------------------------------------------------------
FullyAggregatedIndexScan* FullyAggregatedIndexScan::create(DBLayer& db, DBLayer::DataOrder order, Register* subject, bool subjectBound, Register* predicate, bool predicateBound, Register* object, bool objectBound, double expectedOutputCardinality)
// Constructor
{
    // Setup the slot bindings
    Register* value1 = 0;
    bool bound1 = false;
    switch (order) {
    case DBLayer::Order_No_Order_SPO:
    case DBLayer::Order_Subject_Predicate_Object:
        value1 = subject;
        bound1 = subjectBound;
        assert(!predicate);
        assert(!object);
        break;
    case DBLayer::Order_No_Order_SOP:
    case DBLayer::Order_Subject_Object_Predicate:
        value1 = subject;
        bound1 = subjectBound;
        assert(!object);
        assert(!predicate);
        break;
    case DBLayer::Order_No_Order_OPS:
    case DBLayer::Order_Object_Predicate_Subject:
        value1 = object;
        bound1 = objectBound;
        assert(!predicate);
        assert(!subject);
        break;
    case DBLayer::Order_No_Order_OSP:
    case DBLayer::Order_Object_Subject_Predicate:
        value1 = object;
        bound1 = objectBound;
        assert(!subject);
        assert(!predicate);
        break;
    case DBLayer::Order_No_Order_PSO:
    case DBLayer::Order_Predicate_Subject_Object:
        value1 = predicate;
        bound1 = predicateBound;
        assert(!subject);
        assert(!object);
        break;
    case DBLayer::Order_No_Order_POS:
    case DBLayer::Order_Predicate_Object_Subject:
        value1 = predicate;
        bound1 = predicateBound;
        assert(!object);
        assert(!subject);
        break;
    }

    // Construct the proper operator
    FullyAggregatedIndexScan* result;
    if (!bound1) {
        result = new Scan(db, order, value1, bound1, expectedOutputCardinality);
    } else {
        result = new ScanPrefix1(db, order, value1, bound1, expectedOutputCardinality);
    }

    return result;
}
//---------------------------------------------------------------------------
uint64_t FullyAggregatedIndexScan::Scan::first()
// Produce the first tuple
{
    observedOutputCardinality = 0;
    if (!scan->first())
        return false;
    value1->value = scan->getValue1();

    uint64_t count = scan->getCount();
    observedOutputCardinality += count;
    return count;
}
//---------------------------------------------------------------------------
uint64_t FullyAggregatedIndexScan::Scan::next()
// Produce the next tuple
{
    if (!scan->next())
        return false;
    value1->value = scan->getValue1();

    uint64_t count = scan->getCount();
    observedOutputCardinality += count;
    return count;
}
//---------------------------------------------------------------------------
uint64_t FullyAggregatedIndexScan::ScanPrefix1::first()
// Produce the first tuple
{
    observedOutputCardinality = 0;

    stop1 = value1->value;
    if (!scan->first(stop1, true))
        return false;
    if (scan->getValue1() > stop1)
        return false;

    uint64_t count = scan->getCount();
    observedOutputCardinality += count;
    return count;
}
//---------------------------------------------------------------------------
uint64_t FullyAggregatedIndexScan::ScanPrefix1::next()
// Produce the next tuple
{
    if (!scan->next())
        return false;
    if (scan->getValue1() > stop1)
        return false;

    uint64_t count = scan->getCount();
    observedOutputCardinality += count;
    return count;
}
//---------------------------------------------------------------------------
