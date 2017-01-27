#include "rts/operator/AggregatedIndexScan.hpp"
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
class AggregatedIndexScan::Scan : public AggregatedIndexScan {
public:
    /// Constructor
    Scan(DBLayer& db, DBLayer::DataOrder order, Register* value1, bool bound1, Register* value2, bool bound2, double expectedOutputCardinality) : AggregatedIndexScan(db, order, value1, bound1, value2, bound2, expectedOutputCardinality) {}

    /// First tuple
    uint64_t first();
    /// Next tuple
    uint64_t next();
};
//---------------------------------------------------------------------------
/// Implementation
class AggregatedIndexScan::ScanFilter2 : public AggregatedIndexScan {
public:
    /// The filter value
    uint64_t filter;

public:
    /// Constructor
    ScanFilter2(DBLayer& db, DBLayer::DataOrder order, Register* value1, bool bound1, Register* value2, bool bound2, double expectedOutputCardinality) : AggregatedIndexScan(db, order, value1, bound1, value2, bound2, expectedOutputCardinality) {}

    /// First tuple
    uint64_t first();
    /// Next tuple
    uint64_t next();
};
//---------------------------------------------------------------------------
/// Implementation
class AggregatedIndexScan::ScanPrefix1 : public AggregatedIndexScan {
private:
    /// The stop condition
    uint64_t stop1;

public:
    /// Constructor
    ScanPrefix1(DBLayer& db, DBLayer::DataOrder order, Register* value1, bool bound1, Register* value2, bool bound2, double expectedOutputCardinality) : AggregatedIndexScan(db, order, value1, bound1, value2, bound2, expectedOutputCardinality) {}

    /// First tuple
    uint64_t first();
    /// Next tuple
    uint64_t next();
};
//---------------------------------------------------------------------------
/// Implementation
class AggregatedIndexScan::ScanPrefix12 : public AggregatedIndexScan {
private:
    /// The stop condition
    uint64_t stop1, stop2;

public:
    /// Constructor
    ScanPrefix12(DBLayer& db, DBLayer::DataOrder order, Register* value1, bool bound1, Register* value2, bool bound2, double expectedOutputCardinality) : AggregatedIndexScan(db, order, value1, bound1, value2, bound2, expectedOutputCardinality) {}

    /// First tuple
    uint64_t first();
    /// Next tuple
    uint64_t next();
};
//---------------------------------------------------------------------------
AggregatedIndexScan::AggregatedHint::AggregatedHint(AggregatedIndexScan& scan)
    : scan(scan)
      // Constructor
{
}
//---------------------------------------------------------------------------
AggregatedIndexScan::AggregatedHint::~AggregatedHint()
// Destructor
{
}
//---------------------------------------------------------------------------
void AggregatedIndexScan::AggregatedHint::next(uint64_t& value1, uint64_t& value2)
// Scanning hint
{
    // First value
    if (scan.bound1) {
        uint64_t v = scan.value1->value;
        if ((~v) && (v > value1)) {
            value1 = v;
            value2 = 0;
        }
    }
    for (std::vector<Register*>::const_iterator iter = scan.merge1.begin(), limit = scan.merge1.end(); iter != limit; ++iter) {
        uint64_t v = (*iter)->value;
        if ((~v) && (v > value1)) {
            value1 = v;
            value2 = 0;
        }
    }
    if (scan.value1->domain) {
        uint64_t v = scan.value1->domain->nextCandidate(value1);
        if (v > value1) {
            value1 = v;
            value2 = 0;
        }
    }

    // Second value
    if (scan.bound2) {
        uint64_t v = scan.value2->value;
        if ((~v) && (v > value2)) {
            value2 = v;
        }
    }
    for (std::vector<Register*>::const_iterator iter = scan.merge2.begin(), limit = scan.merge2.end(); iter != limit; ++iter) {
        uint64_t v = (*iter)->value;
        if ((~v) && (v > value2)) {
            value2 = v;
        }
    }
    if (scan.value2->domain) {
        uint64_t v = scan.value2->domain->nextCandidate(value2);
        if (v > value2) {
            value2 = v;
        }
    }
}
//---------------------------------------------------------------------------
AggregatedIndexScan::AggregatedIndexScan(DBLayer& db, DBLayer::DataOrder order, Register* value1, bool bound1, Register* value2, bool bound2, double expectedOutputCardinality)
    : Operator(expectedOutputCardinality), value1(value1), value2(value2), bound1(bound1), bound2(bound2),/*facts(db.getAggregatedFacts(order)),*/order(order), hint(*this), scan(db.getScan(order, DBLayer::AGGR_SKIP_LAST, &hint))
      /*scan(disableSkipping?0:&hint),hint(*this)*/
      // Constructor
{
}
//---------------------------------------------------------------------------
AggregatedIndexScan::~AggregatedIndexScan()
// Destructor
{
}
//---------------------------------------------------------------------------
void AggregatedIndexScan::print(PlanPrinter& out)
// Print the operator tree. Debugging only.
{
    const char* scanType = "";
    switch (order) {
    case DBLayer::Order_No_Order_SPO:
        scanType = "NoOrder_SP";
        break;
    case DBLayer::Order_No_Order_SOP:
        scanType = "NoOrder_SO";
        break;
    case DBLayer::Order_No_Order_POS:
        scanType = "NoOrder_PO";
        break;
    case DBLayer::Order_No_Order_PSO:
        scanType = "NoOrder_PS";
        break;
    case DBLayer::Order_No_Order_OPS:
        scanType = "NoOrder_OP";
        break;
    case DBLayer::Order_No_Order_OSP:
        scanType = "NoOrder_OS";
        break;
    case DBLayer::Order_Subject_Predicate_Object:
        scanType = "SubjectPredicate";
        break;
    case DBLayer::Order_Subject_Object_Predicate:
        scanType = "SubjectObject";
        break;
    case DBLayer::Order_Object_Predicate_Subject:
        scanType = "ObjectPredicate";
        break;
    case DBLayer::Order_Object_Subject_Predicate:
        scanType = "ObjectSubject";
        break;
    case DBLayer::Order_Predicate_Subject_Object:
        scanType = "PredicateSubject";
        break;
    case DBLayer::Order_Predicate_Object_Subject:
        scanType = "PredicateObject";
        break;
    }
    out.beginOperator("AggregatedIndexScan", expectedOutputCardinality, observedOutputCardinality);
    out.addArgumentAnnotation(scanType);
    out.addScanAnnotation(value1, bound1);
    out.addScanAnnotation(value2, bound2);
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
void AggregatedIndexScan::addMergeHint(Register* reg1, Register* reg2)
// Add a merge join hint
{
    handleHints(reg1, reg2, value1, merge1);
    handleHints(reg1, reg2, value2, merge2);
}
//---------------------------------------------------------------------------
void AggregatedIndexScan::getAsyncInputCandidates(Scheduler& /*scheduler*/)
// Register parts of the tree that can be executed asynchronous
{
}
//---------------------------------------------------------------------------
AggregatedIndexScan* AggregatedIndexScan::create(DBLayer& db, DBLayer::DataOrder order, Register* subject, bool subjectBound, Register* predicate, bool predicateBound, Register* object, bool objectBound, double expectedOutputCardinality)
// Constructor
{
    // Setup the slot bindings
    Register* value1 = 0, *value2 = 0;
    bool bound1 = false, bound2 = false;
    switch (order) {
    case DBLayer::Order_No_Order_SPO:
    case DBLayer::Order_Subject_Predicate_Object:
        value1 = subject;
        value2 = predicate;
        bound1 = subjectBound;
        bound2 = predicateBound;
        assert(!object);
        break;
    case DBLayer::Order_No_Order_SOP:
    case DBLayer::Order_Subject_Object_Predicate:
        value1 = subject;
        value2 = object;
        bound1 = subjectBound;
        bound2 = objectBound;
        assert(!predicate);
        break;
    case DBLayer::Order_No_Order_OPS:
    case DBLayer::Order_Object_Predicate_Subject:
        value1 = object;
        value2 = predicate;
        bound1 = objectBound;
        bound2 = predicateBound;
        assert(!subject);
        break;
    case DBLayer::Order_No_Order_OSP:
    case DBLayer::Order_Object_Subject_Predicate:
        value1 = object;
        value2 = subject;
        bound1 = objectBound;
        bound2 = subjectBound;
        assert(!predicate);
        break;
    case DBLayer::Order_No_Order_PSO:
    case DBLayer::Order_Predicate_Subject_Object:
        value1 = predicate;
        value2 = subject;
        bound1 = predicateBound;
        bound2 = subjectBound;
        assert(!object);
        break;
    case DBLayer::Order_No_Order_POS:
    case DBLayer::Order_Predicate_Object_Subject:
        value1 = predicate;
        value2 = object;
        bound1 = predicateBound;
        bound2 = objectBound;
        assert(!subject);
        break;
    }

    // Construct the proper operator
    AggregatedIndexScan* result;
    if (!bound1) {
        if (bound2)
            result = new ScanFilter2(db, order, value1, bound1, value2, bound2, expectedOutputCardinality);
        else
            result = new Scan(db, order, value1, bound1, value2, bound2, expectedOutputCardinality);
    } else {
        if (!bound2)
            result = new ScanPrefix1(db, order, value1, bound1, value2, bound2, expectedOutputCardinality);
        else
            result = new ScanPrefix12(db, order, value1, bound1, value2, bound2, expectedOutputCardinality);
    }

    return result;
}
//---------------------------------------------------------------------------
uint64_t AggregatedIndexScan::Scan::first()
// Produce the first tuple
{
    observedOutputCardinality = 0;

    if (!scan->first())
        return false;
    value1->value = scan->getValue1();
    value2->value = scan->getValue2();

    uint64_t count = scan->getCount();
    observedOutputCardinality += count;
    return count;
}
//---------------------------------------------------------------------------
uint64_t AggregatedIndexScan::Scan::next()
// Produce the next tuple
{
    if (!scan->next())
        return false;
    value1->value = scan->getValue1();
    value2->value = scan->getValue2();

    uint64_t count = scan->getCount();
    observedOutputCardinality += count;
    return count;
}
//---------------------------------------------------------------------------
uint64_t AggregatedIndexScan::ScanFilter2::first()
// Produce the first tuple
{
    observedOutputCardinality = 0;
    filter = value2->value;

    if (!scan->first())
        return false;
    if (scan->getValue2() != filter)
        return next();
    value1->value = scan->getValue1();

    uint64_t count = scan->getCount();
    observedOutputCardinality += count;
    return count;
}
//---------------------------------------------------------------------------
uint64_t AggregatedIndexScan::ScanFilter2::next()
// Produce the next tuple
{
    while (true) {
        if (!scan->next())
            return false;
        if (scan->getValue2() != filter)
            continue;
        value1->value = scan->getValue1();

        uint64_t count = scan->getCount();
        observedOutputCardinality += count;
        return count;
    }
}
//---------------------------------------------------------------------------
uint64_t AggregatedIndexScan::ScanPrefix1::first()
// Produce the first tuple
{
    observedOutputCardinality = 0;
    stop1 = value1->value;
    if (!scan->first(stop1, true, 0, false))
        return false;
    if (scan->getValue1() > stop1)
        return false;
    value2->value = scan->getValue2();

    uint64_t count = scan->getCount();
    observedOutputCardinality += count;
    return count;
}
//---------------------------------------------------------------------------
uint64_t AggregatedIndexScan::ScanPrefix1::next()
// Produce the next tuple
{
    if (!scan->next())
        return false;
    if (scan->getValue1() > stop1)
        return false;
    value2->value = scan->getValue2();

    uint64_t count = scan->getCount();
    observedOutputCardinality += count;
    return count;
}
//---------------------------------------------------------------------------
uint64_t AggregatedIndexScan::ScanPrefix12::first()
// Produce the first tuple
{
    observedOutputCardinality = 0;
    stop1 = value1->value;
    stop2 = value2->value;
    if (!scan->first(stop1, true, stop2, true))
        return false;
    if ((scan->getValue1() > stop1) || ((scan->getValue1() == stop1) && (scan->getValue2() > stop2)))
        return false;

    uint64_t count = scan->getCount();
    observedOutputCardinality += count;
    return count;
}
//---------------------------------------------------------------------------
uint64_t AggregatedIndexScan::ScanPrefix12::next()
// Produce the next tuple
{
    if (!scan->next())
        return false;
    if ((scan->getValue1() > stop1) || ((scan->getValue1() == stop1) && (scan->getValue2() > stop2)))
        return false;

    uint64_t count = scan->getCount();
    observedOutputCardinality += count;
    return count;
}
//---------------------------------------------------------------------------
