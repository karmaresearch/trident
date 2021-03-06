#include "rts/operator/SingletonScan.hpp"
#include "rts/operator/PlanPrinter.hpp"
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
SingletonScan::SingletonScan()
   : Operator(1)
   // Constructor
{
}
//---------------------------------------------------------------------------
SingletonScan::~SingletonScan()
   // Destructor
{
}
//---------------------------------------------------------------------------
uint64_t SingletonScan::first()
   // Produce the first tuple
{
   observedOutputCardinality=1;
   return 1;
}
//---------------------------------------------------------------------------
uint64_t SingletonScan::next()
   // Produce the next tuple
{
   return false;
}
//---------------------------------------------------------------------------
void SingletonScan::print(PlanPrinter& out)
   // Print the operator tree. Debugging only.
{
   out.beginOperator("SingletonScan",expectedOutputCardinality,observedOutputCardinality);
   out.endOperator();
}
//---------------------------------------------------------------------------
void SingletonScan::addMergeHint(Register* /*reg1*/,Register* /*reg2*/)
   // Add a merge join hint
{
}
//---------------------------------------------------------------------------
void SingletonScan::getAsyncInputCandidates(Scheduler& /*scheduler*/)
   // Register parts of the tree that can be executed asynchronous
{
}
//---------------------------------------------------------------------------
