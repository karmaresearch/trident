#ifndef H_cts_plangen_Costs
#define H_cts_plangen_Costs
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
/// A cost model used by the plan generator
class Costs
{
   public:
   /// Data type for costs
   typedef double cost_t;

   private:
   /// Costs for a seek in 1/10ms
   static const unsigned seekCosts = 95;
   /// Costs for a sequential page read in 1/10ms
   static const unsigned scanCosts = 17;
   /// Number of CPU operations per 1/10ms
   static const unsigned cpuSpeed = 100000;

   public:
   /// Costs for traversing a btree
   static cost_t seekBtree() { return 3*seekCosts; }
   /// Costs for scanning a number of pages
   static cost_t scan(unsigned pages) { return pages*scanCosts; }

   // According to the above, apparently rdf3x wants costs in 1/10 ms. --Ceriel

   /// Costs for a merge join
   // Almost for free (but note that the input must be sorted). --Ceriel
   static cost_t mergeJoin(double leftCard,double rightCard) { return (leftCard/cpuSpeed)+(rightCard/cpuSpeed); }
   /// Costs for a hash join
   // Apparently, a hash join has a fixed cost of 30 seconds??? And then also 10 usec for adding an entry,
   // and 1 usec for a lookup? I removed the fixed cost and reduced the cost of lookup a bit --Ceriel

   // Original version:
   static cost_t hashJoin(double leftCard,double rightCard) { return 300000+(leftCard/10)+(rightCard/100); }

   // static cost_t hashJoin(double leftCard,double rightCard) { return (leftCard/10)+(rightCard/200); }
   /// Costs for a filter
   static cost_t filter(double card) { return card/(cpuSpeed/3); }
   /// Costs for a table function
   static cost_t tableFunction(double leftCard) { return leftCard*10000.0; }
};
//---------------------------------------------------------------------------
#endif
