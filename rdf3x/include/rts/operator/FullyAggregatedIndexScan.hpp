#ifndef H_rts_operator_FullyAggregatedIndexScan
#define H_rts_operator_FullyAggregatedIndexScan
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
#include <rts/operator/Operator.hpp>
//#include "rts/database/Database.hpp"
//#include "rts/segment/FullyAggregatedFactsSegment.hpp"
#include <dblayer.hpp>

#include <vector>
//---------------------------------------------------------------------------
class Register;
//---------------------------------------------------------------------------
/// An index scan over the fully aggregated facts table
class FullyAggregatedIndexScan : public Operator
{
   private:
   /// Hints during scanning
   class FullyAggregatedHint : public DBLayer::Hint {
      private:
      /// The scan
      FullyAggregatedIndexScan& scan;

      public:
      /// Constructor
      FullyAggregatedHint(FullyAggregatedIndexScan& scan);
      /// Destructor
      ~FullyAggregatedHint();
      /// The next hint
      void next(uint64_t& value1);
   };
   friend class FullyAggregatedHint;

   /// The registers for the different parts of the triple
   Register* value1;
   /// The different boundings
   bool bound1;
   /// The facts segment
   //FullyAggregatedFactsSegment& facts;
   /// The data order
   DBLayer::DataOrder order;
   /// The scan
   std::unique_ptr<DBLayer::Scan> scan;
   /// The hinting mechanism
   FullyAggregatedHint hint;
   /// Merge hints
   std::vector<Register*> merge1;

   /// Constructor
   FullyAggregatedIndexScan(DBLayer& db,DBLayer::DataOrder order,Register* value1,bool bound1,double expectedOutputCardinality);

   // Implementations
   class Scan;
   class ScanPrefix1;

   public:
   /// Destructor
   ~FullyAggregatedIndexScan();

   /// Produce the first tuple
   virtual uint64_t first() = 0;
   /// Produce the next tuple
   virtual uint64_t next() = 0;

   /// Print the operator tree. Debugging only.
   void print(PlanPrinter& out);
   /// Add a merge join hint
   void addMergeHint(Register* reg1,Register* reg2);

   void setHashKeys(std::vector<uint64_t> *keys, int bitset) {
       hint.setKeys(keys, bitset);
   }

   /// Register parts of the tree that can be executed asynchronous
   void getAsyncInputCandidates(Scheduler& scheduler);

   /// Create a suitable operator
   static FullyAggregatedIndexScan* create(DBLayer& db,DBLayer::DataOrder order,Register* subjectRegister,bool subjectBound,Register* predicateRegister,bool predicateBound,Register* objectRegister,bool objectBound,double expectedOutputCardinality);
};
//---------------------------------------------------------------------------
#endif
