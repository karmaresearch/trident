#ifndef H_cts_plangen_PlanGen
#define H_cts_plangen_PlanGen
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
#include <cts/plangen/Plan.hpp>
#include <cts/infra/BitSet.hpp>
#include <cts/infra/QueryGraph.hpp>
#include <dblayer.hpp>
//---------------------------------------------------------------------------
/// A plan generator that construct a physical plan from a query graph
class PlanGen {
    private:
        /// A subproblem
        struct Problem {
            /// The next problem in the DP table
            Problem* next;
            /// The known solutions to the problem
            Plan* plans;
            /// The relations involved in the problem
            BitSet relations;
        };
        /// A join description
        struct JoinDescription;
        /// The plans
        std::shared_ptr<PlanContainer> plans;
        /// The problems
        StructPool<Problem> problems;
        /// The database
        DBLayer* db;
        /// The current query
        const QueryGraph* fullQuery;

        PlanGen(const PlanGen&);
        void operator=(const PlanGen&);

        /// Add a plan to a subproblem
        void addPlan(Problem* problem, Plan* plan);
        /// Generate an index scan
        void buildIndexScan(const QueryGraph::SubQuery& query, DBLayer::DataOrder order, Problem* problem, uint64_t value1, uint64_t value1C, uint64_t value2, uint64_t value2C, uint64_t value3, uint64_t value3C);
        /// Generate an aggregated index scan
        void buildAggregatedIndexScan(const QueryGraph::SubQuery& query, DBLayer::DataOrder order, Problem* problem, uint64_t value1, uint64_t value1C, uint64_t value2, uint64_t value2C);
        /// Generate an fully aggregated index scan
        void buildFullyAggregatedIndexScan(const QueryGraph::SubQuery& query, DBLayer::DataOrder order, Problem* result, uint64_t value1, uint64_t value1C);
        /// Generate base table accesses
        Problem* buildAssignment(const QueryGraph::SubQuery& query, const QueryGraph::Node& node, uint64_t id);

        Problem* buildValue(const QueryGraph::SubQuery& query, const
                QueryGraph::ValuesNode& node, uint64_t id); Problem*
            buildScan(const QueryGraph::SubQuery& query, const
                    QueryGraph::Node& node, uint64_t id);
        /// Build the informaion about a join
        JoinDescription buildJoinInfo(const QueryGraph::SubQuery& query, const
                QueryGraph::Edge& edge);
        /// Generate an optional part
        Problem* buildOptional(const QueryGraph::SubQuery& query, const QueryGraph
                &entirePlan, uint64_t id, bool completeEstimate);
        /// Generate a union part
        Problem* buildUnion(const std::vector<QueryGraph::SubQuery>& query,
                const QueryGraph &entirePlan,
                uint64_t id, bool completeEstimate);
        /// Generate a table function access
        Problem* buildTableFunction(const QueryGraph::TableFunction& function,
                uint64_t id);

        //Add a filter
        Plan* buildFilters(const QueryGraph::SubQuery& query, Plan* plan, uint64_t value1, uint64_t value2, uint64_t value3);
        Plan *attachFiltersToPlan(QueryGraph::Filter *filter, Plan *plan);
        Plan *buildFilterPlan(const QueryGraph::Filter *filter);

    public:
        /// Constructor
        PlanGen();
        PlanGen(std::shared_ptr<PlanContainer> plans);
        /// Destructor
        ~PlanGen();

        void init(DBLayer* db, const QueryGraph& query);
        /// Translate a query into an operator tree
        Plan* translate(DBLayer& db, const QueryGraph& query, bool completeEstimate = true);
        /// Translate a query into an operator tree
        Plan* translate_int(const QueryGraph::SubQuery& query,
                const QueryGraph &entirePlan,
                bool completeEstimate);
};
//---------------------------------------------------------------------------
#endif
