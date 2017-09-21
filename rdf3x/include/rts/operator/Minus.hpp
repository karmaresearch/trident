#ifndef H_rts_operator_Minus
#define H_rts_operator_Minus

#include <rts/operator/Operator.hpp>
#include <cts/plangen/Plan.hpp>
#include <rts/runtime/Runtime.hpp>
#include <set>
#include <unordered_set>
#include <map>
#include <vector>

//---------------------------------------------------------------------------
/// A sort operator
class Minus : public Operator
{
    private:
        struct HashVectors {
            std::vector<uint64_t> &v1;
            std::vector<uint64_t> &v2;
            const unsigned size;
            HashVectors(std::vector<uint64_t> &v1,
                    std::vector<uint64_t> &v2, unsigned size) : v1(v1),
            v2(v2), size(size) {
            }
            std::size_t operator()(long const& idx) const {
                size_t hash = size;
                if (idx == -1) {
                    for(long i = 0; i < size; ++i) {
                        hash ^= v1[i] + 0x9e3779b9 + (hash << 6) + (hash >> 2);
                    }
                } else {
                    const long start = idx * size;
                    for(long i = 0; i < size; ++i) {
                        hash ^= v2[start + i] + 0x9e3779b9 + (hash << 6) + (hash >> 2);
                    }
                }
                return hash;
            }
        };

        struct EqualVectors {
            std::vector<uint64_t> &v1;
            std::vector<uint64_t> &v2;
            const unsigned size;

            EqualVectors(std::vector<uint64_t> &v1,
                    std::vector<uint64_t> &v2, unsigned size) : v1(v1),
            v2(v2), size(size) {
            }

            const bool operator()(const long &lhs, const long &rhs) const {
                if (lhs != rhs) {
                    if (lhs == -1) {
                        const long start = rhs * size;
                        for(unsigned i = 0; i < size; ++i) {
                            if (v1[i] != v2[start + i]) {
                                return false;
                            }
                        }
                        return true;
                    } else if (rhs == -1) {
                        const long start = lhs * size;
                        for(unsigned i = 0; i < size; ++i) {
                            if (v1[i] != v2[start + i]) {
                                return false;
                            }
                        }
                        return true;
                    }
                }
                return lhs == rhs;
            }
        };

        Operator *mainTree;
        Operator *minusTree;
        std::vector<std::pair<Register*, Register*>> regsToCheck;

        //Hashmap
        bool loaded;
        std::vector<uint64_t> row;
        std::vector<uint64_t> valuesHashMap;
        HashVectors hashF;
        EqualVectors equalF;
        std::unordered_set<long, HashVectors, EqualVectors> valuesToCheck;

    public:
        /// Constructor
        Minus(Operator* mainTree,
                Operator* minusTree,
                std::vector<std::pair<Register*, Register*>> regsToCheck,
                double expectedOutputCardinality);

        /// Destructor
        ~Minus();

        /// Produce the first tuple
        uint64_t first();
        /// Produce the next tuple
        uint64_t next();
        /// Print the operator tree. Debugging only.
        void print(PlanPrinter& out);
        /// Add a merge join hint
        void addMergeHint(Register* reg1,Register* reg2);
        /// Register parts of the tree that can be executed asynchronous
        void getAsyncInputCandidates(Scheduler& scheduler);
};
//---------------------------------------------------------------------------
#endif
