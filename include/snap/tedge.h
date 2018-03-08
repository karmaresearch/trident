#ifndef _T_EDGE_H
#define _T_EDGE_H

class TEdgeI {
    private:
        PairItr *itr;
        Querier *q;

    public:
        TEdgeI(PairItr *itr, Querier *q) : itr(itr), q(q) {
            if (itr && itr->hasNext())
                itr->next();
        }

        /// Increment iterator.
        TEdgeI& operator++ (int) {
            if (itr) {
                if (itr->hasNext()) {
                    itr->next();
                } else {
                    q->releaseItr(itr);
                    itr = NULL;
                    q = NULL;
                }
            }
            return *this;
        }

        bool operator < (const TEdgeI& EdgeI) const {
            if (itr && EdgeI.itr) {
                return itr->getValue1() < EdgeI.itr->getValue1() || (itr->getValue1() == EdgeI.itr->getValue1() && itr->getValue2() < EdgeI.itr->getValue2());
            } else {
                if (itr) {
                    return true;
                } else {
                    return false;
                }
            }
        }

        bool operator == (const TEdgeI& EdgeI) const {
            return itr->getValue1() == EdgeI.itr->getValue1() && itr->getValue2() == EdgeI.itr->getValue2();
        }

        /// Returns edge ID. Always returns -1 since only edges in multigraphs have explicit IDs.
        int GetId() const {
            return 0;
        }

        /// Returns the source of the edge. Since the graph is undirected, this is the node with a smaller ID of the edge endpoints.
        int64_t GetSrcNId() const {
            return itr->getValue1();
        }

        /// Returns the destination of the edge. Since the graph is undirected, this is the node with a greater ID of the edge endpoints.
        int64_t GetDstNId() const {
            return itr->getValue2();
        }

        ~TEdgeI() {
            if (q)
                q->releaseItr(itr);
        }
};

#endif
