#ifndef _DIR_H
#define _DIR_H

#include <snap-core/Snap.h>

#include <trident/kb/kb.h>
#include <trident/kb/querier.h>
#include <trident/tree/treeitr.h>
#include <trident/utils/memoryfile.h>

#include <snap/tnode.h>
#include <snap/tedge.h>
#include <snap/readers.h>

/*****************************
 ****** DIRECTED GRAPH *******
 *****************************/

class Trident_TNGraph {
    private:
        static void getNewColumnPointer(int bytesEl,
                int totalBytes,
                int64_t (**output)(const char*, const int64_t));

        static bool getNodeInfo(TermCoordinates &coord,
                Querier *q,
                int64_t &indeg,
                int64_t &outdeg,
                const char*& osp,
                const char*& sop,
                SnapReaders::pReader& ospReader,
                SnapReaders::pReader& sopReader);

    public:

        typedef ::TNode TNode;
        typedef ::TEdgeI TEdgeI;
        typedef Trident_TNGraph TNet;
        typedef ::TNode TNodeI; //Essentially the same object

        static void getNewColumnPointer_name(int bytesEl, int totalBytes);

    private:
        KB *kb;
        Querier *q;

        std::unique_ptr<MemoryMappedFile> mf;
        const char *rawnodes;
        int64_t nnodes;

    public:
        Trident_TNGraph(KB *kb);

        Querier *getQuerier() {
            return q;
        }

        bool IsNode(int64_t id) {
            return id >= 0 && id < nnodes;
        }

        /// Returns the number of nodes in the graph.
        int64_t GetNodes() const {
            return nnodes;
        }

        /// Returns the number of edges in the graph.
        int64_t GetEdges() const {
            return kb->getSize();
        }

        /// Returns an iterator referring to the first node in the graph.
        TNodeI BegNI() const {
            return TNodeI(rawnodes);
        }

        /// Returns an iterator referring to the past-the-end node in the graph.
        TNodeI EndNI() const {
            return TNodeI(rawnodes + nnodes * 31);
        }

        TEdgeI BegEI() const {
            return TEdgeI(q->getPermuted(IDX_PSO, 0, -1, -1, false), q);
        }

        TEdgeI EndEI() const {
            return TEdgeI(NULL, NULL);
        }

        /// Returns an iterator referring to the node of ID NId in the graph.
        TNodeI GetNI(const int64_t& NId) const {
            return TNodeI(rawnodes + 31 * NId);
        }

        /// Gets a vector IDs of all nodes in the graph.
        void GetNIdV(std::vector<int64_t>& NIdV) const {
            NIdV.resize(GetNodes());
            for (int64_t i = 0; i < nnodes; ++i) {
                const int64_t v = (*(int64_t*)(rawnodes + 31 * i)) & 0XFFFFFFFFFFl;
                NIdV[i] = v;
            }
        }

        bool HasFlag(const TGraphFlag& Flag) const {
            switch (Flag) {
                case gfDirected:
                    return true;
                case gfMultiGraph:
                    return false;
                case gfBipart:
                    return false;
                default:
                    return false;
            }
        }

        ~Trident_TNGraph() {
            delete q;
        }


    public:
        TCRef CRef;
};

typedef TPt<Trident_TNGraph> PTrident_TNGraph;

#endif
