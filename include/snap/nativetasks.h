#ifndef _NATIVE_TASKS_H
#define _NATIVE_TASKS_H

#include <snap/readers.h>
#include <snap-core/Snap.h>

#include <vector>
#include <map>
#include <random>

class Querier;
class NativeTasks {
    private:
        static int64_t GetCommon(const char *p1, const int len1, const int64_t s1,
                const char *p2, int len2, const int64_t s2);

    public:
        template <typename T> static int sgn(T val) {
            return (T(0) < val) - (val < T(0));
        }

        template<class K> //This function is taken from the original Snap library. Modified to make it faster for trident
            static int64_t getShortPath(const K &Graph,
                    const int64_t src,
                    const int64_t dst) {
                int limitDist = ~0;
                TUInt64H NIdDistH;
                NIdDistH.AddDat(src, 0);
                bool FollowOut = true;
                bool FollowIn = true;

                int64_t MxDist = 0;
                TSnapQueue<int64_t> Queue(Graph->GetNodes());
                Queue.Clr(false);
                Queue.Push(src);
                int64_t v;

                while (!Queue.Empty()) {
                    const int64_t NId = Queue.Top();
                    Queue.Pop();
                    const int64_t Dist = NIdDistH.GetDat(NId);
                    if (Dist == limitDist) {
                        break;
                    }
                    const typename K::TObj::TNodeI NodeI = Graph->GetNI(NId);
                    if (FollowOut) {
                        for (v = 0; v < NodeI.GetOutDeg(); v++) {
                            const int64_t DstNId = NodeI.GetOutNId(v);
                            if (!NIdDistH.IsKey(DstNId)) {
                                NIdDistH.AddDat(DstNId, Dist+1);
                                MxDist = TMath::Mx(MxDist, Dist+1);
                                if (DstNId == dst) {
                                    return MxDist;
                                }
                                Queue.Push(DstNId);
                            }
                        }
                    }

                    if (FollowIn) {
                        for (v = 0; v < NodeI.GetInDeg(); v++) {
                            const int64_t DstNId = NodeI.GetInNId(v);
                            if (!NIdDistH.IsKey(DstNId)) {
                                NIdDistH.AddDat(DstNId, Dist+1);
                                MxDist = TMath::Mx(MxDist, Dist+1);
                                if (DstNId == dst) {
                                    return MxDist;
                                }
                                Queue.Push(DstNId);
                            }
                        }
                    }
                }
                return limitDist;
            }

        template<class K>
            static std::vector<int64_t> getShortPath2(const K &Graph,
                    std::vector<std::pair<int64_t,int64_t>> &pairs) {
                std::vector<int64_t> distances;
                for (auto p : pairs) {
                    int64_t d = getShortPath(Graph, p.first, p.second);
                    distances.push_back(d);
                }
                return distances;
            }

        template <class PGraph>
            static void GetTriads(const PGraph& Graph,
                    std::vector<std::pair<int64_t,int64_t>>& NIdCOTriadV) {
                int64_t NNodes;
                NNodes = Graph->GetNodes();
                NIdCOTriadV.resize(NNodes);
                for (int64_t node = 0; node < NNodes; node++) {
                    typename PGraph::TObj::TNodeI NI = Graph->GetNI(node);
                    if (NI.GetDeg() < 2) {
                        NIdCOTriadV[node] = std::make_pair(0, 0); // zero triangles
                        continue;
                    }
                    // count connected neighbors
                    int64_t OpenCnt1 = 0, CloseCnt1 = 0;

                    const auto reader1_out = NI.GetOutReader();
                    const auto begin1_out = NI.GetBeginOut();
                    const int len1_out = NI.GetOutLenField();
                    const auto size1_out = NI.GetOutDeg();

                    /*const auto reader1_in = NI.GetInReader();
                      const auto begin1_in = NI.GetBeginIn();
                      const int len1_in = NI.GetInLenField();
                      const auto size1_in = NI.GetInDeg();*/

                    for (int64_t srcNbr = 0; srcNbr < size1_out; srcNbr++) {
                        const int64_t nbr1 = reader1_out(begin1_out, srcNbr);
                        typename PGraph::TObj::TNodeI NI2 = Graph->GetNI(nbr1);

                        const auto begin2_out = NI2.GetBeginOut();
                        const int len2_out = NI2.GetOutLenField();
                        const auto size2_out = NI2.GetOutDeg();
                        CloseCnt1 += GetCommon(begin1_out, len1_out, size1_out,
                                begin2_out, len2_out, size2_out);

                        const auto begin2_in = NI2.GetBeginIn();
                        const int len2_in = NI2.GetInLenField();
                        const auto size2_in = NI2.GetInDeg();

                        CloseCnt1 += GetCommon(begin1_out, len1_out, size1_out,
                                begin2_in, len2_in, size2_in);
                    }
                    CloseCnt1 /= 2;
                    OpenCnt1 = (NI.GetDeg()*(NI.GetDeg()-1))/2 - CloseCnt1;
                    NIdCOTriadV[node] = std::make_pair(CloseCnt1, OpenCnt1);
                }
            }

        template <class PGraph>
            static void clustcoef(const PGraph& Graph, std::vector<float>& NIdCCfH) {
                NIdCCfH.clear();
                NIdCCfH.reserve(Graph->GetNodes());
                std::vector<std::pair<int64_t,int64_t>> NIdCOTriadV;
                GetTriads<PGraph>(Graph, NIdCOTriadV);
                for (int64_t i = 0; i < NIdCOTriadV.size(); i++) {
                    const int64_t D = NIdCOTriadV[i].first+NIdCOTriadV[i].second;
                    const double CCf = D!=0 ? NIdCOTriadV[i].first / double(D) : 0.0;
                    NIdCCfH[i] = CCf;
                }
            }

        template <class PGraph>
            static int64 GetDiam(const PGraph& Graph,
                    const int& NTestNodes,
                    const bool& IsDir) {
                int64 FullDiam = -1;
                TIntFltH DistToCntH;
                TBreathFS<PGraph> BFS(Graph);
		std::vector<int64_t> NodeIdV;
		Graph->GetNIdV(NodeIdV); std::random_shuffle(NodeIdV.begin(), NodeIdV.end());
                int64_t minnodes = min((int64_t)NTestNodes, Graph->GetNodes());
                for (int tries = 0; tries < minnodes; tries++) {
                    const int NId = (int) NodeIdV[tries];
                    BFS.DoBfs(NId, true, ! IsDir, -1, TInt::Mx);
                    for (int64_t i = 0; i < BFS.NIdDistH.Len(); i++) {
                        DistToCntH.AddDat(BFS.NIdDistH[i]) += 1; }
                }
                TIntFltKdV DistNbrsPdfV;
                double SumPathL=0, PathCnt=0;
                for (int64_t i = 0; i < DistToCntH.Len(); i++) {
                    DistNbrsPdfV.Add(TIntFltKd(DistToCntH.GetKey(i), DistToCntH[i]));
                    SumPathL += DistToCntH.GetKey(i) * DistToCntH[i];
                    PathCnt += DistToCntH[i];
                }
                DistNbrsPdfV.Sort();
                FullDiam = DistNbrsPdfV.Last().Key;
                return FullDiam;
            }

        template<typename PGraph>
            static double GetMod(const PGraph& Graph,
                    const std::vector<int64_t>& NIdV,
                    int64_t GEdges) {
                if (GEdges == -1) { GEdges = Graph->GetEdges(); }
                double EdgesIn = 0.0, EEdgesIn = 0.0;
                TUInt64Set input_s(NIdV.size());
                for(const auto el : NIdV)
                    input_s.AddKey(el);

                for (int64_t e1 = 0; e1 < NIdV.size(); e1++) {
                    typename PGraph::TObj::TNodeI NI = Graph->GetNI(NIdV[e1]);
                    EEdgesIn += NI.GetOutDeg();
                    for (int64_t i = 0; i < NI.GetOutDeg(); i++) {
                        if (input_s.IsKey(NI.GetOutNId(i))) { EdgesIn += 1; }
                    }
                }
                EEdgesIn = EEdgesIn*EEdgesIn / (2.0*GEdges);
                if ((EdgesIn - EEdgesIn) == 0) { return 0; }
                else { return (EdgesIn - EEdgesIn) / (2.0*GEdges); } // modularity
            }
};

#endif
