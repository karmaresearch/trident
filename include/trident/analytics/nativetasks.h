/*
 * Copyright 2017 Jacopo Urbani
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 **/


#ifndef _NATIVE_TASKS_H
#define _NATIVE_TASKS_H

#include <snap/readers.h>
#include <snap-core/Snap.h>

#include <vector>

class Querier;
class NativeTasks {
    private:
        static long GetCommon(const char *p1, const int len1, const long s1,
                const char *p2, int len2, const long s2);

    public:
        template <typename T> static int sgn(T val) {
            return (T(0) < val) - (val < T(0));
        }

        template<class K> //This function is taken from the original Snap library. Modified to make it faster for trident
            static long getShortPath(const K &Graph,
                    const long src,
                    const long dst) {

                TIntH NIdDistH(Graph->GetNodes());
                bool FollowOut = true;
                bool FollowIn = true;

                long MxDist = 0;
                TSnapQueue<long> Queue(Graph->GetNodes());
                Queue.Clr(false);
                Queue.Push(src);
                long v;

                while (!Queue.Empty()) {
                    const long NId = Queue.Top();
                    Queue.Pop();
                    const long Dist = NIdDistH.GetDat(NId);
                    if (Dist == MxDist) {
                        break;
                    }
                    const typename K::TObj::TNodeI NodeI = Graph->GetNI(NId);
                    if (FollowOut) {
                        for (v = 0; v < NodeI.GetOutDeg(); v++) {
                            const long DstNId = NodeI.GetOutNId(v);
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
                            const long DstNId = NodeI.GetInNId(v);
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
                return MxDist;
            }

        template <class PGraph>
            static void GetTriads(const PGraph& Graph,
                    std::vector<std::pair<long,long>>& NIdCOTriadV) {
                long NNodes;
                NNodes = Graph->GetNodes();
                NIdCOTriadV.resize(NNodes);
                for (long node = 0; node < NNodes; node++) {
                    typename PGraph::TObj::TNodeI NI = Graph->GetNI(node);
                    if (NI.GetDeg() < 2) {
                        NIdCOTriadV[node] = std::make_pair(0, 0); // zero triangles
                        continue;
                    }
                    // count connected neighbors
                    long OpenCnt1 = 0, CloseCnt1 = 0;

                    const auto reader1_out = NI.GetOutReader();
                    const auto begin1_out = NI.GetBeginOut();
                    const int len1_out = NI.GetOutLenField();
                    const auto size1_out = NI.GetOutDeg();

                    /*const auto reader1_in = NI.GetInReader();
                    const auto begin1_in = NI.GetBeginIn();
                    const int len1_in = NI.GetInLenField();
                    const auto size1_in = NI.GetInDeg();*/

                    for (long srcNbr = 0; srcNbr < size1_out; srcNbr++) {
                        const long nbr1 = reader1_out(begin1_out, srcNbr);
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
                std::vector<std::pair<long,long>> NIdCOTriadV;
                GetTriads<PGraph>(Graph, NIdCOTriadV);
                for (long i = 0; i < NIdCOTriadV.size(); i++) {
                    const long D = NIdCOTriadV[i].first+NIdCOTriadV[i].second;
                    const double CCf = D!=0 ? NIdCOTriadV[i].first / double(D) : 0.0;
                    NIdCCfH[i] = CCf;
                }
            }

};

#endif
