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

#include <snap-core/Snap.h>

class Querier;
class NativeTasks {
    public:
        template<class K> //This function is taken from the original Snap library
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
};

#endif
