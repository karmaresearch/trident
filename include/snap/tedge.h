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
        long GetSrcNId() const {
            return itr->getValue1();
        }

        /// Returns the destination of the edge. Since the graph is undirected, this is the node with a greater ID of the edge endpoints.
        long GetDstNId() const {
            return itr->getValue2();
        }

        ~TEdgeI() {
            if (q)
                q->releaseItr(itr);
        }
};

#endif
