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


#ifndef _UNDIR_H
#define _UNDIR_H

#include <snap/directed.h>
#include <snap/tunode.h>
#include <snap/tedge.h>

/*****************************
 ****** UNDIRECTED GRAPH *****
 *****************************/

class Trident_UTNGraph {
    public:
        typedef ::TUNode TNode;
        typedef ::TEdgeI TEdgeI;
        typedef Trident_UTNGraph TNet;
        typedef ::TUNode TNodeI; //Essentially the same object

    private:
        KB *kb;
        Querier *q;

        bip::file_mapping mapping;
        bip::mapped_region mapped_rgn;
        const char *rawnodes;
        long nnodes;

    public:
        Trident_UTNGraph(KB *kb);

        Querier *getQuerier() {
            return q;
        }

        /// Returns the number of nodes in the graph.
        long GetNodes() const {
            return nnodes;
        }

        /// Returns the number of edges in the graph.
        long GetEdges() const {
            return kb->getSize();
        }

        /// Returns an iterator referring to the first node in the graph.
        Trident_UTNGraph::TNodeI BegNI() const {
            return TNodeI(rawnodes);
        }

        /// Returns an iterator referring to the past-the-end node in the graph.
        Trident_UTNGraph::TNodeI EndNI() const {
            return TNodeI(rawnodes + nnodes * 31);
        }

        Trident_UTNGraph::TEdgeI BegEI() const {
            return Trident_UTNGraph::TEdgeI(q->getPermuted(IDX_PSO, 0, -1, -1, false), q);
        }

        Trident_UTNGraph::TEdgeI EndEI() const {
            return Trident_UTNGraph::TEdgeI(NULL, NULL);
        }

        /// Returns an iterator referring to the node of ID NId in the graph.
        Trident_UTNGraph::TNodeI GetNI(const long& NId) const {
            return TNodeI(rawnodes + 31 * NId);
        }

        /// Gets a vector IDs of all nodes in the graph.
        void GetNIdV(std::vector<long>& NIdV) const {
            NIdV.resize(GetNodes());
            for (long i = 0; i < nnodes; ++i) {
                const long v = (*(long*)(rawnodes + 31 * i)) & 0XFFFFFFFFFFl;
                NIdV[i] = v;                
            }
        }

        bool HasFlag(const TGraphFlag& Flag) const {
            switch (Flag) {
                case gfDirected:
                    return false;
                case gfMultiGraph:
                    return false;
                case gfBipart:
                    return false;
                default:
                    return false;
            }
        }

        ~Trident_UTNGraph() {
            delete q;
        }


    public:
        TCRef CRef;
};

typedef TPt<Trident_UTNGraph> PTrident_UTNGraph;

#endif
