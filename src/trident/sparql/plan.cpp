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


#include <trident/sparql/plan.h>
#include <trident/sparql/sparqloperators.h>

void TridentQueryPlan::create(Query &query, int typePlanning) {
    if (query.npatterns() == 1) {
        root = std::unique_ptr<SPARQLOperator>(new KBScan(q, query.getPatterns()[0]));
    } else {
        const std::vector<Pattern*> queryPatterns = query.getPatterns();
        if (typePlanning == SIMPLE) {
            std::vector<std::shared_ptr<SPARQLOperator>> patterns;

            for (int i = 0; i < query.npatterns(); ++i) {
                patterns.push_back(std::shared_ptr<SPARQLOperator>(
                                       new KBScan(q, queryPatterns[i])));
            }

            //Optimize them
            std::vector<int> optimalOrder = NestedJoinPlan::reorder(queryPatterns, patterns);

            //Reorder list
            std::vector<std::shared_ptr<SPARQLOperator>> listScans;
            for (int i = 0; i < optimalOrder.size(); ++i) {
                int idx = optimalOrder[i];
                listScans.push_back(patterns[idx]);
            }

            //Create joins
            std::vector<string> projections = query.getProjections();
            root = std::unique_ptr<SPARQLOperator>(new NestedMergeJoin(q, listScans, projections));
        } else { //No query optimization
            std::vector<std::shared_ptr<SPARQLOperator>> patterns;
            for (int i = 0; i < query.npatterns(); ++i) {
                patterns.push_back(std::shared_ptr<SPARQLOperator>(
                                       new KBScan(q, queryPatterns[i])));
            }

            //Create joins
            std::vector<string> projections = query.getProjections();
            root = std::unique_ptr<SPARQLOperator>(new NestedMergeJoin(q, patterns, projections));
        }
    }
}

TupleIterator *TridentQueryPlan::getIterator() {
    return root->getIterator();
}

void TridentQueryPlan::releaseIterator(TupleIterator *itr) {
    root->releaseIterator(itr);
}

void TridentQueryPlan::print() {
    if (root != NULL) {
        root->print(0);
    } else {
        LOG(DEBUG) << "NULL";
    }
}
