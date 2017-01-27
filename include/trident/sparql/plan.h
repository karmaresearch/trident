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


#ifndef _PLAN_H
#define _PLAN_H

#include <trident/sparql/sparqloperators.h>
#include <trident/sparql/query.h>
#include <trident/iterators/tupleiterators.h>

//#include <cts/plangen/PlanGen.hpp>

#define SIMPLE 0
//#define BOTTOMUP 1
#define NONE 2

class Querier;
//class Plan;
class TridentQueryPlan {
private:

    Querier *q;

    std::map<string, uint64_t> mapVars1;
    std::map<uint64_t, string> mapVars2;

    std::shared_ptr<SPARQLOperator> root;

/*    std::shared_ptr<SPARQLOperator> translateJoin(Plan *plan);

    std::shared_ptr<SPARQLOperator> translateIndexScan(Plan *plan);

    std::shared_ptr<SPARQLOperator> translateProjection(Plan *plan);

    std::shared_ptr<SPARQLOperator> translateOperator(Plan *plan);*/

public:

    TridentQueryPlan(Querier *q) : q(q) {
    }

    void create(Query & query, int typePlanning);

    TupleIterator *getIterator();

    void releaseIterator(TupleIterator * itr);

    void print();
};

#endif
