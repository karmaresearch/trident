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


//Trident layer

#include <trident/kb/querier.h>
#include <trident/kb/consts.h>
#include <trident/model/table.h>
#include <layers/TridentLayer.hpp>
#include <infra/util/Type.hpp>
#include <string>
#include <map>
#include <cmath>
#include <limits>
#include <inttypes.h>
#include <set>

bool TridentLayer::lookup(const std::string& text,
        ::Type::ID type,
        unsigned subType,
        uint64_t& id) {
    nTerm longid;
    bool resp;
    if (type == ::Type::ID::URI) {
        string uri = "<" + text + ">";
        resp = dict->getNumber(uri.c_str(), uri.size(), &longid);
    } else {
        resp = dict->getNumber(text.c_str(), text.size(), &longid);
        if (! resp) {
            string tp = "";
            switch(type) {
                case ::Type::ID::String:
                    tp = "http://www.w3.org/2001/XMLSchema#string";
                    break;
                case ::Type::ID::Integer:
                    tp = "http://www.w3.org/2001/XMLSchema#integer";
                    break;
                case ::Type::ID::Decimal:
                    tp = "http://www.w3.org/2001/XMLSchema#decimal";
                    break;
                case ::Type::ID::Double:
                    tp = "http://www.w3.org/2001/XMLSchema#double";
                    break;
                case ::Type::ID::Boolean:
                    tp = "http://www.w3.org/2001/XMLSchema#boolean";
                    break;
                case ::Type::ID::Date:
                    tp = "http://www.w3.org/2001/XMLSchema#date";
                    break;
                default:
                    // TODO?
                    break;
            }
            string txt = "\"" + text + "\"^^<" + tp + ">";
            resp = dict->getNumber(txt.c_str(), txt.size(), &longid);
        }
    }
    if (resp) {
        id = (uint64_t) longid;
    }
    return resp;
}

bool TridentLayer::lookupById(uint64_t id,
        //char *output,
        //size_t &length,
        const char*& start,
        const char*& stop,
        ::Type::ID& type,
        unsigned& subType) {
    int size;
    bool resp;
    resp = dict->getText(id, supportBuffer.get(), size);
    //Subtypes are not supported. Set default value to zero.
    subType = 0;

    if (resp) {
        if (supportBuffer[0] == '<') {
            start = supportBuffer.get() + 1;
            stop = start + size - 2;
            type = ::Type::ID::URI;
        } else {
            start = supportBuffer.get();
            stop = start + size;
            type = ::Type::ID::Literal;
        }
    }
    return resp;
}

bool TridentLayer::lookupById(uint64_t id,
        char *output,
        size_t &length,
        ::Type::ID& type,
        unsigned& subType) {
    bool resp;
    int size;
    resp = dict->getText(id, output, size);
    length = size;
    //Subtypes are not supported. Set default value to zero.
    subType = 0;
    if (resp) {
        if (supportBuffer[0] == '<') {
            output = output + 1;
            length -= 2;
            type = ::Type::ID::URI;
        } else {
            type = ::Type::ID::Literal;
        }
    }
    return resp;
}

uint64_t TridentLayer::getNextId() {
    return kb.getNextID();
}

double TridentLayer::getScanCost(DBLayer::DataOrder order,
        uint64_t value1,
        uint64_t value1C,
        uint64_t value2,
        uint64_t value2C,
        uint64_t value3,
        uint64_t value3C) {

    if (nindices == 3 && (order == DBLayer::DataOrder::Order_No_Order_PSO ||
                order == DBLayer::DataOrder::Order_No_Order_OSP ||
                order == DBLayer::DataOrder::Order_No_Order_SOP ||
                order == DBLayer::DataOrder::Order_Predicate_Subject_Object ||
                order == DBLayer::DataOrder::Order_Object_Subject_Predicate ||
                order == DBLayer::DataOrder::Order_Subject_Object_Predicate)) {
        return std::numeric_limits<double>::max();
    }

    //Check whether the first term in the permutation is a constant.
    int64_t v1 = -1;
    int64_t v2 = -1;
    int64_t v3 = -1;
    if (value1 == UINT64_MAX) {
        v1 = value1C;
    }
    if (value2 == UINT64_MAX) {
        v2 = value2C;
    }
    if (value3 == UINT64_MAX) {
        v3 = value3C;
    }

    if (value1 != UINT64_MAX) { //It's a variable
        if (value2 == UINT64_MAX || value3 == UINT64_MAX) {
            return std::numeric_limits<double>::max(); //no constants allowed after variable
        }
    } else if (value2 != UINT64_MAX) {
        if (value3 == UINT64_MAX)
            return std::numeric_limits<double>::max(); //no constants allowed after variable
    }

    int idx;
    int64_t cost = 0;
    uint64_t reversedCard, aggrEls;
    switch (order) {
        case DBLayer::DataOrder::Order_No_Order_SPO:
        case DBLayer::DataOrder::Order_Subject_Predicate_Object:
            idx = IDX_SPO;
            cost = q->estCardOnIndex(idx, v1, v2, v3);
            //native indexing
            break;
        case DBLayer::DataOrder::Order_No_Order_SOP:
        case DBLayer::DataOrder::Order_Subject_Object_Predicate:
            idx = IDX_SOP;
            cost = q->estCardOnIndex(idx, v1, v3, v2);
            //might be reverse
            reversedCard = q->isReverse(idx, v1, v3, v2);
            if (reversedCard > 0) {
                //The relations must be pre-sorted. Add O(nlogn) cost
                cost += reversedCard * log(reversedCard);
            }
            break;
        case DBLayer::DataOrder::Order_No_Order_PSO:
        case DBLayer::DataOrder::Order_Predicate_Subject_Object:
            idx = IDX_PSO;
            cost = q->estCardOnIndex(idx, v2, v1, v3);
            //might be aggregating
            aggrEls = q->isAggregated(idx, v2, v1, v3);
            if (aggrEls > 0) {
                cost += aggrEls * 2;
            }
            break;
        case DBLayer::DataOrder::Order_No_Order_POS:
        case DBLayer::DataOrder::Order_Predicate_Object_Subject:
            idx = IDX_POS;
            cost = q->estCardOnIndex(idx, v2, v3, v1);
            //might be aggregating
            aggrEls = q->isAggregated(idx, v2, v3, v1);
            if (aggrEls > 0) {
                cost += aggrEls * 2;
            }
            break;
        case DBLayer::DataOrder::Order_No_Order_OSP:
        case DBLayer::DataOrder::Order_Object_Subject_Predicate:
            idx = IDX_OSP;
            cost = q->estCardOnIndex(idx, v3, v1, v2);
            //might be reverse
            reversedCard = q->isReverse(idx, v3, v1, v2);
            if (reversedCard > 0) {
                //The relations must be pre-sorted. Add O(nlogn) cost
                cost += reversedCard * log(reversedCard);
            }

            break;
        case DBLayer::DataOrder::Order_No_Order_OPS:
        case DBLayer::DataOrder::Order_Object_Predicate_Subject:
            idx = IDX_OPS;
            cost = q->estCardOnIndex(idx, v3, v2, v1);
            //native indexing
            break;
    }
    return cost;
}

double TridentLayer::getScanCost(DBLayer::DataOrder order,
        uint64_t value1,
        uint64_t value1C,
        uint64_t value2,
        uint64_t value2C) {
    if (nindices == 3 && (order == DBLayer::DataOrder::Order_No_Order_PSO ||
                order == DBLayer::DataOrder::Order_No_Order_OSP ||
                order == DBLayer::DataOrder::Order_No_Order_SOP ||
                order == DBLayer::DataOrder::Order_Predicate_Subject_Object ||
                order == DBLayer::DataOrder::Order_Object_Subject_Predicate ||
                order == DBLayer::DataOrder::Order_Subject_Object_Predicate)) {
        return std::numeric_limits<double>::max();
    }

    int64_t v1 = -1;
    int64_t v2 = -1;
    if (value1C != UINT64_MAX) {
        v1 = value1C;
    }
    if (value2C != UINT64_MAX) {
        v2 = value2C;
    }

    if (v1 != -1 && v2 != -1)
        return 1;

    int64_t cost = 0;
    int idx;
    switch (order) {
        case DBLayer::DataOrder::Order_No_Order_SPO:
        case DBLayer::DataOrder::Order_Subject_Predicate_Object:
            idx = IDX_SPO;
            cost = q->getCardOnIndex(idx, v1, -1, -1, true);
            break;
        case DBLayer::DataOrder::Order_No_Order_SOP:
        case DBLayer::DataOrder::Order_Subject_Object_Predicate:
            idx = IDX_SOP;
            cost = q->getCardOnIndex(idx, v1, -1, -1, true);
            break;
        case DBLayer::DataOrder::Order_No_Order_PSO:
        case DBLayer::DataOrder::Order_Predicate_Subject_Object:
            idx = IDX_PSO;
            cost = q->getCardOnIndex(idx, -1, v1, -1, true);
            break;
        case DBLayer::DataOrder::Order_No_Order_POS:
        case DBLayer::DataOrder::Order_Predicate_Object_Subject:
            idx = IDX_POS;
            cost = q->getCardOnIndex(idx, -1, v1, -1, true);
            break;
        case DBLayer::DataOrder::Order_No_Order_OSP:
        case DBLayer::DataOrder::Order_Object_Subject_Predicate:
            idx = IDX_OSP;
            cost = q->getCardOnIndex(idx, -1, v1, -1, true);
            break;
        case DBLayer::DataOrder::Order_No_Order_OPS:
        case DBLayer::DataOrder::Order_Object_Predicate_Subject:
            idx = IDX_OPS;
            cost = q->getCardOnIndex(idx, -1, -1, v1, true);
            break;
    }
    LOG(DEBUGL) << "Get cost for " << v1 << " " << v2 << " on index " << order << " is " << cost;
    return cost;
}

double TridentLayer::getScanCost(DBLayer::DataOrder order,
        uint64_t value1,
        uint64_t value1C) {
    if (nindices == 3 && (order == DBLayer::DataOrder::Order_No_Order_PSO ||
                order == DBLayer::DataOrder::Order_No_Order_OSP ||
                order == DBLayer::DataOrder::Order_No_Order_SOP ||
                order == DBLayer::DataOrder::Order_Predicate_Subject_Object ||
                order == DBLayer::DataOrder::Order_Object_Subject_Predicate ||
                order == DBLayer::DataOrder::Order_Subject_Object_Predicate)) {
        return std::numeric_limits<double>::max();
    }

    if (value1C == UINT64_MAX)
        return kb.getNTerms();
    else
        return 1; //It's the cost of a lookup on the tree
}

bool same(TupleTable *t, const size_t idx, const uint8_t *j, const uint8_t sj) {
    const uint64_t *r1 = t->getRow(idx - 1);
    const uint64_t *r2 = t->getRow(idx);
    for (int i = 0; i < sj; ++i) {
        if (r1[i] != r2[i])
            return false;
    }
    return true;
}

int same(const uint64_t *r1, const uint64_t *r2, const uint8_t *pos1,
        const uint8_t *pos2, const uint8_t s) {
    for (int i = 0; i < s; ++i)
        if (r1[pos1[i]] != r2[pos2[i]])
            return r1[pos1[i]] - r2[pos2[i]];
    return 0;
}

double TridentLayer::bifocalSampling_SparseAny(bool valueL1,
        uint64_t value1CL,
        bool value2L,
        uint64_t value2CL,
        bool value3L,
        uint64_t value3CL,
        bool value1R,
        uint64_t value1CR,
        bool value2R,
        uint64_t value2CR,
        bool value3R,
        uint64_t value3CR,
        const int64_t card1,
        const int64_t card2) {
    //The right relation will be sampled
    const double delta2 = log(card2);
    const double s2 = max(1.0, sqrt(card2) + delta2);
    double samplePerc2 = 1.0;
    std::shared_ptr<TupleTable> t2 = sampleQuery(value1R, value1CR, value2R,
            value2CR, value3R, value3CR, (int64_t)s2,
            samplePerc2);

    const double delta1 = log(card1);
    const double s1 = max(1.0, (sqrt(card1) + delta1) * delta1);
    double samplePerc1 = 1.0;
    std::shared_ptr<TupleTable> t1 = sampleQuery(valueL1, value1CL, value2L,
            value2CL, value3L, value3CL, (int64_t)s1,
            samplePerc1);

    //Remove from t2 all elements found in t1. This is to simulate a removal
    //of all dense keys from the left relation (see bifocal paper for details).
    std::vector<std::pair<uint8_t, uint8_t>> posJoins = t1->getPosJoins(t2.get());
    assert(posJoins.size() < 3);
    std::set<uint64_t> keys1;
    std::set<std::pair<uint64_t, uint64_t>> keys2;
    for (size_t i = 0; i < t1->getNRows(); ++i) {
        const uint64_t *row1 = t1->getRow(i);
        if (posJoins.size() == 1) {
            const uint64_t k = row1[posJoins[0].first];
            if (!keys1.count(k)) {
                keys1.insert(k);
            }
        } else {
            const uint64_t k1 = row1[posJoins[0].first];
            const uint64_t k2 = row1[posJoins[1].first];
            std::pair<uint64_t, uint64_t> k = make_pair(k1, k2);
            if (!keys2.count(k)) {
                keys2.insert(k);
            }
        }
    }

    std::vector<uint64_t> acceptableKeys;
    uint64_t nAcceptableKeys = 0;
    //Copy only the rows whose keys are not contained in the sets
    const uint8_t njoins = posJoins.size();
    for (size_t i = 0; i < t2->getNRows(); ++i) {
        const uint64_t *row = t2->getRow(i);
        if (njoins == 1) {
            const uint64_t k = row[posJoins[0].second];
            if (!keys1.count(k))  {
                nAcceptableKeys++;
                if (nAcceptableKeys < 100)
                    acceptableKeys.push_back(k);
            }
        } else {
            if (njoins != 2)
                throw 10;
            const std::pair<uint64_t, uint64_t> k = make_pair(
                    row[posJoins[0].second], row[posJoins[1].second]);
            if (!keys2.count(k)) {
                nAcceptableKeys++;
                if (nAcceptableKeys < 100) {
                    acceptableKeys.push_back(k.first);
                    acceptableKeys.push_back(k.second);
                }
            }
        }
    }

    //For each tuple in t2, get number of triples in the left relation.
    std::vector<uint8_t> posToFilter1;
    for (const auto &el : posJoins)
        posToFilter1.push_back(el.first);
    if (valueL1) {
        for (int i = 0; i < posToFilter1.size(); ++i)
            posToFilter1[i]++;
    }
    if (value2L) {
        for (int i = 0; i < posToFilter1.size(); ++i)
            if (posToFilter1[i] >= 1)
                posToFilter1[i]++;
    }

    int64_t subj1, p1, o1;
    if (!valueL1) {
        subj1 = -1;
    } else {
        subj1 = value1CL;
    }
    if (!value2L) {
        p1 = -1;
    } else {
        p1 = value2CL;
    }
    if (!value3L) {
        o1 = -1;
    } else {
        o1 = value3CL;
    }
    int64_t sizeOutput = 0;
    if (acceptableKeys.size() > 0) {
        sizeOutput = getSizeOutput(subj1, p1, o1, &posToFilter1,
                &acceptableKeys);
        if (nAcceptableKeys > 100) {
            sizeOutput = sizeOutput * (double)nAcceptableKeys / 100;
        }
    }
    const double output = card2 / s2 * sizeOutput;
    return output;
}

double TridentLayer::bifocalSampling_DenseDense(bool valueL1,
        uint64_t value1CL,
        bool value2L,
        uint64_t value2CL,
        bool value3L,
        uint64_t value3CL,
        bool value1R,
        uint64_t value1CR,
        bool value2R,
        uint64_t value2CR,
        bool value3R,
        uint64_t value3CR,
        const int64_t card1,
        const int64_t card2) {

    //SampleSize1
    const double delta1 = log(card1);
    const double s1 = max(1.0, (sqrt(card1) + delta1) * delta1);
    double samplePerc1 = 1.0;
    std::shared_ptr<TupleTable> t1 = sampleQuery(valueL1, value1CL, value2L,
            value2CL, value3L, value3CL, (int64_t)s1,
            samplePerc1);
    const double delta2 = log(card2);
    const double s2 = max(1.0, (sqrt(card2) + delta2) * delta2);
    double samplePerc2 = 1.0;
    std::shared_ptr<TupleTable> t2 = sampleQuery(value1R, value1CR, value2R,
            value2CR, value3R, value3CR, (int64_t)s2,
            samplePerc2);

    //Sort the two relations by join fields
    std::vector<std::pair<uint8_t, uint8_t>> joinFields =
        t1->getPosJoins(t2.get());
    std::vector<uint8_t> joins1;
    std::vector<uint8_t> joins2;
    for (auto &el : joinFields) {
        joins1.push_back(el.first);
        joins2.push_back(el.second);
    }
    std::unique_ptr<TupleTable> st1(t1->sortBy(joins1));
    std::unique_ptr<TupleTable> st2(t2->sortBy(joins2));

    double output = 0.0;
    size_t idx1 = 0;
    size_t idx2 = 0;
    size_t count1 = 0;
    size_t count2 = 0;
    const uint8_t *j1 = &joins1[0];
    const uint8_t sj1 = joins1.size();
    const uint8_t *j2 = &joins2[0];
    const uint8_t sj2 = joins2.size();
    while (idx1 < st1->getNRows() && idx2 < st2->getNRows()) {
        if (count1 == 0) {
            do {
                count1++;
                idx1++;
            } while (idx1 < st1->getNRows() && same(st1.get(), idx1, j1, sj1));
        }
        if (count2 == 0) {
            do {
                count2++;
                idx2++;
            } while (idx2 < st2->getNRows() && same(st2.get(), idx2, j2, sj2));
        }

        if (count1 < delta1 && count2 < delta2) {
            count1 = 0;
            count2 = 0;
        } else {
            const int resp = same(st1->getRow(idx1 - 1),
                    st2->getRow(idx2 - 1),
                    j1, j2, sj1);
            if (count1 >= delta1 && count2 >= delta2) {
                //Check the join
                if (resp == 0) {
                    output += count1 * count2;
                    count1 = 0;
                    count2 = 0;
                } else if (resp < 0) {
                    count1 = 0;
                } else {
                    count2 = 0;
                }
            } else if (count1 >= delta1) {
                //Reset the count only if the key is greater than the previous
                if (resp < 0)
                    count1 = 0;
                count2 = 0;
            } else if (count2 >= delta2) {
                //Reset the count only if the key is greater than the previous
                if (resp > 0)
                    count2 = 0;
                count1 = 0;
            } else {
                throw 10;
            }
        }
    }
    output = (card1 / s1) * (card2 / s2) * output;
    return output;
}

double TridentLayer::bifocalSampling(bool valueL1,
        uint64_t value1CL,
        bool value2L,
        uint64_t value2CL,
        bool value3L,
        uint64_t value3CL,
        bool value1R,
        uint64_t value1CR,
        bool value2R,
        uint64_t value2CR,
        bool value3R,
        uint64_t value3CR,
        const int64_t card1,
        const int64_t card2) {

    double output = bifocalSampling_DenseDense(valueL1, value1CL, value2L,
            value2CL, value3L, value3CL, value1R, value1CR, value2R,
            value2CR, value3R, value3CR, card1, card2);


    output += bifocalSampling_SparseAny(valueL1, value1CL, value2L,
            value2CL, value3L, value3CL,
            value1R, value1CR, value2R,
            value2CR, value3R, value3CR,
            card1, card2);

    output += bifocalSampling_SparseAny(value1R, value1CR, value2R,
            value2CR, value3R, value3CR,
            valueL1, value1CL, value2L,
            value2CL, value3L, value3CL,
            card2, card1);

    return output / (card1 * card2);
}

double TridentLayer::getJoinSelectivity(bool valueL1,
        uint64_t value1CL,
        bool value2L,
        uint64_t value2CL,
        bool value3L,
        uint64_t value3CL,
        bool value1R,
        uint64_t value1CR,
        bool value2R,
        uint64_t value2CR,
        bool value3R,
        uint64_t value3CR) {

    LOG(DEBUGL) << "Exec join selectivity: " << valueL1 << " " << value1CL << " " << value2L << " " << value2CL << " " << value3L << " " <<
        value3CL << "-" << value1R << " " <<  value1CR << " " << value2R << " " << value2CR << " " << value3R << " " << value3CR;

    std::chrono::system_clock::time_point start = std::chrono::system_clock::now();

    std::shared_ptr<TupleTable> t1;
    const int64_t card1 = getCardinality(!valueL1 ? UINT64_MAX : value1CL,
            !value2L ? UINT64_MAX : value2CL,
            !value3L ? UINT64_MAX : value3CL);
    const bool sampleT1 = card1 > SMALLREL;

    std::shared_ptr<TupleTable> t2;
    const int64_t card2 = getCardinality(!value1R ? UINT64_MAX : value1CR,
            !value2R ? UINT64_MAX : value2CR,
            !value3R ? UINT64_MAX : value3CR);
    const bool sampleT2 = card2 > SMALLREL;

    if (!bifSampl) {
        //Bifocal sampling is disabled. I perform a simple estimate.
        return card1 * card2;
    }

    if (card1 == 0 || card2 == 0)
        return 0;

    if (sampleT1 && sampleT2) {
        //Do bifocal sampling
        double cost = bifocalSampling(valueL1,
                value1CL,
                value2L,
                value2CL,
                value3L,
                value3CL,
                value1R,
                value1CR,
                value2R,
                value2CR,
                value3R,
                value3CR,
                card1,
                card2);
        std::chrono::duration<double> dur = std::chrono::system_clock::now() - start;
        LOG(DEBUGL) << "Time bifocal sampling between: " << valueL1 << " " << value1CL << " " << value2L << " " << value2CL << " " << value3L << " " <<
            value3CL << "-" << value1R << " " <<  value1CR << " " << value2R << " " << value2CR << " " << value3R << " " << value3CR << ": " << dur.count() * 1000 << "retval=" << cost;
        return cost;
    } else {
        //One of the two tables is small enough. Do the computation
        //LOG(DEBUGL) << "Exact estimation " << card1 << " " << card2;
        if (!sampleT1 && !sampleT2) {
            t1 = query(valueL1, value1CL, value2L, value2CL,
                    value3L, value3CL);
            t2 = query(value1R, value1CR, value2R, value2CR,
                    value3R, value3CR);
            TupleTable::JoinHitStats estimates = t1->joinHitRates(t2.get());
            LOG(DEBUGL) << "Exact cost between " << value1CL << " "
                << value2CL << " " << value3CL << " "
                << value1CR << " " << value2CR << " "
                << value3CR << " is " << estimates.ratio;
            return estimates.ratio;
        } else if (sampleT1) {
            double samplePerc1 = 1.0;
            t1 = sampleQuery(valueL1, value1CL, value2L, value2CL,
                    value3L, value3CL, LIMIT_SAMPLE, samplePerc1);
            t2 = query(value1R, value1CR, value2R, value2CR,
                    value3R, value3CR);
            TupleTable::JoinHitStats estimates = t1->joinHitRates(t2.get());
            if (estimates.ratio == 0) {
                //Could be that the sample is too small. Assume all the exact values match
                estimates.ratio = min((size_t)(t1->getNRows() / (samplePerc1 * kb.getSampleRate())),
                        t2->getNRows()) /
                    (t1->getNRows() / (samplePerc1 * kb.getSampleRate()) * t2->getNRows());
            } else {
                estimates.ratio = estimates.ratio / (samplePerc1 * kb.getSampleRate());
            }
            LOG(DEBUGL) << "Exact cost between " << value1CL
                << " " << value2CL << " " << value3CL << " " << value1CR
                << " " << value2CR << " " << value3CR << " is "
                << estimates.ratio;
            return estimates.ratio;
        } else { //sampleT2 = true
            t1 = query(valueL1, value1CL, value2L, value2CL,
                    value3L, value3CL);
            double samplePerc2 = 1.0;
            t2 = sampleQuery(value1R, value1CR, value2R, value2CR,
                    value3R, value3CR, LIMIT_SAMPLE, samplePerc2);
            TupleTable::JoinHitStats estimates = t1->joinHitRates(t2.get());
            if (estimates.ratio == 0) {
                //Could be that the sample is too small. Assume all the exact values match
                estimates.ratio = min(t1->getNRows(), (size_t)(t2->getNRows() / (samplePerc2 * kb.getSampleRate()))) /
                    (t1->getNRows() / (samplePerc2 * kb.getSampleRate()) * t2->getNRows());
            } else {
                estimates.ratio = estimates.ratio / (kb.getSampleRate() * samplePerc2);
            }
            LOG(DEBUGL) << "Exact cost between " << value1CL
                << " " << value2CL << " " << value3CL << " " << value1CR
                << " " << value2CR << " " << value3CR << " is "
                << estimates.ratio;
            return estimates.ratio;
        }
    }
}

uint64_t TridentLayer::getCardinality(uint64_t c1,
        uint64_t c2,
        uint64_t c3) {
    int64_t v1 = -1;
    if (c1 != UINT64_MAX) {
        v1 = c1;
    }
    int64_t v2 = -1;
    if (c2 != UINT64_MAX) {
        v2 = c2;
    }
    int64_t v3 = -1;
    if (c3 != UINT64_MAX) {
        v3 = c3;
    }
    int64_t card = q->getCard(v1, v2, v3);
    return card;
}

uint64_t TridentLayer::getCardinality() {
    return kb.getSize();
}

std::unique_ptr<DBLayer::Scan> TridentLayer::getScan(
        const DBLayer::DataOrder order,
        const DBLayer::Aggr_t a,
        Hint * hint) {

    //Must convert the DataOrder in a IDX_flag
    int perm = 0;
    switch (order) {
        case Order_No_Order_SOP:
        case Order_Subject_Object_Predicate:
            perm = IDX_SOP;
            break;
        case Order_No_Order_SPO:
        case Order_Subject_Predicate_Object:
            perm = IDX_SPO;
            break;
        case Order_No_Order_POS:
        case Order_Predicate_Object_Subject:
            perm = IDX_POS;
            break;
        case Order_No_Order_PSO:
        case Order_Predicate_Subject_Object:
            perm = IDX_PSO;
            break;
        case Order_No_Order_OPS:
        case Order_Object_Predicate_Subject:
            perm = IDX_OPS;
            break;
        case Order_No_Order_OSP:
        case Order_Object_Subject_Predicate:
            perm = IDX_OSP;
            break;
    }
    std::unique_ptr<DBLayer::Scan> s(new TridentScan(perm, a, q.get(), hint));
    return s;
}

std::shared_ptr<TupleTable> TridentLayer::query(Querier * querier,
        const bool cs1,
        const uint64_t s1,
        const bool cp1,
        const uint64_t p1,
        const bool co1,
        const uint64_t o1,
        int limit,
        double & sample) {

    int64_t s = -1;
    int64_t p = -1;
    int64_t o = -1;
    std::vector<int> vars;
    std::vector<int> posvars;
    if (!cs1) {
        vars.push_back(s1);
        posvars.push_back(0);
    } else {
        s = s1;
    }
    if (!cp1) {
        vars.push_back(p1);
        posvars.push_back(1);
    } else {
        p = p1;
    }
    if (!co1) {
        vars.push_back(o1);
        posvars.push_back(2);
    } else {
        o = o1;
    }

    int idx = querier->getIndex(s, p, o);
    PairItr *itr = querier->getIterator(idx, s, p, o);
    int * order = querier->getInvOrder(idx);
    int posToCopy[3];
    for (int i = 0; i < vars.size(); ++i) {
        posToCopy[i] = order[posvars[i]];
    }
    const uint8_t nPosToCopy = vars.size();

    //Create tuple table and return it
    std::shared_ptr<TupleTable> output(new TupleTable(vars));
    int i = 0;
    while (itr && itr->hasNext() && (limit == -1 || i++ < limit)) {
        itr->next();
        for (int i = 0; i < nPosToCopy; ++i) {
            switch (posToCopy[i]) {
                case 0:
                    output->addValue(itr->getKey());
                    break;
                case 1:
                    output->addValue(itr->getValue1());
                    break;
                case 2:
                    output->addValue(itr->getValue2());
                    break;
            }
        }
    }

    if (i >= limit && limit != -1) {
        //Set the sample rate
        const int64_t card = querier->estCard(s, p, o);
        sample = (double) limit / card;
        //LOG(DEBUGL) << "Sample " << sample << " card=" << card << " " << limit << " time=" << dur.count() * 1000;
    } else {
        //LOG(DEBUGL) << "No sampling" << i;
        sample = 1.0;
    }
    querier->releaseItr(itr);
    return output;
}

int64_t TridentLayer::getSizeOutput(int64_t s, int64_t p, int64_t o,
        std::vector<uint8_t> *posToFilter,
        std::vector<uint64_t> *valuesToFilter) {
    //LOG(DEBUGL) << "Start getSizeOutput " << valuesToFilter->size();
    assert(posToFilter);
    int64_t count = 0;
    int nvars = 0;
    int nconsts = 0;
    if (s < 0) {
        nvars++;
    } else {
        nconsts++;
    }
    if (p < 0) {
        nvars++;
    } else {
        nconsts++;
    }
    if (o < 0) {
        nvars++;
    } else {
        nconsts++;
    }

    //Do a merge join
    int idxVar = -2;
    uint8_t posJoins[3];
    const uint8_t njoins = posToFilter->size();
    for (int j = 0; j < posToFilter->size(); ++j) {
        switch (posToFilter->at(j)) {
            case 0:
                s = idxVar--;
                break;
            case 1:
                p = idxVar--;
                break;
            case 2:
                o = idxVar--;
                break;
        }
        posJoins[j] = nconsts + j;
    }
    const int idx = q->getIndex(s, p, o);
    if (s < 0)
        s = -1;
    if (p < 0)
        p = -1;
    if (o < 0)
        o = -1;
    PairItr *itr = q->getIterator(idx, s, p, o);
    size_t idxValues = 0;
    uint64_t *valsToFilter = &(valuesToFilter->at(0));
    while (itr->hasNext()) {
        itr->next();
        //Match?
next_test:
        int cmpValue = 0;
        for (uint8_t i = 0; i < njoins && !cmpValue; ++i) {
            switch (posJoins[i]) {
                case 0:
                    if (valsToFilter[idxValues + i] < itr->getKey()) {
                        cmpValue = -1;
                    } else if (valsToFilter[idxValues + i] > itr->getKey()) {
                        cmpValue = 1;
                    }
                    break;
                case 1:
                    if (valsToFilter[idxValues + i] < itr->getValue1()) {
                        cmpValue = -1;
                    } else if (valsToFilter[idxValues + i] > itr->getValue1()) {
                        cmpValue = 1;
                    }
                    break;
                case 2:
                    if (valsToFilter[idxValues + i] < itr->getValue2()) {
                        cmpValue = -1;
                    } else if (valsToFilter[idxValues + i] > itr->getValue2()) {
                        cmpValue = 1;
                    }
                    break;
            }
        }
        if (!cmpValue) {
            count++;
        } else if (cmpValue == -1) {
            idxValues += njoins;
            if (idxValues < valuesToFilter->size()) {
                goto next_test;
            } else {
                break;
            }
        } else {
            for (int i = 0; i < njoins; ++i) {
                if (posJoins[i] == 0) {
                    //Check if we need to move the key
                    if (itr->getKey() < valsToFilter[idxValues + i]) {
                        itr->gotoKey(valsToFilter[idxValues + i]);
                    }
                } else if (posJoins[i] == 1) {
                    if (i == 1) {
                        if (itr->getKey() != valsToFilter[idxValues + i - 1]) {
                            i = njoins;
                            continue;
                        }
                    }
                    if (itr->getValue1() < valsToFilter[idxValues + i]) {
                        itr->moveto(valsToFilter[idxValues + i], 0);
                    }
                } else { //posJoins == 2
                    if (itr->getValue2() < valsToFilter[idxValues + i]) {
                        itr->moveto(itr->getValue1(), valsToFilter[idxValues + i]);
                    }
                }
            }
        }
    }
    q->releaseItr(itr);

    return count;
}

//-----------------------------------------------------------------------------

uint64_t TridentScan::getValue1() {
    return itr->getKey();
}

uint64_t TridentScan::getValue2() {
    assert(a != DBLayer::Aggr_t::AGGR_SKIP_2LAST);
    return itr->getValue1();
}

uint64_t TridentScan::getValue3() {
    assert(a == DBLayer::Aggr_t::AGGR_NO);
    return itr->getValue2();
}

uint64_t TridentScan::getCount() {
    return itr->getCount();
}

bool TridentScan::next() {

    if (hint && countHint == 0) {
        uint64_t s = 0, p = 0, o = 0;
        if (a == DBLayer::AGGR_SKIP_LAST) {
            hint->next(s, p);
            if (s > itr->getKey()) {
                itr->gotoKey(s);
            } else if (s == itr->getKey()) {
                if (p > itr->getValue1()) {
                    itr->moveto(p, 0);
                }
            }
        } else if (a == DBLayer::AGGR_NO) {
            hint->next(s, p, o);
            if (p > itr->getValue1())
                itr->moveto(p, o);
        } else {
            //hint->next(s);
            //This is not supported (yet)
        }
    }
    if (countHint++ > COUNTHINT_MAX)
        countHint = 0;

    assert(itr != NULL);
    if (itr->hasNext()) {
        itr->next();
        //cerr <<  "Type=" << itr->getTypeItr() << " " << itr->getKey() << " " << itr->getValue1() << endl;
        return true;
    } else {
        q->releaseItr(itr);
        itr = NULL;
        return false;
    }
}

bool TridentScan::first() {
    if (a == DBLayer::AGGR_SKIP_2LAST) {
        itr = q->getTermList(perm);
        bool resp = itr->hasNext();
        if (resp)
            itr->next();
        return resp;
    } else {
        itr = q->getPermuted(perm, -1, -1, -1, false);
        if (a == DBLayer::AGGR_SKIP_LAST)
            itr->ignoreSecondColumn();
        bool resp = itr->hasNext();
        if (resp)
            itr->next();
        return resp;
    }
}

bool TridentScan::first(uint64_t el, bool constrained) {
    if (a != DBLayer::Aggr_t::AGGR_NO)
        throw 10; //Not supported

    if (constrained)
        itr = q->getPermuted(perm, el, -1, -1, true);
    else
        itr = q->getPermuted(perm, -1, -1, -1, false);

    if (itr->hasNext()) {
        itr->next();
        return true;
    } else {
        q->releaseItr(itr);
        itr = NULL;
        return false;
    }
}

bool TridentScan::first(uint64_t el1, bool constrained1, uint64_t el2, bool constrained2) {
    if (a == DBLayer::Aggr_t::AGGR_SKIP_2LAST)
        throw 10; //Not supported. Should also never occur

    if (constrained1) {
        if (constrained2) {
            itr = q->getPermuted(perm, el1, el2, -1, true);
        } else {
            itr = q->getPermuted(perm, el1, -1, -1, true);
        }
    } else {
        itr = q->getPermuted(perm, -1, -1, -1, false);
    }

    if (a == DBLayer::Aggr_t::AGGR_SKIP_LAST) {
        itr->ignoreSecondColumn();
    }
    if (itr->hasNext()) {
        itr->next();
        return true;
    } else {
        q->releaseItr(itr);
        itr = NULL;
        return false;
    }
}

bool TridentScan::first(uint64_t el1, bool constrained1, uint64_t el2,
        bool constrained2, uint64_t el3, bool constrained3) {

    if (a != DBLayer::Aggr_t::AGGR_NO)
        throw 10; //Not supported. Should also never occur

    if (constrained1) {
        if (constrained2) {
            if (constrained3) {
                itr = q->getPermuted(perm, el1, el2, el3, true);
            } else {
                itr = q->getPermuted(perm, el1, el2, -1, true);
            }
        } else {
            itr = q->getPermuted(perm, el1, -1, -1, true);
        }
    } else {
        itr = q->getPermuted(perm, -1, -1, -1, false);
    }

    bool resp = itr->hasNext();
    if (resp) {
        itr->next();
    } else {
        q->releaseItr(itr);
        itr = NULL;
    }
    return resp;
}

TridentScan::~TridentScan() {
    if (itr != NULL) {
        q->releaseItr(itr);
        itr = NULL;
    }
}
