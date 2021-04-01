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


#ifndef _TRIDENT_LAYER_H
#define _TRIDENT_LAYER_H

#include <trident/kb/kb.h>
#include <trident/kb/querier.h>
#include <trident/model/table.h>
#include <kognac/stringscol.h>
#include <dblayer.hpp>

#define COUNTHINT_MAX 1
#define SMALLREL 20
#define LIMIT_SAMPLE 100

class TridentScan : public DBLayer::Scan {
    private:
        const DBLayer::Aggr_t a;
        const int perm;
        PairItr *itr;
        Querier *q;
        DBLayer::Hint *hint;
        size_t countHint;

    public:
        TridentScan(const int perm, const DBLayer::Aggr_t a,
                Querier *q, DBLayer::Hint *hint) : a(a), perm(perm),
        itr(NULL),
        q(q),
        hint(hint),
        countHint(0) {
        }

        uint64_t getValue1();

        uint64_t getValue2();

        uint64_t getValue3();

        uint64_t getCount();

        bool next();

        bool first();

        bool first(uint64_t, bool);

        bool first(uint64_t, bool, uint64_t, bool);

        bool first(uint64_t, bool, uint64_t, bool, uint64_t, bool);

        ~TridentScan();
};

class TridentLayer : public DBLayer {
    private:
        KB &kb;
        DictMgmt *dict;
        std::unique_ptr<Querier> q;
        bool bifSampl;
        const int nindices;

        //Used to translate IDs back to strings
        std::unique_ptr<char[]> supportBuffer;

        std::shared_ptr<TupleTable> query(Querier *q,
                const bool cs1,
                const uint64_t s1,
                const bool cp1,
                const uint64_t p1,
                const bool co1,
                const uint64_t o1,
                const int limit,
                double &sampleRate);

        std::shared_ptr<TupleTable> query(const bool cs1,
                const uint64_t s1,
                const bool cp1,
                const uint64_t p1,
                const bool co1,
                const uint64_t o1) {
            double sample = 0;
            return query(q.get(), cs1, s1, cp1, p1, co1, o1, -1, sample);
        }

        std::shared_ptr<TupleTable> sampleQuery(const bool cs1,
                const uint64_t s1,
                const bool cp1,
                const uint64_t p1,
                const bool co1,
                const uint64_t o1,
                const int limit,
                double &sampleRate) {
            return query(&q->getSampler(), cs1, s1, cp1, p1, co1, o1, limit,
                    sampleRate);
        }

        int64_t getSizeOutput(int64_t s, int64_t p, int64_t o,
                std::vector<uint8_t> *posToFilter,
                std::vector<uint64_t> *valuesToFilter);

        double bifocalSampling(bool valueL1,
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
                const int64_t card2);

        double bifocalSampling_DenseDense(bool valueL1,
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
                const int64_t card2);

        double bifocalSampling_SparseAny(bool valueL1,
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
                const int64_t card2);

    public:
        TridentLayer(KB &kb) : kb(kb), dict(kb.getDictMgmt()), q(kb.query()),
        bifSampl(true), nindices(kb.getNIndices()), supportBuffer(new
                char[MAX_TERM_SIZE]) {
	    if (kb.getNIndices() < 6) {
		disableBifocalSampling();
	    }
	}

		DDLEXPORT bool lookup(const std::string& text,
                ::Type::ID type,
                unsigned subType,
                uint64_t& id);

        void disableBifocalSampling() {
            bifSampl = false;
        }

		DDLEXPORT bool lookupById(uint64_t id,
                const char*& start,
                const char*& stop,
                ::Type::ID& type,
                unsigned& subType);

		DDLEXPORT bool lookupById(uint64_t id,
                char *output,
                size_t &length,
                ::Type::ID& type,
                unsigned& subType);

        DDLEXPORT uint64_t getNextId();

        DDLEXPORT double getScanCost(DBLayer::DataOrder order,
                uint64_t value1,
                uint64_t value1C,
                uint64_t value2,
                uint64_t value2C,
                uint64_t value3,
                uint64_t value3C);

		DDLEXPORT double getScanCost(DBLayer::DataOrder order,
                uint64_t value1,
                uint64_t value1C,
                uint64_t value2,
                uint64_t value2C);

		DDLEXPORT double getJoinSelectivity(bool valueL1,
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
                uint64_t value3CR);

		DDLEXPORT double getScanCost(DBLayer::DataOrder order,
                uint64_t value1,
                uint64_t value1C);

        DDLEXPORT uint64_t getCardinality(uint64_t c1,
                uint64_t c2,
                uint64_t c3);

        DDLEXPORT uint64_t getCardinality();

        uint64_t getNTerms() {
            return kb.getNTerms();
        }

		DDLEXPORT std::unique_ptr<DBLayer::Scan> getScan(const DBLayer::DataOrder order,
                const DBLayer::Aggr_t,
                Hint *hint);

        Querier *getQuerier() {
            return q.get();
        }

        KB *getKB() {
            return &kb;
        }
};

#endif
