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


#ifndef PAIRITR_H_
#define PAIRITR_H_


#include <inttypes.h>

#define NO_CONSTRAINT -1

class PairItr {
    protected:
        int64_t constraint1;
        int64_t constraint2;
        int64_t key;
    public:
        void initializeConstraints() {
            setConstraint1(NO_CONSTRAINT);
            setConstraint2(NO_CONSTRAINT);
        }

        virtual int getTypeItr() = 0;

        virtual int64_t getValue1() = 0;

        virtual int64_t getValue2() = 0;

        int64_t getKey() {
            return key;
        }

        void setKey(int64_t key) {
            this->key = key;
        }

        virtual bool hasNext() = 0;

        virtual void next() = 0;

        virtual bool next(int64_t &v1, int64_t &v2, int64_t &v3) {
            next();
            return hasNext();
        }

        virtual void ignoreSecondColumn() = 0;

        virtual int64_t getCount() = 0;

        virtual uint64_t getCardinality() = 0;

        virtual uint64_t estCardinality() = 0;

        virtual ~PairItr() {
        }

        virtual void clear() = 0;

        int64_t getConstraint2() {
            return constraint2;
        }

        virtual void setConstraint2(const int64_t c2) {
            constraint2 = c2;
        }

        int64_t getConstraint1() {
            return constraint1;
        }

        virtual void setConstraint1(const int64_t c1) {
            constraint1 = c1;
        }

        virtual void mark() = 0;

        virtual void reset(const char i) = 0;

        virtual void gotoKey(int64_t k) {
            throw 10; //The only iterator that can use this method is scanitr,
            //whic overrides it.
        }

        virtual int64_t getValue1AtRow(int64_t rowid) {
            //Only a column-oriented approach implements it
            throw 10;
        }

        virtual int64_t getValue2AtRow(int64_t rowid) {
            //Only a column-oriented approach implements it
            throw 10;
        }

        virtual void moveto(const int64_t c1, const int64_t c2) = 0;

};

#endif
