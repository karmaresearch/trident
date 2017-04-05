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
    long constraint1;
    long constraint2;
    long key;
public:
    void initializeConstraints() {
	setConstraint1(NO_CONSTRAINT);
	setConstraint2(NO_CONSTRAINT);
    }   

    virtual int getTypeItr() = 0;

    virtual long getValue1() = 0;

    virtual long getValue2() = 0;

    long getKey() {
        return key;
    }

    void setKey(long key) {
        this->key = key;
    }

    virtual bool hasNext() = 0;

    virtual void next() = 0;

    virtual bool next(long &v1, long &v2, long &v3) {
        next();
        return hasNext();
    }

    virtual void ignoreSecondColumn() = 0;

    virtual long getCount() = 0;

    virtual uint64_t getCardinality() = 0;

    virtual uint64_t estCardinality() = 0;

    virtual ~PairItr() {
    }

    virtual void clear() = 0;

    long getConstraint2() {
        return constraint2;
    }

    virtual void setConstraint2(const long c2) {
        constraint2 = c2;
    }

    long getConstraint1() {
        return constraint1;
    }

    virtual void setConstraint1(const long c1) {
        constraint1 = c1;
    }

    virtual void mark() = 0;

    virtual void reset(const char i) = 0;

    virtual void gotoKey(long k) {
        throw 10; //The only iterator that can use this method is scanitr,
        //whic overrides it.
    }

    virtual long getValue1AtRow(long rowid) {
        //Only a column-oriented approach implements it
        throw 10;
    }

    virtual long getValue2AtRow(long rowid) {
        //Only a column-oriented approach implements it
        throw 10;
    }

    virtual void moveto(const long c1, const long c2) = 0;
};

#endif
