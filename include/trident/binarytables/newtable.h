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


#ifndef _NEWTABLE_H
#define _NEWTABLE_H

#include <trident/iterators/pairitr.h>



class AbsNewTable : public PairItr {
    public:
        virtual char getReaderSize1() const = 0;

        virtual char getReaderSize2() const = 0;

        virtual char getReaderCountSize() const = 0;

        virtual void setup(const char* start, const char *end) = 0;

        virtual void setup(long c1, const char* start, const char *end) = 0;

        virtual void setup(long c1, long c2, const char* start, const char *end) = 0;

        virtual long _getValue1AtRow(const char *start,
                const long rowId) const  {
            throw 10;
        }
};

#endif
