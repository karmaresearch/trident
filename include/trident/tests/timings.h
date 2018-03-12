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


#ifndef _TIMINGS_H
#define _TIMINGS_H

#include <trident/kb/consts.h>

#include <string>
#include <chrono>

#define T_SPO 0
#define T_OPS 1
#define T_POS 2
#define T_SOP 3
#define T_OSP 4
#define T_PSO 5

class Timings {
private:
    std::string queryfile;

public:
    Timings(std::string filequeries) : queryfile(filequeries) {}

    virtual void init() = 0;

    virtual std::chrono::duration<double> launchQuery(const int perm,
            const int64_t s,
            const int64_t p,
            const int64_t o,
            const int countIgnores,
            int64_t &c,
            int64_t &junk) = 0;

    LIBEXP void launchTests();
};

#endif
