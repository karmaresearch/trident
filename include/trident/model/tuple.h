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


#ifndef _TUPLE_H
#define _TUPLE_H

#include <trident/model/term.h>
#include <vector>

#define MAXTUPLESIZE 3
class Tuple {
private:
    const uint8_t sizetuple;
    Term terms[MAXTUPLESIZE];
public:
    Tuple(const uint8_t sizetuple) : sizetuple(sizetuple) {}
    size_t getSize() const {
        return sizetuple;
    }
    Term get(const int pos) const {
        return terms[pos];
    }
    void set(const Term term, const int pos) {
        terms[pos] = term;
    }

    std::vector<std::pair<uint8_t, uint8_t>> getRepeatedVars() const {
        std::vector<std::pair<uint8_t, uint8_t>> output;
        for (uint8_t i = 0; i < sizetuple; ++i) {
            Term t1 = get(i);
            if (t1.isVariable()) {
                for (uint8_t j = i + 1; j < sizetuple; ++j) {
                    Term t2 = get(j);
                    if (t2.getId() == t1.getId()) {
                        output.push_back(std::make_pair(i, j));
                    }
                }
            }
        }
        return output;
    }
};


#endif
