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


#ifndef trident_filter_h
#define trident_filter_h

#include <inttypes.h>

class Filter {
public:
    virtual bool isValid(uint64_t v1, uint64_t v2, uint64_t v3) const = 0;

    virtual ~Filter() {}
};

class SameVarFilter : public Filter {
private:
    const int pos1;
    const int pos2;
    const int pos3;
public:
    SameVarFilter(int pos1, int pos2, int pos3) : pos1(pos1), pos2(pos2), pos3(pos3) {}

    bool isValid(uint64_t v1, uint64_t v2, uint64_t v3) const {
        uint64_t value = 0;
        switch (pos1) {
        case 0:
            value = v1;
            break;
        case 1:
            value = v2;
            break;
        case 2:
            value = v3;
            break;
        }

        bool ok = false;
        switch (pos2) {
        case 0:
            ok = value == v1;
            break;
        case 1:
            ok = value == v2;
            break;
        case 2:
            ok = value == v3;
            break;
        }
        if (!ok) {
            return false;
        }

        if (pos3 != -1) {
            switch (pos3) {
            case 0:
                ok = value == v1;
                break;
            case 1:
                ok = value == v2;
                break;
            case 2:
                ok = value == v3;
                break;
            }
        }

        return ok;
    }
};
#endif
