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


#ifndef UPDATER_H_
#define UPDATER_H_

#include <trident/kb/kbconfig.h>
#include <trident/kb/diffindex.h>

#include <kognac/stringscol.h>
#include <kognac/hashmap.h>

#include <string>

class Querier;
class PairItr;
class KB;
class DictMgmt;
class Updater {
private:

    struct Triple {
        uint64_t s;
        uint64_t p;
        uint64_t o;

        static bool sorter(const Triple &t1, const Triple &t2) {
            if (t1.s < t2.s) {
                return true;
            } else if (t1.s == t2.s) {
                if (t1.p < t2.p) {
                    return true;
                } else if (t1.p == t2.p) {
                    return t1.o < t2.o;
                }
            }
            return false;
        }

        static bool equal(const Triple &t1, const Triple &t2) {
            return t1.s == t2.s && t1.p == t2.p && t1.o == t2.o;
        }

    };

    struct TextualTriple {
        const char *s;
        int lens;
        uint64_t nums;
        const char *p;
        uint64_t nump;
        int lenp;
        const char *o;
        uint64_t numo;
        int leno;
    };

    void compressUpdate(DiffIndex::TypeUpdate type,
                        string updatedir,
                        std::vector<uint64_t> &all_s,
                        std::vector<uint64_t> &all_p,
                        std::vector<uint64_t> &all_o,
                        KB *kb,
                        Querier *q,
                        ByteArrayToNumberMap &tmpdict,
                        StringCollection &tmpdictsupport);

    void parseUpdate(std::string update,
                     StringCollection &support,
                     std::vector<TextualTriple> &output);

    static void match(DiffIndex::TypeUpdate type,
                      std::vector<uint64_t> &outputs,
                      std::vector<uint64_t> &outputp,
                      std::vector<uint64_t> &outputo,
                      Querier *q,
                      std::vector<Triple> &input);

    static bool sortByS(const TextualTriple &t1, const TextualTriple &t2) {
        return std::string(t1.s, t1.lens) < std::string(t2.s, t2.lens);
    }

    static bool sortByP(const TextualTriple &t1, const TextualTriple &t2) {
        return std::string(t1.p, t1.lenp) < std::string(t2.p, t2.lenp);
    }

    static bool sortByO(const TextualTriple &t1, const TextualTriple &t2) {
        return std::string(t1.o, t1.leno) < std::string(t2.o, t2.leno);
    }

    static int cmp(PairItr *itr, const Triple &t);

    static void writeDict(DictMgmt *dictmgmt, string updatedir, ByteArrayToNumberMap &dict);

public:
    void creatediffupdate(DiffIndex::TypeUpdate type, std::string kbdir, std::string updatedir);

    static std::string getPathForUpdate(std::string kbdir);
};
#endif
