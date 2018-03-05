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


#ifndef _RES_STATS_H
#define _RES_STATS_H

#include <kognac/logs.h>

#include <vector>
#include <string>
#include <sstream>

class TridentUtils {
    public:

        static int64_t getVmRSS();

        static double getCPUUsage();

        static int64_t diskread();

        static int64_t diskwrite();

        static int64_t phy_diskread();

        static int64_t phy_diskwrite();

        static void loadFromFile(std::string inputfile,
                std::vector<int64_t> &values);

        static void loadPairFromFile(std::string inputfile,
                std::vector<std::pair<int64_t,int64_t>> &values, char sep);

        static uint64_t spaceLeft(std::string location);

        template<typename K>
            static K lexical_cast(std::string v) {
                K var;
                std::istringstream iss;
                iss.str(v);
                iss >> var;
                if (iss.fail()) {
                    LOG(ERRORL) << "Failed conversion of " << v << " " << var;
                    throw 10;
                }
                return var;
            }
};

#endif
