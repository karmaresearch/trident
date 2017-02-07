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

#include <vector>
#include <string>

class TridentUtils {
    public:

        static long getVmRSS();

        static double getCPUUsage();

        static long diskread();

        static long diskwrite();

        static long phy_diskread();

        static long phy_diskwrite();

        static void loadFromFile(std::string inputfile,
                std::vector<long> &values);

        static void loadPairFromFile(std::string inputfile,
                std::vector<std::pair<long,long>> &values, char sep);
};

#endif
