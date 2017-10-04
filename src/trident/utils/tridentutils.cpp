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

#include <trident/utils/tridentutils.h>

#include <kognac/logs.h>

#include <boost/lexical_cast.hpp>

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <iostream>
#include <fstream>
#include <sys/statvfs.h>


using namespace std;

long parseLine(char* line, int skip){
    long i = strlen(line);
    const char* p = line;
    while (*p <'0' || *p > '9') p++;
    line[i - skip] = '\0';
    i = atol(p);
    return i;
}

long TridentUtils::getVmRSS() {
    FILE* file = fopen("/proc/self/status", "r");
    long result = -1;
    char line[128];

    while (fgets(line, 128, file) != NULL){
        if (strncmp(line, "VmRSS:", 6) == 0){
            result = parseLine(line, 3);
            break;
        }
        //cout << line << endl;
    }
    fclose(file);
    return result;
}

static unsigned long long lastTotalUser = 0, lastTotalUserLow = 0, lastTotalSys = 0, lastTotalIdle = 0;
double TridentUtils::getCPUUsage() {
    double percent;
    FILE* file;
    unsigned long long totalUser, totalUserLow, totalSys, totalIdle, total;
    file = fopen("/proc/stat", "r");
    fscanf(file, "cpu %llu %llu %llu %llu", &totalUser, &totalUserLow, &totalSys, &totalIdle);
    fclose(file);
    if (totalUser < lastTotalUser || totalUserLow < lastTotalUserLow || totalSys < lastTotalSys || totalIdle < lastTotalIdle) {
        percent = -1.0;
    } else {
        total = (totalUser - lastTotalUser) + (totalUserLow - lastTotalUserLow) + (totalSys - lastTotalSys);
        percent = total;
        total += (totalIdle - lastTotalIdle);
        percent /= total;
        percent *= 100;
    }
    lastTotalUser = totalUser;
    lastTotalUserLow = totalUserLow;
    lastTotalSys = totalSys;
    lastTotalIdle = totalIdle;
    return percent;
}

long TridentUtils::diskread() {
    FILE* file = fopen("/proc/self/io", "r");
    long result = -1;
    char line[128];

    while (fgets(line, 128, file) != NULL){
        if (strncmp(line, "rchar: ", 7) == 0){
            result = parseLine(line, 0);
            break;
        }
    }
    fclose(file);
    return result;
}

long TridentUtils::diskwrite() {
    FILE* file = fopen("/proc/self/io", "r");
    long result = -1;
    char line[128];

    while (fgets(line, 128, file) != NULL){
        if (strncmp(line, "wchar: ", 7) == 0){
            result = parseLine(line, 0);
            break;
        }
    }
    fclose(file);
    return result;
}

long TridentUtils::phy_diskread() {
    FILE* file = fopen("/proc/self/io", "r");
    long result = -1;
    char line[128];

    while (fgets(line, 128, file) != NULL){
        if (strncmp(line, "read_bytes: ", 12) == 0){
            result = parseLine(line, 0);
            break;
        }
    }
    fclose(file);
    return result;
}

long TridentUtils::phy_diskwrite() {
    FILE* file = fopen("/proc/self/io", "r");
    long result = -1;
    char line[128];

    while (fgets(line, 128, file) != NULL){
        if (strncmp(line, "write_bytes: ", 13) == 0){
            result = parseLine(line, 0);
            break;
        }
    }
    fclose(file);
    return result;
}

void TridentUtils::loadFromFile(string inputfile, std::vector<long> &values) {
    std::ifstream ifs(inputfile);
    std::string line;
    while (std::getline(ifs, line)) {
        long value;
        try {
            value = boost::lexical_cast<long>(line);
        } catch (boost::bad_lexical_cast &) {
            LOG(ERRORL) << "Failed conversion of " << line;
            throw 10;
        }
        values.push_back(value);
    }
    ifs.close();
}

void TridentUtils::loadPairFromFile(std::string inputfile,
        std::vector<std::pair<long,long>> &values, char sep) {
    std::ifstream ifs(inputfile);
    std::string line;
    while (std::getline(ifs, line)) {
        long v1, v2;
        try {
            auto pos = line.find(sep);
            v1 = boost::lexical_cast<long>(line.substr(0, pos));
            v2 = boost::lexical_cast<long>(line.substr(pos+1, line.size()));
        } catch (boost::bad_lexical_cast &) {
            LOG(ERRORL) << "Failed conversion of " << line;
            throw 10;
        }
        values.push_back(std::make_pair(v1, v2));
    }
    ifs.close();
}

uint64_t TridentUtils::spaceLeft(std::string location) {
    struct statvfs stat;
    statvfs(location.c_str(), &stat);
    return stat.f_bsize * stat.f_bfree;
    //TODO: does it work on the mac?
}
