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

#include <stdlib.h>
#include <stdio.h>
#include <inttypes.h>
#include <string.h>
#include <iostream>
#include <fstream>
#include <thread>
#include <condition_variable>

#if defined(__unix__) || defined(__unix) || defined(unix) || (defined(__APPLE__) && defined(__MACH__))
#include <sys/statvfs.h>
#elif defined(_WIN32)

#endif

using namespace std;

int64_t parseLine(char* line, int skip){
    int i = strlen(line);
    const char* p = line;
    while (*p <'0' || *p > '9') p++;
    line[i - skip] = '\0';
    return std::stoll(p);
}

int64_t TridentUtils::getVmRSS() {
    FILE* file = fopen("/proc/self/status", "r");
    int64_t result = -1;
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

static uint64_t lastTotalUser = 0, lastTotalUserLow = 0, lastTotalSys = 0, lastTotalIdle = 0;
double TridentUtils::getCPUUsage() {
    double percent;
    FILE* file;
    uint64_t totalUser, totalUserLow, totalSys, totalIdle, total;
    file = fopen("/proc/stat", "r");
    fscanf(file, "cpu %" SCNu64 " %" SCNu64 " %" SCNu64 " %" SCNu64, &totalUser, &totalUserLow, &totalSys, &totalIdle);
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

int64_t TridentUtils::diskread() {
    FILE* file = fopen("/proc/self/io", "r");
    int64_t result = -1;
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

int64_t TridentUtils::diskwrite() {
    FILE* file = fopen("/proc/self/io", "r");
    int64_t result = -1;
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

int64_t TridentUtils::phy_diskread() {
    FILE* file = fopen("/proc/self/io", "r");
    int64_t result = -1;
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

int64_t TridentUtils::phy_diskwrite() {
    FILE* file = fopen("/proc/self/io", "r");
    int64_t result = -1;
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

void TridentUtils::loadFromFile(string inputfile, std::vector<int64_t> &values) {
    std::ifstream ifs(inputfile);
    std::string line;
    while (std::getline(ifs, line)) {
        int64_t value;
        try {
            value = TridentUtils::lexical_cast<int64_t>(line);
        } catch (int v) {
            LOG(ERRORL) << "Failed conversion of " << line << v;
            throw 10;
        }
        values.push_back(value);
    }
    ifs.close();
}

void TridentUtils::loadPairFromFile(std::string inputfile,
        std::vector<std::pair<int64_t,int64_t>> &values, char sep) {
    std::ifstream ifs(inputfile);
    std::string line;
    while (std::getline(ifs, line)) {
        int64_t v1, v2;
        try {
            auto pos = line.find(sep);
            v1 = TridentUtils::lexical_cast<int64_t>(line.substr(0, pos));
            v2 = TridentUtils::lexical_cast<int64_t>(line.substr(pos+1, line.size()));
        } catch (int v) {
            LOG(ERRORL) << "Failed conversion of " << line << v;
            throw 10;
        }
        values.push_back(std::make_pair(v1, v2));
    }
    ifs.close();
}

uint64_t TridentUtils::spaceLeft(std::string location) {
#if defined(_WIN32)
    LOG(ERRORL) << "spaceLeft not supported under Windows";
    throw 10;
#else
    //Does it work on the mac?
    struct statvfs stat;
    statvfs(location.c_str(), &stat);
    return stat.f_bsize * stat.f_bfree;
#endif
}

void TridentUtils::monitorPerformance(int seconds, std::condition_variable *cv,
        std::mutex *mtx,  bool *isFinished) {
    //Monitor CPU usage, memory usage and disk I/O
    while (true) {
        std::unique_lock<std::mutex> lck(*mtx);
        cv->wait_for(lck,std::chrono::seconds(seconds));
        if (*isFinished)
            break;
        lck.unlock();

        uint64_t mem = TridentUtils::getVmRSS();
        double cpu = TridentUtils::getCPUUsage();
        uint64_t diskread = TridentUtils::diskread();
        uint64_t diskwrite = TridentUtils::diskwrite();
        uint64_t phy_diskread = TridentUtils::phy_diskread();
        uint64_t phy_diskwrite = TridentUtils::phy_diskwrite();
        LOG(DEBUGL) << "STATS:\tvmrss_kb=" << mem << "\tcpu_perc=" << cpu
            << "\tdiskread_bytes=" << diskread << "\tdiskwrite_bytes="
            << diskwrite << "\tphydiskread_bytes=" << phy_diskread
            << "\tphydiskwrite_bytes=" << phy_diskwrite;
    }
}
