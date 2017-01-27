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


#ifndef PM_H_
#define PM_H_

#include <boost/log/trivial.hpp>

#include <string>
#include <map>
#include <cstring>

using namespace std;

class PropertyMap {
private:
    map<int, string> privateMap;

public:
    void setInt(int key, int value) {
        string svalue = to_string(value);
        set(key, svalue);
    }

    int getInt(int key) {
        map<int, string>::iterator itr = privateMap.find(key);
        if (itr != privateMap.end()) {
            return stoi(itr->second);
        }
        throw 10;
    }

    int getInt(int key, int defaultvalue) {
        map<int, string>::iterator itr = privateMap.find(key);
        if (itr != privateMap.end()) {
            return stoi(itr->second);
        }
        return defaultvalue;
    }

    void setLong(int key, long value) {
        string svalue = to_string(value);
        set(key, svalue);
    }

    long getLong(int key, long defaultv) {
        map<int, string>::iterator itr = privateMap.find(key);
        if (itr != privateMap.end()) {
            return stol(itr->second);
        }
        return defaultv;
    }

    long getLong(int key) {
        map<int, string>::iterator itr = privateMap.find(key);
        if (itr != privateMap.end()) {
            return stol(itr->second);
        }
        throw 10;
    }


    void setBool(int key, bool value) {
        string svalue = to_string(value);
        set(key, svalue);
    }

    bool getBool(int key) {
        map<int, string>::iterator itr = privateMap.find(key);
        if (itr != privateMap.end()) {
            return stoi(itr->second) != 0;
        }
        throw 10;
    }

    bool getBool(int key, bool defaultv) {
        map<int, string>::iterator itr = privateMap.find(key);
        if (itr != privateMap.end()) {
            return stoi(itr->second) != 0;
        }
        return defaultv;
    }

    void set(int key, string value) {
        map<int, string>::iterator itr = privateMap.find(key);
        if (itr != privateMap.end()) {
            itr->second = value;
        } else {
            privateMap.insert(make_pair(key, value));
        }
    }

    string get(int key) {
        map<int, string>::iterator itr = privateMap.find(key);
        if (itr != privateMap.end()) {
            return itr->second;
        }
        throw 10;
    }
};

#endif
