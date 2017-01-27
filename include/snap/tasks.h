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


#ifndef _METH_H
#define _METH_H

#include <string>
#include <vector>
#include <map>

#include <boost/lexical_cast.hpp>

using namespace std;

class AnalyticsTasks {
    public:
        typedef enum e_type { DOUBLE, INT, LONG, BOOL } TYPE;

        struct Param {
            const string name;
            const TYPE type;
            const string defvalue;
            string value;

            Param(string name, TYPE type, string defvalue) : name(name), type(type), defvalue(defvalue), value("") {
            }

            void set(string value);

            string getRawValue() {
                if (value == "")
                    return defvalue;
                else
                    return value;
            }

            template<typename K>
                K as() {
                    return boost::lexical_cast<K>(getRawValue());
                }
        };

        struct Task {
            const string name;
            vector<Param> params;

            Task(string name, vector<Param> params) : name(name), params(params) {}

            void updateParam(string nameparam, string valueparam);

            Param &getParam(string nameparam);

            string tostring();
        };

        bool isInitialized() {
            return init;
        }

    private:
        static AnalyticsTasks __tasks_;

        std::map<string, Task> tasks;
        bool init;

        void initialize();

        string getStringType(TYPE t);

    public:
        static AnalyticsTasks &getInstance() {
            if (!AnalyticsTasks::__tasks_.isInitialized()) {
                AnalyticsTasks::__tasks_.initialize();
            }
            return AnalyticsTasks::__tasks_;
        }

        AnalyticsTasks() {
            init = false;
        }

        std::map<string, Task> getTasks() {
            return tasks;
        }

        Task getTask(string name) {
            return tasks.find(name)->second;
        }

        bool isValidTask(string name) {
            return tasks.find(name) != tasks.end();
        }

        string getTaskDetails();

        void load(string nametask, string raw);
};
#endif
