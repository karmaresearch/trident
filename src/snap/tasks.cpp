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


#include <snap/tasks.h>

#include <boost/log/trivial.hpp>

#include <iostream>


AnalyticsTasks AnalyticsTasks::__tasks_;

void AnalyticsTasks::initialize() {
    std::vector<Param> params;
    params.push_back(Param("maxiter", INT, "100"));
    params.push_back(Param("eps", DOUBLE, "0"));
    params.push_back(Param("C", DOUBLE, "0.85"));
    tasks.insert(std::make_pair("pagerank", Task("pagerank", params)));

    params.clear();
    params.push_back(Param("maxiter", INT, "20"));
    tasks.insert(std::make_pair("hits", Task("hits", params)));

    params.clear();
    params.push_back(Param("nodefrac", DOUBLE, "1.0"));
    tasks.insert(std::make_pair("betcentr", Task("betcentr", params)));

    params.clear();
    params.push_back(Param("samplen", INT, "-1"));
    tasks.insert(std::make_pair("avg_clustcoef", Task("avg_clustcoef", params)));

    params.clear();
    tasks.insert(std::make_pair("clustcoef", Task("clustcoef", params)));

    params.clear();
    params.push_back(Param("samplen", INT, "-1"));
    tasks.insert(std::make_pair("triads", Task("triads", params)));

    params.clear();
    tasks.insert(std::make_pair("triangles", Task("triangles", params)));

    params.clear();
    params.push_back(Param("src", LONG, ""));
    params.push_back(Param("dst", LONG, ""));
    tasks.insert(std::make_pair("bfs", Task("bfs", params)));

    init = true;
}

string AnalyticsTasks::getTaskDetails() {
    string out = "";
    for(auto task : tasks) {
        out += "TASK: " + task.first + " PARAMS:\n";
        for(auto param : task.second.params) {
            out += "  name=" + param.name + "  typ=" + getStringType(param.type) + "  def=" + param.defvalue + "\n";
        }
    }
    return out;
}

string AnalyticsTasks::getStringType(TYPE t) {
    switch (t) {
        case LONG:
            return "long";
        case INT:
            return "int";
        case DOUBLE:
            return "double";
        case BOOL:
            return "bool";
        default:
            return "unknown";
    }
}

void AnalyticsTasks::load(string nametask, string raw) {
    if (raw == "")
        return;

    istringstream f(raw);
    string s;
    while (getline(f, s, ';')) {
        size_t pos = s.find("=");
        if (pos == string::npos) {
            BOOST_LOG_TRIVIAL(error) << "Param string not well-formed";
            throw 10;
        }
        string nameparam = s.substr(0, pos);
        string valueparam = s.substr(pos + 1, s.length());
        if (nameparam.empty() || valueparam.empty()) {
            BOOST_LOG_TRIVIAL(error) << "Name or value params are empty";
            throw 10;
        }

        auto task = tasks.find(nametask);
        if (task == tasks.end()) {
            BOOST_LOG_TRIVIAL(error) << "Param task " << nametask << " not found";
            throw 10;
        }
        task->second.updateParam(nameparam, valueparam);
    }
}

AnalyticsTasks::Param &AnalyticsTasks::Task::getParam(string nameparam) {
    for(int i = 0; i < params.size(); ++i) {
        if (params[i].name == nameparam)
            return params[i];
    }
    BOOST_LOG_TRIVIAL(error) << "Param " << nameparam << " not found";
    throw 10;
}


void AnalyticsTasks::Task::updateParam(string nameparam, string valueparam) {
    Param &p = getParam(nameparam);
    p.set(valueparam);
}

string AnalyticsTasks::Task::tostring() {
    string out = name + " ";
    for (auto p : params) {
        out += p.name + "=" + p.getRawValue() + " ";
    }
    return out;
}

void AnalyticsTasks::Param::set(string value) {
    //Check the type
    try {
        switch (type) {
            case DOUBLE:
                boost::lexical_cast<double>(value);
                break;
            case INT:
                boost::lexical_cast<int>(value);
                break;
            case LONG:
                boost::lexical_cast<long>(value);
                break;
            case BOOL:
                boost::lexical_cast<bool>(value);
                break;
        }
    } catch (boost::bad_lexical_cast &) {
        BOOST_LOG_TRIVIAL(error) << "Failed conversion of " << value;
        throw 10;
    }
    this->value = value;
}
