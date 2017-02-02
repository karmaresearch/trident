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


#ifndef _ANALYTICS_H
#define _ANALYTICS_H

#include <snap-core/Snap.h>

#include <trident/kb/kb.h>
#include <trident/kb/querier.h>
#include <trident/kb/dictmgmt.h>
#include <trident/analytics/nativetasks.h>

#include <snap/tasks.h>

#include <snap/directed.h>
#include <snap/undirected.h>

#include <boost/iostreams/filtering_stream.hpp>
#include <boost/iostreams/filter/gzip.hpp>

#include <functional>

typedef enum _RetValue { NONE, DOUBLE, INT } F_RetValue;

class Analytics {

    private:
        template<class K, class V>
            static void saveToFile(K Graph,
                    F_RetValue retValue,
                    int64 retValue_int,
                    double retValue_double,
                    int outfields,
                    std::vector<float> &values1,
                    std::vector<float> &values2,
                    string outputfile) {
                BOOST_LOG_TRIVIAL(info) << "Saving the results on " << outputfile << " ...";
                std::ofstream file_out(outputfile, ios_base::binary);
                {
                    boost::iostreams::filtering_ostream out;
                    out.push(boost::iostreams::gzip_compressor());
                    out.push(file_out);

                    //First write the output value (if any)
                    switch (retValue) {
                        case INT:
                            out << "OUTPUT: " << retValue_int << endl;
                            break;
                        case DOUBLE:
                            out << "OUTPUT: " << retValue_double << endl;
                            break;
                        default:
                            break;
                    }

                    //Then write stats per nodes
                    if (outfields > 0) {
                        out << "NODE_ID\t";
                        for(int i = 0; i < outfields; ++i) {
                            out << "FIELD" << to_string(i) << "\t";
                        }
                        out << "ORIG_NODE_TEXT" << "\n";
                        DictMgmt *dict = Graph->getQuerier()->getDictMgmt();
                        std::unique_ptr<char[]> supportBuffer = std::unique_ptr<char[]>(new char[MAX_TERM_SIZE + 2]);

                        for (typename V::TNodeI NI = Graph->BegNI(); NI < Graph->EndNI(); NI++) {
                            const long NId = NI.GetId();
                            const double v1 = values1[NId];
                            out << NId << "\t" << to_string(v1);
                            if (outfields > 1) {
                                const double v2 = values2[NId];
                                out << "\t" << to_string(v2);
                            }
                            if (dict) {
                                bool resp = dict->getText(NId, supportBuffer.get());
                                if (resp) {
                                    out << "\t" << string(supportBuffer.get());
                                } else {
                                    out << "\tN.A";
                                }
                            }
                            out << endl;
                        }
                    }
                }
                file_out.close();
                BOOST_LOG_TRIVIAL(info) << "Done.";
            }

        static void runTask(string nameTask,
                std::function<void()> &f,
                std::function<int64()> &f_int,
                std::function<double()> &f_double,
                F_RetValue r,
                int64 &retValue_int,
                double &retValue_double) {
            std::chrono::system_clock::time_point start = std::chrono::system_clock::now();
            switch (r) {
                case NONE:
                    f(); break;
                case INT:
                    retValue_int = f_int(); break;
                case DOUBLE:
                    retValue_double = f_double(); break;
            }
            std::chrono::microseconds duration = std::chrono::duration_cast<
                std::chrono::microseconds>(std::chrono::system_clock::now() - start);
            BOOST_LOG_TRIVIAL(info) << "Runtime " << nameTask << ": " << duration.count() / 1000 << " ms. (" << duration.count() << " mu_s)";
            switch (r) {
                case INT:
                    BOOST_LOG_TRIVIAL(info) << "Output " << nameTask << ": " << retValue_int;
                    break;
                case DOUBLE:
                    BOOST_LOG_TRIVIAL(info) << "Output " << nameTask << ": " << retValue_double;
                    break;
                default:
                    break;
            }
        }

        //Run operation
        template<class K, class V>
            static void runTask(KB &kb,
                    string nameTask,
                    string outputfile,
                    string params) {
                AnalyticsTasks &tasks = AnalyticsTasks::getInstance();
                tasks.load(nameTask, params);
                AnalyticsTasks::Task task = tasks.getTask(nameTask);
                //Check if the graph comply with the requirements of the task

                BOOST_LOG_TRIVIAL(info) << "Loading the graph ...";
                K Graph = new V(&kb);

                //Possible outputs
                std::vector<float> values1;
                std::vector<float> values2;
                int nargs = 1;
                F_RetValue retValue = NONE;

                //Pointers to functions
                std::function<void()> f;
                std::function<int64()> f_int;
                std::function<double()> f_double;

                BOOST_LOG_TRIVIAL(info) << "Run task " << task.tostring();

                if (nameTask == "pagerank") {
                    f = std::bind(TSnap::GetPageRank_stl<K>,
                            std::ref(Graph),
                            std::ref(values1),
                            task.getParam("C").as<double>(),
                            task.getParam("eps").as<double>(),
                            task.getParam("maxiter").as<int>());
                    nargs = 1;

                } else if (nameTask == "hits") {
                    f = std::bind(TSnap::GetHits_stl<K>,
                            std::ref(Graph),
                            std::ref(values1),
                            std::ref(values2),
                            task.getParam("maxiter").as<int>());
                    nargs = 2;

                } else if (nameTask == "betcentr") {
                    auto fp = static_cast<void (*)(const K& , std::vector<float>& ,
                            const bool& ,
                            const double&)>(&TSnap::GetBetweennessCentr_stl<K>);
                    f = std::bind(fp,
                            std::ref(Graph),
                            std::ref(values1),
                            true,
                            task.getParam("nodefrac").as<double>());
                    nargs = 1;

                } else if (nameTask == "avg_clustcoef") {
                    auto fp = static_cast<double (*)(const K& ,
                            long)>(&TSnap::GetClustCf<K>);
                    f_double = std::bind(fp,
                            std::ref(Graph),
                            task.getParam("samplen").as<long>());
                    nargs = 0;
                    retValue = DOUBLE;

                } else if (nameTask == "clustcoef") {
                    auto fp = static_cast<void (*)(const K&,
                            std::vector<float>&)>(&NativeTasks::clustcoef<K>);
                            //std::vector<float>&)>(&TSnap::GetNodeClustCf<K>);
                    f = std::bind(fp,
                            std::ref(Graph),
                            std::ref(values1));
                    nargs = 1;

                } else if (nameTask == "triads") {
                    auto fp = static_cast<int64 (*)(const K& ,
                            long)>(&TSnap::GetTriads<K>);
                    f_int = std::bind(fp,
                            std::ref(Graph),
                            task.getParam("samplen").as<long>());
                    nargs = 0;
                    retValue = INT;

                } else if (nameTask == "triangles") {
                    auto fp = static_cast<int64 (*)(const K&)>(&TSnap::CountTriangles<K>);
                    f_int = std::bind(fp,
                            std::ref(Graph));
                    nargs = 0;
                    retValue = INT;

                } else if (nameTask == "bfs") {
                    auto fp = static_cast<long (*)(const K&, const long, const long)>(&NativeTasks::getShortPath);
                    f_int = std::bind(fp,
                            std::ref(Graph),
                            task.getParam("src").as<long>(),
                            task.getParam("dst").as<long>());
                    nargs = 0;
                    retValue = INT;

                } else {
                    BOOST_LOG_TRIVIAL(error) << "Task " << nameTask << " is not known!";
                    throw 10;
                }

                /**** RUN SNAP FUNCTIONS ****/
                int64 retValue_int;
                double retValue_double;
                runTask(nameTask, f, f_int, f_double,
                        retValue, retValue_int,
                        retValue_double);

                /**** SAVE THE RESULTS TO A FILE ****/
                if (outputfile != "") {
                    saveToFile<K,V>(Graph, retValue, retValue_int, retValue_double,
                            nargs, values1, values2,
                            outputfile);
                }
            }

    public:

        //Main method to invoke
        static void run(KB &kb,
                string nameTask,
                string outputfile,
                string params) {

            //Check what type of graph is stored in the KB
            if (kb.getGraphType() == GraphType::DIRECTED) {
                Analytics::runTask<PTrident_TNGraph, Trident_TNGraph>(kb, nameTask, outputfile, params);
            } else {
                //Graph should be undirected
                if ((kb.getGraphType() != GraphType::UNDIRECTED)) {
                    BOOST_LOG_TRIVIAL(error) << "Graph analytical operations work only on simple directed or simple undirected graphs";
                    throw 10;
                }
                Analytics::runTask<PTrident_UTNGraph, Trident_UTNGraph>(kb, nameTask, outputfile, params);
            }

        }
};

#endif
