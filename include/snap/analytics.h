#ifndef _ANALYTICS_H
#define _ANALYTICS_H

#include <snap-core/Snap.h>
#include <snap/nativetasks.h>
#include <snap/tasks.h>
#include <snap/directed.h>
#include <snap/undirected.h>

#include <trident/kb/kb.h>
#include <trident/kb/querier.h>
#include <trident/kb/dictmgmt.h>
#include <trident/utils/tridentutils.h>

#include <kognac/logs.h>

#include <boost/iostreams/filtering_stream.hpp>
#include <boost/iostreams/filter/gzip.hpp>

#include <functional>

typedef enum _RetValue { NORETURN, DOUBLE, INT, V_LONG } F_RetValue;

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
                LOG(INFO) << "Saving the results on " << outputfile << " ...";
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
                LOG(INFO) << "Done.";
            }

        static void runTask(string nameTask,
                std::function<void()> &f,
                std::function<int64()> &f_int,
                std::function<double()> &f_double,
                std::function<std::vector<long>()> &f_vlong,
                F_RetValue r,
                int64 &retValue_int,
                double &retValue_double,
                std::vector<long> &retValue_vlong) {
            std::chrono::system_clock::time_point start = std::chrono::system_clock::now();
            switch (r) {
                case NORETURN:
                    f(); break;
                case INT:
                    retValue_int = f_int(); break;
                case DOUBLE:
                    retValue_double = f_double(); break;
                case V_LONG:
                    retValue_vlong = f_vlong(); break;
            }
            std::chrono::microseconds duration = std::chrono::duration_cast<
                std::chrono::microseconds>(std::chrono::system_clock::now() - start);
            LOG(INFO) << "Runtime " << nameTask << ": " << duration.count() / 1000 << " ms. (" << duration.count() << " mu_s)";
            string els = "";
            int i = 0;
            switch (r) {
                case INT:
                    LOG(INFO) << "Output " << nameTask << ": " << retValue_int;
                    break;
                case DOUBLE:
                    LOG(INFO) << "Output " << nameTask << ": " << retValue_double;
                    break;
                case V_LONG:
                    i = 0;
                    for (auto el : retValue_vlong) {
                        if (i == 16) {
                            els += " ...";
                            break;
                        }
                        els += to_string(el) + " ";
                        i++;
                    }
                    LOG(INFO) << "Output " << nameTask << ": (" << retValue_vlong.size() << ") " << els;
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

                LOG(INFO) << "Loading the graph ...";
                K Graph = new V(&kb);

                //Possible inputs
                std::vector<long> inputv;
                std::vector<std::pair<long,long>> inputp;

                //Possible outputs
                std::vector<float> values1;
                std::vector<float> values1_i;
                std::vector<float> values2;
                int nargs = 1;
                F_RetValue retValue = NORETURN;

                //Pointers to functions
                std::function<void()> f;
                std::function<int64()> f_int;
                std::function<double()> f_double;
                std::function<std::vector<long>()> f_vlong;

                LOG(INFO) << "Run task " << task.tostring();

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
                    string pairs = task.getParam("pairs").as<string>();
                    if (pairs != "") {
                        TridentUtils::loadPairFromFile(pairs, inputp, '\t');
                        f_vlong = std::bind(NativeTasks::getShortPath2<K>,
                                std::ref(Graph),
                                std::ref(inputp));
                        nargs = 0;
                        retValue = V_LONG;
                    } else {
                        auto fp = static_cast<long (*)(const K&, const long, const long)>(&NativeTasks::getShortPath);
                        f_int = std::bind(fp,
                                std::ref(Graph),
                                task.getParam("src").as<long>(),
                                task.getParam("dst").as<long>());
                        nargs = 0;
                        retValue = INT;
                    }

                } else if (nameTask == "diameter") {
                    auto fp = static_cast<int64 (*)(const K&, const int&, const bool&)>(&NativeTasks::GetDiam<K>);
                    f_int = std::bind(fp,
                            std::ref(Graph),
                            task.getParam("testnodes").as<int>(),
                            true);
                    nargs = 0;
                    retValue = INT;
                } else if (nameTask == "mod") {
                    TridentUtils::loadFromFile(task.getParam("nodes").as<string>(),
                            inputv);
                    auto fp = static_cast<double (*)(const K&,
                            const std::vector<long>&,
                            long)>(&NativeTasks::GetMod<K>);
                    f_double = std::bind(fp,
                            std::ref(Graph),
                            std::ref(inputv),
                            Graph->GetEdges());
                    nargs = 0;
                    retValue = DOUBLE;

                } else if (nameTask == "maxwcc") {
                    f_double = std::bind(TSnap::GetMxWccSz<K>,
                            std::ref(Graph));
                    nargs = 0;
                    retValue = DOUBLE;

                } else if (nameTask == "maxscc") {
                    f_double = std::bind(TSnap::GetMxSccSz<K>,
                            std::ref(Graph));
                    nargs = 0;
                    retValue = DOUBLE;

                } else if (nameTask == "rw") {
                    if (task.getParam("nodes").as<string>() != "") {
                        TridentUtils::loadFromFile(task.getParam("nodes").as<string>(),
                                inputv);
                        f_vlong = std::bind(TSnap::randomWalk2<K>,
                                std::ref(Graph),
                                std::ref(inputv),
                                task.getParam("len").as<long>());
                        nargs = 0;
                        retValue = V_LONG;
                    } else {
                        f_vlong = std::bind(TSnap::randomWalk<K>,
                                std::ref(Graph),
                                task.getParam("node").as<long>(),
                                task.getParam("len").as<long>());
                        nargs = 0;
                        retValue = V_LONG;
                    }

                } else {
                    LOG(ERROR) << "Task " << nameTask << " is not known!";
                    throw 10;
                }

                /**** RUN SNAP FUNCTIONS ****/
                int64 retValue_int;
                double retValue_double;
                std::vector<long> retValue_vlong;
                runTask(nameTask, f, f_int, f_double, f_vlong,
                        retValue, retValue_int,
                        retValue_double,
                        retValue_vlong);

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
                    LOG(ERROR) << "Graph analytical operations work only on simple directed or simple undirected graphs";
                    throw 10;
                }
                Analytics::runTask<PTrident_UTNGraph, Trident_UTNGraph>(kb, nameTask, outputfile, params);
            }

        }
};

#endif
