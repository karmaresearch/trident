#ifndef _TESTER_H
#define _TESTER_H

#include <trident/ml/embeddings.h>
#include <trident/kb/kb.h>
#include <trident/kb/querier.h>
#include <trident/utils/json.h>

#include <kognac/logs.h>

#include <cstdint>
#include <vector>
#include <iostream>
#include <string>
#include <sstream>
#include <unordered_map>

using namespace std;


template<typename K>
class Tester {
    protected:
        std::shared_ptr<Embeddings<K>> E;
        std::shared_ptr<Embeddings<K>> R;
        Querier* q;


        struct _OutputTest {
            uint64_t positionsO = 0;
            uint64_t positionsS = 0;
            uint64_t hit10O = 0;
            uint64_t hit10S = 0;
            uint64_t hit3S = 0;
            uint64_t hit3O = 0;
        };

        struct ResSingleQuery {
            uint32_t posO;
            uint32_t posS;
            uint64_t s,p,o;
        };

        void test_seq(std::vector<uint64_t> &testset, size_t start,
                size_t end, _OutputTest *out,
                std::vector<ResSingleQuery> &resultsPerQuery) {

            //Support variables
            std::vector<double> scores;
            const uint64_t ne = E->getN();
            scores.resize(ne);
            const uint16_t dime = E->getDim();
            const uint16_t dimr = R->getDim();
            std::vector<K> testArray(ne);
            double *test = testArray.data();
            std::vector<std::size_t> indices(ne);
            std::iota(indices.begin(), indices.end(), 0u);
            std::vector<std::size_t> indices2(ne);
            std::iota(indices2.begin(), indices2.end(), 0u);

            //Output statistics
            uint64_t positionsO = 0;
            uint64_t positionsS = 0;
            uint64_t hit10O = 0;
            uint64_t hit10S = 0;
            uint64_t hit3S = 0;
            uint64_t hit3O = 0;

            int64_t counter = 0;
            int stepperc = 10;
            int64_t sizeinput = end - start;
            while (start != end)  {
                //Get an input triple to test
                uint64_t s = testset[start];
                uint64_t p = testset[start+1];
                uint64_t o = testset[start+2];

                start += 3;

                //Test objects
                predictO(s, dime, p, dimr, test);
                for(uint64_t idx = 0; idx < ne; ++idx) {
                    scores[idx] = closeness(test, idx, dime);
                }
                const uint64_t posO = getPos(ne, scores, indices, indices2, o) + 1;
                positionsO += posO;
                hit10O += posO <= 10;
                hit3O += posO <= 3;


                //Test subjects
                predictS(test, p, dimr, o, dime);
                for(uint64_t idx = 0; idx < ne; ++idx) {
                    scores[idx] = closeness(test, idx, dime);
                }
                const uint64_t posS = getPos(ne, scores, indices, indices2, s) + 1;
                positionsS += posS;
                hit10S += posS <= 10;
                hit3S += posS <= 3;

                //Track progress
                counter++;
                float perc = (float)(3*counter) / sizeinput * 100;
                if (perc > stepperc) {
                    LOG(DEBUGL) << "***Processed " << perc << "\% testcases***";
                    stepperc += 10;
                }

                //Store the results per single query
                ResSingleQuery res;
                res.s = s;
                res.p = p;
                res.o = o;
                res.posO = posO;
                res.posS = posS;
                resultsPerQuery.push_back(res);
            }

            out->positionsO = positionsO;
            out->positionsS = positionsS;
            out->hit10O = hit10O;
            out->hit10S = hit10S;
            out->hit3O = hit3O;
            out->hit3S = hit3S;
        }

    public:
        Tester(std::shared_ptr<Embeddings<K>> E,
                std::shared_ptr<Embeddings<K>> R) {
            this->E = E;
            this->R = R;
        }

        Tester(std::shared_ptr<Embeddings<K>> E,
               std::shared_ptr<Embeddings<K>> R,
               Querier *q) {
            this->E = E;
            this->R = R;
            this->q = q;
        }

        struct OutputTest {
            double loss;
            std::vector<ResSingleQuery> results;
        };

        static uint64_t getPos(const uint64_t ne, const std::vector<double> &scores,
                std::vector<size_t> &indices,
                std::vector<size_t> &indices2,
                const uint64_t pos) {

            std::sort(indices.begin(), indices.end(), [&](size_t lhs, size_t rhs) {
                    return scores[lhs] < scores[rhs];
                    });
            std::sort(indices2.begin(), indices2.end(), [&](size_t lhs, size_t rhs) {
                    return indices[lhs] < indices[rhs];
                    });
            return indices2[pos];
        }

        virtual double closeness(K *v1, uint64_t entity, uint16_t dim) = 0;

        virtual void predictO(uint64_t sub, uint16_t dims, uint64_t pred, uint16_t dimp, K* o) = 0;

        virtual void predictS(K *s, uint64_t pred, uint16_t dimp, uint64_t obj, uint16_t dimo) = 0;

        std::shared_ptr<OutputTest> test(string nameTestset,
                std::vector<uint64_t> &testset,
                const uint16_t nthreads,
                const uint16_t epoch) {
            std::chrono::time_point<std::chrono::system_clock> starttime =std::chrono::system_clock::now();

            std::vector<_OutputTest> outputs;
            std::vector<std::vector<ResSingleQuery>> resQueries;
            resQueries.resize(nthreads);
            outputs.resize(nthreads);
            std::vector<std::thread> threads;
            threads.resize(nthreads);
            uint64_t ntriples = testset.size() / 3;
            uint64_t triplesPerThread = ntriples / nthreads;
            uint64_t nelsPerThread = triplesPerThread * 3;

            LOG(DEBUGL) << "test set size = " << testset.size();
            size_t start = 0;
            for(uint16_t i = 0; i < nthreads; ++i) {
                size_t end = 0;
                if (i == nthreads - 1) {
                    end = testset.size();
                } else {
                    end = start + nelsPerThread;
                }
                threads[i] = std::thread(&Tester::test_seq,
                        this,
                        std::ref(testset),
                        start,
                        end,
                        &outputs[i],
                        std::ref(resQueries[i]));
                start = end;
            }
            for(uint16_t i = 0; i < nthreads; ++i) {
                threads[i].join();
            }
            uint64_t positionsO = 0;
            uint64_t positionsS = 0;
            uint64_t hit10O = 0;
            uint64_t hit10S = 0;
            uint64_t hit3S = 0;
            uint64_t hit3O = 0;
            //Collect the numbers
            for(uint16_t i = 0; i < nthreads; ++i) {
                positionsO += outputs[i].positionsO;
                positionsS += outputs[i].positionsS;
                hit10O += outputs[i].hit10O;
                hit10S += outputs[i].hit10S;
                hit3O += outputs[i].hit3O;
                hit3S += outputs[i].hit3S;
            }

            //return the output statistics
            double avgsubj = (positionsS / (double) ntriples);
            double avgobj = (positionsO / (double) ntriples);
            double totalavg = (avgsubj + avgobj) / 2;
            double avghit10s = (hit10S) / (double) ntriples * 100;
            double avghit10o = (hit10O) / (double) ntriples * 100;
            double avghit10 = (avghit10s + avghit10o) / 2;
            double avghit3s = (hit3S) / (double) ntriples * 100;
            double avghit3o = (hit3O) / (double) ntriples * 100;
            double avghit3 = (avghit3s + avghit3o) / 2;
            std::chrono::duration<double> elapsed_seconds = std::chrono::system_clock::now() - starttime;


            LOG(INFOL) << "Time: " << elapsed_seconds.count() << " sec. Mean subj pos: " << avgsubj << " Mean obj pos: " << avgobj << " Mean pos: " << totalavg;
            LOG(INFOL) << "Hit@10(s): " << avghit10s << "% Hit@10(o): " << avghit10o << "% Hit@10: " << avghit10 << "%";
            LOG(INFOL) << "Hit@3(s): " << avghit3s << "% Hit@3(o): " << avghit3o << "% Hit@3: " << avghit3 << "%";

            //Write JSON output
            JSON jsonresults;
            jsonresults.put("epoch", to_string(epoch + 1));
            jsonresults.put("timesec", elapsed_seconds.count());
            jsonresults.put("sizetestset", testset.size());
            jsonresults.put("nametestset", nameTestset);
            jsonresults.put("meanranks", avgsubj);
            jsonresults.put("meanranko", avgobj);
            jsonresults.put("meanrank", totalavg);
            jsonresults.put("hit10s", avghit10s);
            jsonresults.put("hit10o", avghit10o);
            jsonresults.put("hit10", avghit10);
            jsonresults.put("hit3s", avghit3s);
            jsonresults.put("hit3o", avghit3o);
            jsonresults.put("hit3", avghit3);
            std::stringstream output;
            jsonresults.write(output, jsonresults);
            LOG(DEBUGL) << "JSON: " << output.str();

            std::shared_ptr<OutputTest> results(new OutputTest());
            results->loss = (positionsS + positionsO) / (double)2;
            //Collect all results
            for (auto v : resQueries) {
                std::copy(v.begin(), v.end(), std::back_inserter(results->results));
            }
            return results;
        }
};

struct PredictParams {
    string nametestset;
    uint16_t nthreads;
    string path_modele;
    string path_modelr;
    string binary;

    PredictParams();

    string changeable_tostring();

    string tostring() {
        return changeable_tostring();
    }
};

class Predictor {
    public:
        static void launchPrediction(KB &kb, string algo, PredictParams &p);
};

#endif
