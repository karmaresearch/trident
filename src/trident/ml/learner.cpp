#include <trident/ml/learner.h>
#include <trident/ml/transe.h>
#include <trident/ml/distmul.h>
#include <trident/ml/transetester.h>
#include <trident/ml/feedback.h>
#include <kognac/utils.h>

#include <tbb/concurrent_queue.h>

#include <boost/filesystem/operations.hpp>
#include <boost/filesystem/path.hpp>

#include <iostream>
#include <chrono>
#include <cmath>

namespace fs = boost::filesystem;

using namespace std;

void Learner::setup(const uint16_t nthreads,
        std::shared_ptr<Embeddings<double>> E,
        std::shared_ptr<Embeddings<double>> R,
        std::unique_ptr<double> pe,
        std::unique_ptr<double> pr) {
    this->E = E;
    this->R = R;
    this->pe2 = std::move(pe);
    this->pr2 = std::move(pr);
}

void Learner::setup(const uint16_t nthreads) {
    BOOST_LOG_TRIVIAL(debug) << "Creating E ...";
    std::shared_ptr<Embeddings<double>> E = std::shared_ptr<Embeddings<double>>(new Embeddings<double>(ne, dim));
    //Initialize it
    E->init(nthreads, true);
    BOOST_LOG_TRIVIAL(debug) << "Creating R ...";
    std::shared_ptr<Embeddings<double>> R = std::shared_ptr<Embeddings<double>>(new Embeddings<double>(nr, dim));
    R->init(nthreads, false);

    std::unique_ptr<double> lpe2;
    std::unique_ptr<double> lpr2;
    if (adagrad) {
        lpe2 = std::unique_ptr<double>(new double[(uint64_t)dim * ne]);
        lpr2 = std::unique_ptr<double>(new double[(uint64_t)dim * nr]);
        //Init to zero
        memset(lpe2.get(), 0, sizeof(double) * (uint64_t)dim * ne);
        memset(lpr2.get(), 0, sizeof(double) * (uint64_t)dim * nr);
    }
    setup(nthreads, E, R, std::move(lpe2), std::move(lpr2));
}

float Learner::dist_l1(double* head, double* rel, double* tail,
        float *matrix) {
    float result = 0.0;
    for (uint16_t i = 0; i < dim; ++i) {
        auto value = head[i] + rel[i] - tail[i];
        matrix[i] = value;
        result += abs(value);
    }
    return result;
}

void Learner::batch_processer(
        Querier *q,
        tbb::concurrent_bounded_queue<std::shared_ptr<BatchIO>> *inputQueue,
        tbb::concurrent_bounded_queue<std::shared_ptr<BatchIO>> *outputQueue,
        ThreadOutput *output,
        uint16_t epoch) {
    std::shared_ptr<BatchIO> pio;
    uint16_t nbatches = 0;
    while (true) {
        inputQueue->pop(pio);
        if (pio == NULL) {
            break;
        }
        pio->q = q;
        process_batch(*pio.get(), epoch, nbatches);
        output->violations += pio->violations;
        output->conflicts += pio->conflicts;
        pio->clear();
        outputQueue->push(pio);
        nbatches += 1;
    }
}

void Learner::store_model(string path,
        const bool compressstorage,
        const uint16_t nthreads) {
    BOOST_LOG_TRIVIAL(debug) << "Start serialization ...";
    fs::create_directories(path);
    BOOST_LOG_TRIVIAL(debug) << "Serializing R ...";
    R->store(path + "/R", compressstorage, nthreads);
    BOOST_LOG_TRIVIAL(debug) << "Serializing E ...";
    E->store(path + "/E", compressstorage, nthreads);
    BOOST_LOG_TRIVIAL(debug) << "Serialization done";
}

void Learner::train(BatchCreator &batcher, const uint16_t nthreads,
        const uint16_t nstorethreads,
        const uint32_t evalits, const uint32_t storeits, string pathvalid,
        const string storefolder,
        const bool compresstorage) {

    bool shouldStoreModel = storefolder != "" && evalits > 0;
    double bestresult = std::numeric_limits<double>::max();
    int bestepoch = -1;

    //storefolder should point to a directory. Create it if it does not exist
    if (shouldStoreModel && !fs::exists(fs::path(storefolder))) {
        fs::create_directories(storefolder);
    }

    std::vector<std::unique_ptr<Querier>> queriers;
    for(uint16_t i = 0; i < nthreads; ++i) {
        queriers.push_back(std::unique_ptr<Querier>(kb.query()));
    }

    timens::system_clock::time_point start = timens::system_clock::now();
    for (uint16_t epoch = 0; epoch < epochs; ++epoch) {
        std::chrono::time_point<std::chrono::system_clock> start=std::chrono::system_clock::now();
        //Init code
        if (batcher.getFeedback()) {
            batcher.getFeedback()->setCurrentEpoch(epoch);
        }
        batcher.start();
        uint32_t batchcounter = 0;
        tbb::concurrent_bounded_queue<std::shared_ptr<BatchIO>> inputQueue;
        tbb::concurrent_bounded_queue<std::shared_ptr<BatchIO>> doneQueue;
        std::vector<ThreadOutput> outputs;
        outputs.resize(nthreads);
        for(uint16_t i = 0; i < nthreads; ++i) {
            doneQueue.push(std::shared_ptr<BatchIO>(new BatchIO(batchsize, dim)));
        }

        //Start nthreads
        std::vector<std::thread> threads;
        for(uint16_t i = 0; i < nthreads; ++i) {
            Querier *q = queriers[i].get();
            threads.push_back(std::thread(&Learner::batch_processer,
                        this, q, &inputQueue, &doneQueue, &outputs[i],
                        epoch));
        }

        //Process all batches
        while (true) {
            std::shared_ptr<BatchIO> pio;
            doneQueue.pop(pio);
            if (batcher.getBatch(pio->field1, pio->field2, pio->field3)) {
                pio->epoch = epoch;
                pio->violations = 0;
                inputQueue.push(pio);
                batchcounter++;
                if (batchcounter % 100000 == 0) {
                    BOOST_LOG_TRIVIAL(debug) << "Processed " << batchcounter << " batches";
                }
            } else {
                //Puts nthread NULL pointers to tell the threads to stop
                for(uint16_t i = 0; i < nthreads; ++i) {
                    inputQueue.push(std::shared_ptr<BatchIO>());
                }
                break;
            }
        }

        //Wait until all threads are finished
        uint64_t totalV = 0;
        uint64_t totalC = 0;
        for(uint16_t i = 0; i < nthreads; ++i) {
            threads[i].join();
            totalV += outputs[i].violations;
            totalC += outputs[i].conflicts;
        }

        E->postprocessUpdates();
        R->postprocessUpdates();

        std::chrono::duration<double> elapsed_seconds = std::chrono::system_clock::now() - start;
        BOOST_LOG_TRIVIAL(info) << "Epoch " << epoch << ". Time=" <<
            elapsed_seconds.count() << "sec. Violations=" << totalV <<
            " Conflicts=" << totalC;

        if (shouldStoreModel && (epoch + 1) % storeits == 0) {
            string pathmodel = storefolder + "/model-" + to_string(epoch+1);
            BOOST_LOG_TRIVIAL(info) << "Storing the model into " << pathmodel << " with " << nstorethreads << " threads";
            store_model(pathmodel, compresstorage, nstorethreads);
        }

        if ((epoch+1) % evalits == 0) {
            if (fs::exists(pathvalid)) {
                //Load the loading data into a vector
                std::vector<uint64_t> testset;
                BatchCreator::loadTriples(pathvalid, testset);
                //Do the test
                TranseTester<double> tester(E, R);
                BOOST_LOG_TRIVIAL(debug) << "Testing on the valid dataset ...";
                auto result = tester.test("valid", testset, nthreads, epoch);
                if (result->loss < bestresult) {
                    bestresult = result->loss;
                    bestepoch = epoch;
                    BOOST_LOG_TRIVIAL(debug) << "Epoch " << epoch << " got best results";
                }
                if (batcher.getFeedback()) {
                    batcher.getFeedback()->addFeedbacks(result);
                }
                //Store the results of the detailed queries
                if (shouldStoreModel) {
                    string pathresults = storefolder + "/results-" + to_string(epoch+1);
                    BOOST_LOG_TRIVIAL(debug) << "Storing the results ...";
                    ofstream out;
                    out.open(pathresults);
                    out << "Query\tPosS\tPosO" << endl;
                    for (auto v : result->results) {
                        //Print a line
                        out << to_string(v.s) << " " << to_string(v.p) << " " << to_string(v.o);
                        out << "\t" << to_string(v.posS) << "\t" << to_string(v.posO) << endl;
                    }
                }
            } else {
                BOOST_LOG_TRIVIAL(warning) << "I'm supposed to test the model but no data is available";
            }
        }
    }
    boost::chrono::duration<double> duration = boost::chrono::system_clock::now() - start;
    BOOST_LOG_TRIVIAL(info) << "Best epoch: " << bestepoch << " Accuracy: " << bestresult << " Time(s):" << duration.count();
}

void Learner::update_gradients(BatchIO &io,
        std::vector<EntityGradient> &ge,
        std::vector<EntityGradient> &gr) {
    //Update the gradients of the entities and relations
    if (adagrad) {
        for(auto &i : ge) {
            double *emb = E->get(i.id);

            if (gradDebugger) {
                gradDebugger->add(io.epoch, i.id, i.dimensions, i.n);
            }
            if (E->isLocked(i.id)) {
                io.conflicts++;
                E->incrConflict(i.id);
            }
            E->lock(i.id);
            E->incrUpdates(i.id);

            double *pent = pe2.get() + i.id * dim;
            double sum = 0.0; //used for normalization
            for(uint16_t j = 0; j < dim; ++j) {
                const double g = (double)i.dimensions[j] / i.n;
                pent[j] += g * g;
                double spent = sqrt(pent[j]);
                double maxv = max(spent, (double)1e-7);
                emb[j] -= learningrate * g / maxv;
                sum += emb[j] * emb[j];
            }
            //normalization step
            sum = sqrt(sum);
            for(uint16_t j = 0; j < dim; ++j) {
                emb[j] = emb[j] / sum;
            }

            E->unlock(i.id);
        }
        for(auto &i : gr) {
            double *emb = R->get(i.id);

            if (R->isLocked(i.id)) {
                io.conflicts++;
                R->incrConflict(i.id);
            }
            R->lock(i.id);
            R->incrUpdates(i.id);

            double *pr = pr2.get() + i.id * dim;
            for(uint16_t j = 0; j < dim; ++j) {
                const double g = (double)i.dimensions[j] / i.n;
                pr[j] += g * g;
                double maxv = max(sqrt(pr[j]), (double)1e-7);
                emb[j] -= learningrate * g / maxv;
            }

            R->unlock(i.id);
        }
    } else { //sgd
        for (auto &i : ge) {
            double *emb = E->get(i.id);
            auto n = i.n;
            if (n > 0) {
                double sum = 0.0; //used for normalization
                for(uint16_t j = 0; j < dim; ++j) {
                    emb[j] -= learningrate * i.dimensions[j] / n;
                    sum += emb[j] * emb[j];
                }
                sum = sqrt(sum);
                //normalization step
                for(uint16_t j = 0; j < dim; ++j) {
                    emb[j] = emb[j] / sum;
                }
            }
        }
        for (auto &i : gr) {
            double *emb = R->get(i.id);
            auto n = i.n;
            if (n > 0) {
                for(uint16_t j = 0; j < dim; ++j) {
                    emb[j] -= learningrate * i.dimensions[j] / n;
                }
            }
        }
    }
}

void Learner::launchLearning(KB &kb, string op, LearnParams &p) {
    std::unique_ptr<GradTracer> debugger;
    if (p.filetrace != "") {
        debugger = std::unique_ptr<GradTracer>(new GradTracer(p.ne, 1000, p.dim));
        p.gradDebugger = std::move(debugger);
    }
    std::shared_ptr<Feedback> feedback;
    if (p.feedback) {
        feedback = std::shared_ptr<Feedback>(new Feedback());
    }
    bool filter = true;
    BOOST_LOG_TRIVIAL(debug) << "Launching " << op << " with params: " << p.tostring();
    BatchCreator batcher(kb.getPath(),
            p.batchsize,
            p.nthreads,
            p.valid,
            p.test,
            filter,
            feedback);
    if (op == "transe") {
        TranseLearner tr(kb, p);
        BOOST_LOG_TRIVIAL(info) << "Setting up TranSE ...";
        tr.setup(p.nthreads);
        BOOST_LOG_TRIVIAL(info) << "Launching the training of TranSE ...";
        tr.train(batcher, p.nthreads,
                p.nstorethreads,
                p.evalits, p.storeits,
                batcher.getValidPath(),
                p.storefolder,
                p.compressstorage);
        BOOST_LOG_TRIVIAL(info) << "Done.";
        tr.getDebugger(debugger);

    } else if (op == "distmul") {
        DistMulLearner tr(kb, p);
        BOOST_LOG_TRIVIAL(info) << "Setting up DistMul...";
        tr.setup(p.nthreads);
        BOOST_LOG_TRIVIAL(info) << "Launching the training of DistMul...";
        tr.train(batcher, p.nthreads,
                p.nstorethreads,
                p.evalits, p.storeits,
                batcher.getValidPath(),
                p.storefolder,
                p.compressstorage);
        BOOST_LOG_TRIVIAL(info) << "Done.";
        tr.getDebugger(debugger);

    } else {
        BOOST_LOG_TRIVIAL(error) << "Task " << op << " not recognized";
        return;
    }
    //BOOST_LOG_TRIVIAL(debug) << "Launching DistMul with epochs=" << epochs << " dim=" << dim << " ne=" << ne << " nr=" << nr << " learningrate=" << learningrate << " batchsize=" << batchsize << " evalits=" << evalits << " storefolder=" << storefolder << " nthreads=" << nthreads << " nstorethreads=" << nstorethreads << " adagrad=" << adagrad << " compress=" << compresstorage;

    if (p.filetrace != "") {
        debugger->store(p.filetrace);
    }

}
