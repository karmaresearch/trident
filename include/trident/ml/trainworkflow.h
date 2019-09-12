#ifndef _TRAINWORKFLOW_H
#define _TRAINWORKFLOW_H

#include <trident/ml/learner.h>
#include <trident/utils/parallel.h>

template<typename Learner, typename Tester>
class TrainWorkflow {
    private:
        KB &kb;
        BatchCreator &batcher;
        Learner &tr;
        const uint32_t epochs;

        void batch_processer(
                Querier *q,
                ConcurrentQueue<std::shared_ptr<BatchIO>> *inputQueue,
                ConcurrentQueue<std::shared_ptr<BatchIO>> *outputQueue,
                ThreadOutput *output,
                uint32_t epoch) {
            std::shared_ptr<BatchIO> pio;
            uint64_t nbatches = 0;
            while (true) {
                inputQueue->pop_wait(pio);
                if (pio == NULL) {
                    break;
                }
                pio->q = q;
                tr.process_batch(*pio.get(), epoch, nbatches);
                output->violations += pio->violations;
                output->conflicts += pio->conflicts;
                output->loss += pio->loss;
                pio->clear();
                outputQueue->push(pio);
                nbatches += 1;
            }
        }

        void train(const uint16_t nthreads,
                const uint16_t nevalthreads,
                const uint16_t nstorethreads,
                const uint32_t evalits,
                const uint32_t storeits,
                const string pathvalid,
                const string storefolder,
                const bool compresstorage) {

            bool shouldStoreModel = storefolder != "" && evalits > 0;
            double bestresult = std::numeric_limits<double>::max();
            int bestepoch = -1;

            //storefolder should point to a directory. Create it if it does not exist
            if (shouldStoreModel && !Utils::exists(storefolder)) {
                Utils::create_directories(storefolder);
            }

            auto E = tr.getE();
            auto R = tr.getR();

            std::vector<std::unique_ptr<Querier>> queriers;
            for(uint16_t i = 0; i < nthreads; ++i) {
                queriers.push_back(std::unique_ptr<Querier>(kb.query()));
            }

            std::chrono::system_clock::time_point start = std::chrono::system_clock::now();
	    uint64_t oldViolations = std::numeric_limits<uint64_t>::max();
            for (uint32_t epoch = 0; epoch < epochs; ++epoch) {
                std::chrono::time_point<std::chrono::system_clock> start=std::chrono::system_clock::now();
                //Init code
                if (batcher.getFeedback()) {
                    batcher.getFeedback()->setCurrentEpoch(epoch);
                }
                batcher.start();
                uint32_t batchcounter = 0;
                ConcurrentQueue<std::shared_ptr<BatchIO>> inputQueue;
                ConcurrentQueue<std::shared_ptr<BatchIO>> doneQueue;
                std::vector<ThreadOutput> outputs;
                outputs.resize(nthreads);
                for(uint16_t i = 0; i < nthreads; ++i) {
                    doneQueue.push(std::shared_ptr<BatchIO>(
                                new BatchIO(batcher.getBatchSize())));
                }

                //Start nthreads
                std::vector<std::thread> threads;
                for(uint16_t i = 0; i < nthreads; ++i) {
                    Querier *q = queriers[i].get();
                    threads.push_back(std::thread(&TrainWorkflow<Learner,Tester>::batch_processer,
                                this, q, &inputQueue, &doneQueue, &outputs[i],
                                epoch));
                }

                //Process all batches
                while (true) {
                    std::shared_ptr<BatchIO> pio;
                    doneQueue.pop_wait(pio);
                    if (batcher.getBatch(pio->field1, pio->field2, pio->field3)) {
                        pio->epoch = epoch;
                        pio->violations = 0;
                        inputQueue.push(pio);
                        batchcounter++;
                        if (batchcounter % 100000 == 0) {
                            LOG(DEBUGL) << "Processed " << batchcounter << " batches";
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
                double totalLoss = 0.0;
                for(uint16_t i = 0; i < nthreads; ++i) {
                    threads[i].join();
                    totalV += outputs[i].violations;
                    totalC += outputs[i].conflicts;
                    totalLoss += outputs[i].loss;
                }

                E->postprocessUpdates();
                R->postprocessUpdates();

                std::chrono::duration<double> elapsed_seconds = std::chrono::system_clock::now() - start;
                string sviol = " Loss=" + to_string(totalLoss);
                if (tr.generateViolations()) {
                    sviol = " Violations=" + to_string(totalV);
		    if (totalV < oldViolations) {
			oldViolations = totalV;
			sviol += '*';
		    }
                }

                if (nthreads > 1) {
                    LOG(INFOL) << "Epoch " << epoch << ". Time=" <<
                        elapsed_seconds.count() << "sec." << " Conflicts=" << totalC << sviol;
                } else {
                    LOG(INFOL) << "Epoch " << epoch << ". Time=" <<
                        elapsed_seconds.count() << "sec." << sviol;
                }

                // TODO: print gradients here.

                if (shouldStoreModel && (epoch + 1) % storeits == 0) {
                    string pathmodel = storefolder + "/model-" + to_string(epoch+1);
                    LOG(INFOL) << "Storing the model into " << pathmodel << " with " << nstorethreads << " threads";
                    tr.store_model(pathmodel, compresstorage, nstorethreads);
                }

                if ((epoch+1) % evalits == 0) {
                    if (Utils::exists(pathvalid)) {
                        //Load the loading data into a vector
                        std::vector<uint64_t> testset;
                        BatchCreator::loadTriples(pathvalid, testset);
                        //Do the test
                        Tester tester(E, R);
                        LOG(DEBUGL) << "Testing on the valid dataset ...";
                        auto result = tester.test("valid", testset, nevalthreads, epoch);
                        //Store the results of the detailed queries
                        if (shouldStoreModel) {
                            string pathresults = storefolder + "/results-" + to_string(epoch+1);
                            LOG(DEBUGL) << "Storing the results ...";
                            ofstream out;
                            out.open(pathresults);
                            out << "Query\tPosS\tPosO" << endl;
                            for (auto v : result->results) {
                                //Print a line
                                out << to_string(v.s) << " " << to_string(v.p) << " " << to_string(v.o);
                                out << "\t" << to_string(v.posS) << "\t" << to_string(v.posO) << endl;
                            }
                        }
                        if (result->loss < bestresult) {
                            bestresult = result->loss;
                            bestepoch = epoch;
                            LOG(DEBUGL) << "Epoch " << epoch << " got best results";
                            if (shouldStoreModel) {
                                string pathbestmodel = storefolder + "/best-model";
                                //Remove current directory
                                if (Utils::exists(pathbestmodel)) {
                                    Utils::rmlink(pathbestmodel);
                                }
                                string pathmodel = storefolder + "/model-" + to_string(epoch+1);
                                if (Utils::exists(pathmodel)) {
                                    //If the model already exist then link it
                                    Utils::linkdir("model-" + to_string(epoch+1), pathbestmodel);
                                } else {
                                    tr.store_model(pathbestmodel, compresstorage, nstorethreads);
                                }
                            }
                        }
                        if (batcher.getFeedback()) {
                            batcher.getFeedback()->addFeedbacks(result);
                        }
                    } else {
                        LOG(WARNL) << "I'm supposed to test the model but no data is available";
                    }
                }
            }
            std::chrono::duration<double> duration = std::chrono::system_clock::now() - start;
            if (Utils::exists(pathvalid)) {
                LOG(INFOL) << "Best epoch: " << bestepoch << " Accuracy: " << bestresult << " Time(s):" << duration.count();
            } else {
                LOG(INFOL) << "Time(s):" << duration.count();
            }
        }

    public:
        TrainWorkflow(KB &kb, BatchCreator &b, Learner &l, uint32_t epochs) : kb(kb),
        batcher(b), tr(l), epochs(epochs) {}

        static void launchLearning(KB &kb, LearnParams &p) {
            std::unique_ptr<GradTracer> debugger;
            if (p.filetrace != "") {
                debugger = std::unique_ptr<GradTracer>(new GradTracer(p.ne, 1000, p.dim));
                p.gradDebugger = std::move(debugger);
            }
            std::shared_ptr<Feedback> feedback;
            bool filter = p.feedbacks;
            if (p.feedbacks) {
                feedback = std::shared_ptr<Feedback>(new Feedback(p.feedbacks_threshold,
                            p.feedbacks_minfulle));
            }
            LOG(INFOL) << "Launching learning algorithm with params: " << p.tostring();
            BatchCreator batcher(kb.getPath(),
                    p.batchsize,
                    p.nthreads,
                    p.valid,
                    p.test,
                    filter,
                    p.regeneratebatch,
                    feedback);

            Learner tr(kb, p);
            LOG(INFOL) << "Setting up " << tr.getName() << " ...";
            tr.setup(p.nthreads);
            LOG(INFOL) << "Launching the training of " << tr.getName() << " ...";
            TrainWorkflow<Learner, Tester> w(kb, batcher, tr, p.epochs);
            w.train(p.nthreads,
                    p.nevalthreads,
                    p.nstorethreads,
                    p.evalits, p.storeits,
                    batcher.getValidPath(),
                    p.storefolder,
                    p.compresstorage);
            LOG(INFOL) << "Done.";
            tr.getDebugger(debugger);

            if (p.filetrace != "") {
                debugger->store(p.filetrace);
            }
        }
};

#endif
