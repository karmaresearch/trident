#include <trident/ml/learner.h>
#include <trident/ml/transetester.h>
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
/*    ofstream ofs;
    if (compressstorage) {
        path = path + ".gz";
    }
    ofs.open(path, std::ofstream::out);
    boost::iostreams::filtering_stream<boost::iostreams::output> out;
    if (compressstorage) {
        out.push(boost::iostreams::gzip_compressor());
    }
    out.push(ofs);
    out.write((char*)&dim, 4);
    out.write((char*)&nr, 4);
    out.write((char*)&ne, 4);
    auto starr  = R->getPAllEmbeddings();
    auto endr = starr + dim * nr;
    while (starr != endr) {
        out.write((char*)starr, 8);
        starr++;
    }

    //Parallelize the dumping of the entities
    auto data = E->getPAllEmbeddings();
    std::vector<std::thread> threads;
    uint64_t batchsize = ((long)ne * dim) / nthreads;
    uint64_t begin = 0;
    uint16_t idx = 0;
    for(uint16_t i = 0; i < nthreads; ++i) {
        string localpath = path + "." + to_string(idx);
        uint64_t end = begin + batchsize;
        if (end > ((long)ne * dim) || i == nthreads - 1) {
            end = (long)ne * dim;
        }
        BOOST_LOG_TRIVIAL(debug) << "Storing " << (end - begin) << " values in " << localpath << " ...";
        if (begin < end) {
            threads.push_back(std::thread(_store_entities,
                        localpath, compressstorage,
                        data + begin, data + end));
            idx++;
        }
        begin = end;
    }
    for(uint16_t i = 0; i < threads.size(); ++i) {
        threads[i].join();
    }*/

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

    for (uint16_t epoch = 0; epoch < epochs; ++epoch) {
        std::chrono::time_point<std::chrono::system_clock> start=std::chrono::system_clock::now();
        //Init code
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
                if (result < bestresult) {
                    bestresult = result;
                    bestepoch = epoch;
                    BOOST_LOG_TRIVIAL(debug) << "Epoch " << epoch << " got best results";
                }
            }
        }
    }
    BOOST_LOG_TRIVIAL(info) << "Best epoch: " << bestepoch << " Accuracy: " << bestresult;
}

std::pair<std::shared_ptr<Embeddings<double>>,std::shared_ptr<Embeddings<double>>>
Learner::loadModel(string path) {
    ifstream ifs;
    ifs.open(path);
    boost::iostreams::filtering_stream<boost::iostreams::input> inp;
    inp.push(boost::iostreams::gzip_decompressor());
    inp.push(ifs);
    boost::archive::text_iarchive ia(inp);
    uint16_t dim;
    ia >> dim;
    uint32_t nr;
    ia >> nr;
    std::vector<double> emb_r;
    ia >> emb_r;
    uint32_t ne;
    ia >> ne;
    //Load R
    std::shared_ptr<Embeddings<double>> R = std::shared_ptr<Embeddings<double>>(
            new Embeddings<double>(nr, dim, emb_r)
            );

    std::vector<double> emb_e;
    emb_e.resize(ne * dim);
    fs::path bpath(path);
    string dirname = bpath.parent_path().string();
    std::vector<string> files_e = Utils::getFilesWithPrefix(
            dirname,
            bpath.filename().string() + ".");

    //Load the files one by one
    uint32_t idxe = 0;
    uint16_t processedfiles = 0;
    while (processedfiles < files_e.size()) {
        string file = dirname + "/" + bpath.filename().string() + "." + to_string(processedfiles);
        BOOST_LOG_TRIVIAL(debug) << "Processing file " << file;
        ifstream ifs2;
        ifs2.open(file);
        boost::iostreams::filtering_stream<boost::iostreams::input> inp2;
        inp2.push(boost::iostreams::gzip_decompressor());
        inp2.push(ifs2);
        boost::archive::text_iarchive ia(inp2);
        std::vector<double> values;
        ia >> values;
        //Copy the values into emb_e
        for(size_t i = 0; i < values.size(); ++i) {
            emb_e[idxe++] = values[i];
        }
        processedfiles++;
    }

    //Load E
    std::shared_ptr<Embeddings<double>> E = std::shared_ptr<Embeddings<double>>(
            new Embeddings<double>(ne,dim,emb_e)
            );

    return std::make_pair(E,R);
}
