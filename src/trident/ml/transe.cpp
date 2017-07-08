#include <trident/ml/transe.h>
#include <trident/ml/transetester.h>
#include <kognac/utils.h>

#include <tbb/concurrent_queue.h>

#include <boost/filesystem/operations.hpp>
#include <boost/filesystem/path.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <boost/archive/text_iarchive.hpp>
#include <boost/iostreams/filter/gzip.hpp>
#include <boost/iostreams/filtering_stream.hpp>
#include <boost/serialization/vector.hpp>

#include <iostream>
#include <fstream>
#include <chrono>
#include <cmath>

namespace fs = boost::filesystem;

using namespace std;

void Transe::setup(const uint16_t nthreads,
        std::shared_ptr<Embeddings<double>> E,
        std::shared_ptr<Embeddings<double>> R,
        std::unique_ptr<double> pe,
        std::unique_ptr<double> pr) {
    this->E = E;
    this->R = R;
    this->pe2 = std::move(pe);
    this->pr2 = std::move(pr);
}

void Transe::setup(const uint16_t nthreads) {
    BOOST_LOG_TRIVIAL(debug) << "Creating E ...";
    std::shared_ptr<Embeddings<double>> E = std::shared_ptr<Embeddings<double>>(new Embeddings<double>(ne, dim));
    //Initialize it
    E->init(nthreads);
    BOOST_LOG_TRIVIAL(debug) << "Creating R ...";
    std::shared_ptr<Embeddings<double>> R = std::shared_ptr<Embeddings<double>>(new Embeddings<double>(nr, dim));
    R->init(nthreads);

    //Load from disk
    /*ofstream ofs;
      ofs.open("TODO");
      ofs.write((char*)&this->dim, 2);
      ofs.write((char*)&this->ne, 4);
      ofs.write((char*)&this->nr, 4);
      const double *embs = E->getPAllEmbeddings();
      for(uint32_t i = 0; i < this->dim * this->ne; ++i) {
      ofs.write((char*)(embs + i), 8);
      }
      const double *embsr = R->getPAllEmbeddings();
      for(uint32_t i = 0; i < this->dim * this->nr; ++i) {
      ofs.write((char*)(embsr + i), 8);
      }
      ofs.close();
      ifstream ifs;
      ifs.open("TODO");
      char buffer[10];
      ifs.read(buffer, 10);
      std::vector<double> embs;
      for(uint32_t i = 0; i < this->dim * this->ne; ++i) {
      ifs.read(buffer, 8);
      embs.push_back(*(double*)buffer);
      }
      E = std::shared_ptr<Embeddings<double>>(new Embeddings<double>(ne, dim, embs));
      embs.clear();
      for(uint32_t i = 0; i < this->dim * this->nr; ++i) {
      ifs.read(buffer, 8);
      embs.push_back(*(double*)buffer);
      }
      R = std::shared_ptr<Embeddings<double>>(new Embeddings<double>(nr, dim, embs));
      ifs.close();*/

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

float Transe::dist_l1(double* head, double* rel, double* tail,
        float *matrix) {
    float result = 0.0;
    for (uint16_t i = 0; i < dim; ++i) {
        auto value = head[i] + rel[i] - tail[i];
        matrix[i] = value;
        result += abs(value);
    }
    return result;
}

void Transe::gen_random(
        BatchIO &io,
        std::vector<uint64_t> &input,
        const bool subjObjs,
        const uint16_t ntries) {
    for(uint32_t i = 0; i < input.size(); ++i) {
        long s, p, o;
        s = io.field1[i];
        p = io.field2[i];
        o = io.field3[i];
        uint16_t attemptId;
        for(attemptId = 0; attemptId < ntries; ++attemptId) {
            input[i] = dis(gen);
            //Check if the resulting triple is existing ...
            if (subjObjs) {
                s = input[i];
            } else {
                o = input[i];
            }
            if (q->isEmpty(s, p, o)) {
                break;
            }
        }
    }
}

void Transe::update_gradient_matrix(std::vector<EntityGradient> &gradients,
        std::vector<std::unique_ptr<float>> &signmatrix,
        std::vector<uint32_t> &inputTripleID,
        std::vector<uint64_t> &inputTerms,
        int pos, int neg) {
    uint16_t gidx = 0;
    for(uint16_t i = 0; i < inputTripleID.size(); ++i) {
        uint16_t tripleId = inputTripleID[i];
        const uint64_t t = inputTerms[tripleId];
        while (gradients[gidx].id < t && gidx < gradients.size()) {
            gidx += 1;
        }
        //I must reach the point when the element exists
        assert(gidx < gradients.size() && gradients[gidx].id == t);
        float *signs = signmatrix[tripleId].get();
        for(uint16_t dimidx = 0; dimidx < dim; ++dimidx) {
            if (signs[dimidx] != 0) {
                gradients[gidx].dimensions[dimidx] += signs[dimidx] > 0 ? pos : neg;
            }
        }
        gradients[gidx].n++;
    }
}

void Transe::process_batch(BatchIO &io, const uint16_t epoch, const uint16_t nbatches) {
    //Generate negative samples
    std::vector<uint64_t> oneg;
    std::vector<uint64_t> sneg;
    uint32_t sizebatch = io.field1.size();
    oneg.resize(sizebatch);
    sneg.resize(sizebatch);
    gen_random(io, sneg, true, 10);
    gen_random(io, oneg, false, 10);

    /*ofstream ofs;
    ofs.open("TODO/batch-" + to_string(epoch) + "-" + to_string(nbatches));
    long size = io.field1.size();
    ofs.write((char*)&size, 8);
    for(int i = 0; i < size; ++i) {
        ofs.write((char*)&io.field1[i], 8);
        ofs.write((char*)&io.field2[i], 8);
        ofs.write((char*)&io.field3[i], 8);
        ofs.write((char*)&sneg[i], 8);
        ofs.write((char*)&oneg[i], 8);
    }
    ofs.close();*/

    process_batch(io, oneg, sneg);
}

void Transe::process_batch(BatchIO &io, std::vector<uint64_t> &oneg,
        std::vector<uint64_t> &sneg) {
    std::vector<uint64_t> &output1 = io.field1;
    std::vector<uint64_t> &output2 = io.field2;
    std::vector<uint64_t> &output3 = io.field3;
    std::vector<std::unique_ptr<float>> &posSignMatrix = io.posSignMatrix;
    std::vector<std::unique_ptr<float>> &neg1SignMatrix = io.neg1SignMatrix;
    std::vector<std::unique_ptr<float>> &neg2SignMatrix = io.neg2SignMatrix;
    const uint32_t sizebatch = io.field1.size();

    //Support data structures
    std::vector<EntityGradient> gradientsE; //The gradients for all entities
    std::vector<EntityGradient> gradientsR; //The gradients for all entities

    std::vector<uint32_t> posSubjsUpdate1; //List that contains the positive subjects to update
    std::vector<uint32_t> posObjsUpdate1; //List that contains the positive objects to update
    std::vector<uint32_t> negObjsUpdate1; //List that contains the negative objects to update

    std::vector<uint32_t> posSubjsUpdate2; //List that contains the positive subjects to update
    std::vector<uint32_t> negSubjsUpdate2; //List that contains the negative subjects to update
    std::vector<uint32_t> posObjsUpdate2; //List that contains the positive objects to update

    std::vector<uint32_t> posRels1; //List that contains all relations to update
    std::vector<uint32_t> posRels2; //List that contains all relations to update

    for(uint32_t i = 0; i < sizebatch; ++i) {
        //Get corresponding embeddings
        double* sp = E->get(output1[i]);
        double* pp = R->get(output2[i]);
        double* op = E->get(output3[i]);
        double* on = E->get(oneg[i]);
        double* sn = E->get(sneg[i]);

        //Get the distances
        auto diffp = dist_l1(sp, pp, op, posSignMatrix[i].get());
        auto diff1n = dist_l1(sp, pp, on, neg1SignMatrix[i].get());
        auto diff2n = dist_l1(sn, pp, op, neg2SignMatrix[i].get());

        //Calculate the violations
        if (diffp - diff1n + margin > 0) {
            io.violations += 1;
            posSubjsUpdate1.push_back(i);
            posObjsUpdate1.push_back(i);
            negObjsUpdate1.push_back(i);
            posRels1.push_back(i);
        }
        if (diffp - diff2n + margin > 0) {
            io.violations += 1;
            posObjsUpdate2.push_back(i);
            posSubjsUpdate2.push_back(i);
            negSubjsUpdate2.push_back(i);
            posRels2.push_back(i);
        }
    }

    //Sort the triples in the batch by subjects and objects
    std::sort(posSubjsUpdate1.begin(), posSubjsUpdate1.end(), _TranseSorter(output1));
    std::sort(posSubjsUpdate2.begin(), posSubjsUpdate2.end(), _TranseSorter(output1));
    std::sort(posObjsUpdate1.begin(), posObjsUpdate1.end(), _TranseSorter(output3));
    std::sort(posObjsUpdate2.begin(), posObjsUpdate2.end(), _TranseSorter(output3));
    std::sort(negObjsUpdate1.begin(), negObjsUpdate1.end(), _TranseSorter(oneg));
    std::sort(negSubjsUpdate2.begin(), negSubjsUpdate2.end(), _TranseSorter(sneg));
    std::sort(posRels1.begin(), posRels1.end(), _TranseSorter(output2));
    std::sort(posRels2.begin(), posRels2.end(), _TranseSorter(output2));

    //Get all the terms that appear in the batch
    std::vector<uint64_t> allterms;
    for(auto idx : posSubjsUpdate1) {
        uint64_t t = output1[idx];
        allterms.push_back(t);
    }
    for(auto idx : posObjsUpdate1) {
        uint64_t t = output3[idx];
        allterms.push_back(t);
    }
    for(auto idx : posSubjsUpdate2) {
        uint64_t t = output1[idx];
        allterms.push_back(t);
    }
    for(auto idx : posObjsUpdate2) {
        uint64_t t = output3[idx];
        allterms.push_back(t);
    }
    for(auto idx : negSubjsUpdate2) {
        uint64_t t = sneg[idx];
        allterms.push_back(t);
    }
    for(auto idx : negObjsUpdate1) {
        uint64_t t = oneg[idx];
        allterms.push_back(t);
    }
    std::sort(allterms.begin(), allterms.end());
    auto it = std::unique(allterms.begin(), allterms.end());
    allterms.resize(std::distance(allterms.begin(), it));
    std::vector<uint64_t> allrels;
    for(auto idx : posRels1) {
        uint64_t t = output2[idx];
        allrels.push_back(t);
    }
    for(auto idx : posRels2) {
        uint64_t t = output2[idx];
        allrels.push_back(t);
    }
    std::sort(allrels.begin(), allrels.end());
    it = std::unique(allrels.begin(), allrels.end());
    allrels.resize(std::distance(allrels.begin(), it));

    //Initialize gradient matrix
    for(uint16_t i = 0; i < allterms.size(); ++i) {
        gradientsE.push_back(EntityGradient(allterms[i], dim));
    }
    for(uint16_t i = 0; i < allrels.size(); ++i) {
        gradientsR.push_back(EntityGradient(allrels[i], dim));
    }

    //Update the gradient matrix with the positive subjects
    update_gradient_matrix(gradientsE, posSignMatrix, posSubjsUpdate1,
            output1, 1, -1);
    update_gradient_matrix(gradientsE, posSignMatrix, posObjsUpdate1,
            output3, -1, 1);
    update_gradient_matrix(gradientsE, neg1SignMatrix, posSubjsUpdate1,
            output1, -1, 1);
    update_gradient_matrix(gradientsE, neg1SignMatrix, negObjsUpdate1,
            oneg, 1, -1);

    update_gradient_matrix(gradientsE, posSignMatrix, posSubjsUpdate2,
            output1, 1, -1);
    update_gradient_matrix(gradientsE, posSignMatrix, posObjsUpdate2,
            output3, -1, 1);
    update_gradient_matrix(gradientsE, neg2SignMatrix, negSubjsUpdate2,
            sneg, -1, 1);
    update_gradient_matrix(gradientsE, neg2SignMatrix, posObjsUpdate2,
            output3, 1, -1);


    //Update the gradient matrix of the relations
    update_gradient_matrix(gradientsR, posSignMatrix, posRels1,
            output2, 1, -1);
    update_gradient_matrix(gradientsR, neg1SignMatrix, posRels1,
            output2, -1, 1);
    update_gradient_matrix(gradientsR, posSignMatrix, posRels2,
            output2, 1, -1);
    update_gradient_matrix(gradientsR, neg2SignMatrix, posRels2,
            output2, -1, 1);

    //Update the gradients of the entities and relations
    if (adagrad) {
        for(auto &i : gradientsE) {
            double *emb = E->get(i.id);
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
            sum = sqrt(sum);
            //normalization step
            for(uint16_t j = 0; j < dim; ++j) {
                emb[j] = emb[j] / sum;
            }


        }
        for(auto &i : gradientsR) {
            double *emb = R->get(i.id);
            double *pr = pr2.get() + i.id * dim;
            for(uint16_t j = 0; j < dim; ++j) {
                const double g = (double)i.dimensions[j] / i.n;
                pr[j] += g * g;
                double maxv = max(sqrt(pr[j]), (double)1e-7);
                emb[j] -= learningrate * g / maxv;
            }
        }
    } else { //sgd
        for (auto &i : gradientsE) {
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
        for (auto &i : gradientsR) {
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

void Transe::batch_processer(
        tbb::concurrent_bounded_queue<std::shared_ptr<BatchIO>> *inputQueue,
        tbb::concurrent_bounded_queue<std::shared_ptr<BatchIO>> *outputQueue,
        uint64_t *violations,
        uint16_t epoch) {
    std::shared_ptr<BatchIO> pio;
    uint64_t viol = 0;
    uint16_t nbatches = 0;
    while (true) {
        inputQueue->pop(pio);
        if (pio == NULL) {
            break;
        }
        process_batch(*pio.get(), epoch, nbatches);
        viol += pio->violations;
        pio->clear();
        outputQueue->push(pio);
        nbatches += 1;
    }
    *violations = viol;
    BOOST_LOG_TRIVIAL(debug) << nbatches;
}

void _store_entities(string path, bool compress, const double *b, const double *e) {
    ofstream ofs;
    ofs.open(path, std::ofstream::out);
    boost::iostreams::filtering_stream<boost::iostreams::output> out;
    if (compress) {
        out.push(boost::iostreams::gzip_compressor());
    }
    out.push(ofs);
    while (b != e) {
        ofs.write((char*)b, 8);
        b++;
    }
    BOOST_LOG_TRIVIAL(debug) << "Done";
}

void Transe::store_model(string path,
        const bool compressstorage,
        const uint16_t nthreads) {
    ofstream ofs;
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
    }
    BOOST_LOG_TRIVIAL(debug) << "Serialization done";
}

void Transe::train(BatchCreator &batcher, const uint16_t nthreads,
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

    for (uint16_t epoch = 0; epoch < epochs; ++epoch) {
        std::chrono::time_point<std::chrono::system_clock> start=std::chrono::system_clock::now();
        //Init code
        batcher.start();
        uint32_t batchcounter = 0;
        tbb::concurrent_bounded_queue<std::shared_ptr<BatchIO>> inputQueue;
        tbb::concurrent_bounded_queue<std::shared_ptr<BatchIO>> doneQueue;
        std::vector<uint64_t> violations;
        violations.resize(nthreads);
        for(uint16_t i = 0; i < nthreads; ++i) {
            doneQueue.push(std::shared_ptr<BatchIO>(new BatchIO(batchsize, dim)));
            violations[i] = 0;
        }

        //Start nthreads
        std::vector<std::thread> threads;
        for(uint16_t i = 0; i < nthreads; ++i) {
            threads.push_back(std::thread(&Transe::batch_processer,
                        this, &inputQueue, &doneQueue, &violations[i],
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
        for(uint16_t i = 0; i < nthreads; ++i) {
            threads[i].join();
            totalV += violations[i];
        }
        std::chrono::duration<double> elapsed_seconds = std::chrono::system_clock::now() - start;
        BOOST_LOG_TRIVIAL(info) << "Epoch " << epoch << ". Time=" << elapsed_seconds.count() << "sec. Violations=" << totalV;

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
Transe::loadModel(string path) {
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
