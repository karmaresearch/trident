#include <trident/ml/transe.h>
#include <iostream>
#include <fstream>

#include <tbb/concurrent_queue.h>

#include <boost/filesystem/operations.hpp>
#include <boost/filesystem/path.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <boost/archive/text_iarchive.hpp>
#include <boost/iostreams/filter/gzip.hpp>
#include <boost/iostreams/filtering_stream.hpp>
#include <boost/serialization/vector.hpp>

namespace fs = boost::filesystem;

using namespace std;

void Transe::setup(const uint16_t nthreads) {
    BOOST_LOG_TRIVIAL(debug) << "Creating E ...";
    E = std::shared_ptr<Embeddings<float>>(new Embeddings<float>(ne, dim));
    //Initialize it
    E->init(nthreads);
    BOOST_LOG_TRIVIAL(debug) << "Creating R ...";
    R = std::shared_ptr<Embeddings<float>>(new Embeddings<float>(nr, dim));
    R->init(nthreads);
}

float Transe::dist_l1(float* head, float* rel, float* tail,
        float *matrix) {
    float result = 0.0;
    for (uint16_t i = 0; i < dim; ++i) {
        auto value = head[i] + rel[i] - tail[i];
        matrix[i] = value;
        result += abs(value);
    }
    return result;
}

void Transe::gen_random(std::vector<uint64_t> &input, const uint64_t max) {
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(0, max-1);
    for(uint32_t i = 0; i < input.size(); ++i) {
        input[i] = dis(gen);
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
            gradients[gidx].dimensions[dimidx] += signs[dimidx] >= 0 ? pos : neg;
        }
        gradients[gidx].n++;
    }
}

void Transe::process_batch(BatchIO &io) {
    std::vector<uint64_t> &output1 = io.field1;
    std::vector<uint64_t> &output2 = io.field2;
    std::vector<uint64_t> &output3 = io.field3;
    std::vector<std::unique_ptr<float>> &posSignMatrix = io.posSignMatrix;
    std::vector<std::unique_ptr<float>> &neg1SignMatrix = io.neg1SignMatrix;
    std::vector<std::unique_ptr<float>> &neg2SignMatrix = io.neg2SignMatrix;

    //Generate negative samples
    std::vector<uint64_t> oneg;
    std::vector<uint64_t> sneg;
    uint32_t sizebatch = output1.size();
    oneg.resize(sizebatch);
    sneg.resize(sizebatch);
    gen_random(oneg, ne);
    gen_random(sneg, ne);

    //Support data structures
    std::vector<EntityGradient> gradientsE; //The gradients for all entities
    std::vector<EntityGradient> gradientsR; //The gradients for all entities
    std::vector<uint32_t> posSubjsUpdate; //List that contains the positive subjects to update
    std::vector<uint32_t> posObjsUpdate; //List that contains the positive objects to update
    std::vector<uint32_t> negSubjsUpdate; //List that contains the negative subjects to update
    std::vector<uint32_t> negObjsUpdate; //List that contains the negative objects to update
    std::vector<uint32_t> posRels; //List that contains all relations to update

    for(uint32_t i = 0; i < sizebatch; ++i) {
        //Get corresponding embeddings
        float* sp = E->get(output1[i]);
        float* pp = R->get(output2[i]);
        float* op = E->get(output3[i]);
        float* on = E->get(oneg[i]);
        float* sn = E->get(sneg[i]);

        //Get the distances
        auto diffp = dist_l1(sp, pp, op, posSignMatrix[i].get());
        auto diff1n = dist_l1(sp, pp, on, neg1SignMatrix[i].get());
        auto diff2n = dist_l1(sn, pp, op, neg2SignMatrix[i].get());

        //Calculate the violations
        if (diffp - diff1n + margin > 0) {
            io.violations += 1;
            posObjsUpdate.push_back(i);
            negObjsUpdate.push_back(i);
            posRels.push_back(i);
        }
        if (diffp - diff2n + margin > 0) {
            io.violations += 1;
            posSubjsUpdate.push_back(i);
            negSubjsUpdate.push_back(i);
            posRels.push_back(i);
        }
    }

    //Sort the triples in the batch by subjects and objects
    std::sort(posSubjsUpdate.begin(), posSubjsUpdate.end(), _TranseSorter(output1));
    std::sort(negSubjsUpdate.begin(), negSubjsUpdate.end(), _TranseSorter(sneg));
    std::sort(posObjsUpdate.begin(), posObjsUpdate.end(), _TranseSorter(output3));
    std::sort(negObjsUpdate.begin(), negObjsUpdate.end(), _TranseSorter(oneg));
    std::sort(posRels.begin(), posRels.end(), _TranseSorter(output2));

    //Get all the terms that appear in the batch
    std::vector<uint64_t> allterms;
    for(auto idx : posSubjsUpdate) {
        uint64_t t = output1[idx];
        allterms.push_back(t);
    }
    for(auto idx : negSubjsUpdate) {
        uint64_t t = sneg[idx];
        allterms.push_back(t);
    }
    for(auto idx : posObjsUpdate) {
        uint64_t t = output3[idx];
        allterms.push_back(t);
    }
    for(auto idx : negObjsUpdate) {
        uint64_t t = oneg[idx];
        allterms.push_back(t);
    }
    std::sort(allterms.begin(), allterms.end());
    auto it = std::unique(allterms.begin(), allterms.end());
    allterms.resize(std::distance(allterms.begin(), it));
    std::vector<uint64_t> allrels;
    for(auto idx : posRels) {
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
    update_gradient_matrix(gradientsE, posSignMatrix, posSubjsUpdate,
            output1, 1, -1);
    update_gradient_matrix(gradientsE, neg2SignMatrix, negSubjsUpdate,
            sneg, -1, 1);
    update_gradient_matrix(gradientsE, posSignMatrix, posObjsUpdate,
            output3, -1, 1);
    update_gradient_matrix(gradientsE, neg1SignMatrix, negObjsUpdate,
            oneg, 1, -1);

    //Update the gradient matrix of the relations
    update_gradient_matrix(gradientsR, posSignMatrix, posRels,
            output2, 1, -1);
    /*update_gradient_matrix(gradientsR, neg1SignMatrix, posRels,
      output2, -1, 1);
      update_gradient_matrix(gradientsR, neg2SignMatrix, posRels,
      output2, -1, 1);*/

    //Update the gradients of the entities and relations
    for (auto &i : gradientsE) {
        float *emb = E->get(i.id);
        auto n = i.n;
        if (n > 0) {
            for(uint16_t j = 0; j < dim; ++j) {
                emb[j] -= learningrate * i.dimensions[j] / n;
            }
        }
    }
    for (auto &i : gradientsR) {
        float *emb = R->get(i.id);
        auto n = i.n;
        if (n > 0) {
            for(uint16_t j = 0; j < dim; ++j) {
                emb[j] -= learningrate * i.dimensions[j] / n;
            }
        }
    }
}

void Transe::batch_processer(
        tbb::concurrent_bounded_queue<std::shared_ptr<BatchIO>> *inputQueue,
        tbb::concurrent_bounded_queue<std::shared_ptr<BatchIO>> *outputQueue,
        uint64_t *violations) {
    std::shared_ptr<BatchIO> pio;
    uint64_t viol = 0;
    while (true) {
        inputQueue->pop(pio);
        if (pio == NULL) {
            break;
        }

        process_batch(*pio.get());
        viol += pio->violations;
        pio->clear();
        outputQueue->push(pio);
    }
    *violations = viol;
}

void Transe::store_model(string path) {
    ofstream ofs;
    ofs.open(path, std::ofstream::out);
    boost::iostreams::filtering_stream<boost::iostreams::output> out;
    out.push(boost::iostreams::gzip_compressor());
    out.push(ofs);
    boost::archive::text_oarchive oa(out);
    oa << ne;
    oa << nr;
    oa << dim;
    oa << E->getAllEmbeddings();
    oa << R->getAllEmbeddings();
}

void Transe::train(BatchCreator &batcher, const uint16_t nthreads,
        const uint32_t eval_its, const string storefolder) {

    bool shouldStoreModel = storefolder != "" && eval_its > 0;
    //storefolder should point to a directory. Create it if it does not exist
    if (shouldStoreModel && !fs::exists(fs::path(storefolder))) {
        fs::create_directories(storefolder);
    }

    for (uint16_t epoch = 0; epoch < epochs; ++epoch) {
        BOOST_LOG_TRIVIAL(info) << "Start epoch " << epoch;
        //Init code
        batcher.start();
        uint32_t batchcounter = 0;
        tbb::concurrent_bounded_queue<std::shared_ptr<BatchIO>> inputQueue;
        tbb::concurrent_bounded_queue<std::shared_ptr<BatchIO>> doneQueue;
        std::vector<uint64_t> violations;
        violations.resize(nthreads);
        for(uint16_t i = 0; i < nthreads; ++i) {
            doneQueue.push(std::shared_ptr<BatchIO>(new BatchIO(batchsize, dim)));
        }

        //Start nthreads
        std::vector<std::thread> threads;
        for(uint16_t i = 0; i < nthreads; ++i) {
            threads.push_back(std::thread(&Transe::batch_processer,
                        this, &inputQueue, &doneQueue, &violations[i]));
        }

        //Process all batches
        while (true) {
            std::shared_ptr<BatchIO> pio;
            doneQueue.pop(pio);
            if (batcher.getBatch(pio->field1, pio->field2, pio->field3)) {
                inputQueue.push(pio);
                batchcounter++;
                if (batchcounter % 10000 == 0) {
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
        BOOST_LOG_TRIVIAL(info) << "End epoch. Violations=" << totalV;
        if (shouldStoreModel && (epoch+1) % eval_its == 0) {
            string pathmodel = storefolder + "/model-" + to_string(epoch+1) + ".gz";
            BOOST_LOG_TRIVIAL(info) << "Storing the model into " << pathmodel;
            store_model(pathmodel);
        }
    }
}

std::pair<std::shared_ptr<Embeddings<float>>,std::shared_ptr<Embeddings<float>>>
Transe::loadModel(string path) {
    ifstream ifs;
    ifs.open(path);
    boost::iostreams::filtering_stream<boost::iostreams::input> inp;
    inp.push(boost::iostreams::gzip_decompressor());
    inp.push(ifs);
    boost::archive::text_iarchive ia(inp);
    uint32_t ne;
    uint32_t nr;
    uint16_t dim;
    ia >> ne;
    ia >> nr;
    ia >> dim;
    std::vector<float> emb_e;
    ia >> emb_e;
    std::vector<float> emb_r;
    ia >> emb_r;
    //Load E
    std::shared_ptr<Embeddings<float>> E = std::shared_ptr<Embeddings<float>>(
            new Embeddings<float>(ne,dim,emb_e)
            );
    //Load R
    std::shared_ptr<Embeddings<float>> R = std::shared_ptr<Embeddings<float>>(
            new Embeddings<float>(nr, dim, emb_r)
            );
    return std::make_pair(E,R);
}
