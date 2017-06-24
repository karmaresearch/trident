#include <trident/ml/transe.h>
#include <iostream>

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

void Transe::train(BatchCreator &batcher, const uint16_t nthreads) {
    std::vector<uint64_t> output1;
    std::vector<uint64_t> output2;
    std::vector<uint64_t> output3;

    std::vector<uint64_t> oneg;
    std::vector<uint64_t> sneg;

    for (uint16_t epoch = 0; epoch < epochs; ++epoch) {
        BOOST_LOG_TRIVIAL(info) << "Start epoch " << epoch;
        //Init code
        batcher.start();
        uint64_t violations = 0;
        uint32_t batchcounter = 0;
        std::vector<std::unique_ptr<float>> posSignMatrix;
        std::vector<std::unique_ptr<float>> neg1SignMatrix;
        std::vector<std::unique_ptr<float>> neg2SignMatrix;
        //Create the sign matrices (used to calculate the gradients)
        for(uint16_t i = 0; i < batchsize; ++i) {
            posSignMatrix.push_back(std::unique_ptr<float>(new float[dim]));
            neg1SignMatrix.push_back(std::unique_ptr<float>(new float[dim]));
            neg2SignMatrix.push_back(std::unique_ptr<float>(new float[dim]));
        }

        while (batcher.getBatch(output1, output2, output3)) {
            //Generate negative samples
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
            for(uint16_t i = 0; i < batchsize; ++i) {
                memset(posSignMatrix[i].get(), 0, sizeof(float) * dim);
                memset(neg1SignMatrix[i].get(), 0, sizeof(float) * dim);
                memset(neg2SignMatrix[i].get(), 0, sizeof(float) * dim);
            }

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
                    violations += 1;
                    posObjsUpdate.push_back(i);
                    negObjsUpdate.push_back(i);
                    posRels.push_back(i);
                }
                if (diffp - diff2n + margin > 0) {
                    violations += 1;
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

            //Update the gradient matrix with the negative subjects
            update_gradient_matrix(gradientsE, neg2SignMatrix, negSubjsUpdate,
                    sneg, -1, 1);

            //Update the gradient matrix with the positive objects
            update_gradient_matrix(gradientsE, posSignMatrix, posObjsUpdate,
                    output3, -1, 1);

            //Update the gradient matrix with the negative objects
            update_gradient_matrix(gradientsE, neg1SignMatrix, negObjsUpdate,
                    oneg, 1, -1);

            //Update the gradient matrix of the relations
            update_gradient_matrix(gradientsR, posSignMatrix, posRels,
                    output2, 1, -1);

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

            batchcounter++;
            if (batchcounter % 10000 == 0) {
                BOOST_LOG_TRIVIAL(debug) << "Processed " << batchcounter << " batches";
            }
        }
        BOOST_LOG_TRIVIAL(info) << "End epoch. Violations=" << violations;
    }
}
