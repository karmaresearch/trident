#include <boost/chrono.hpp>

#include <sstream>
#include <fstream>
#include <iostream>
#include <vector>
#include <cstdlib>

#include <trident/ml/transe.h>
#include <trident/ml/embeddings.h>

namespace timens = boost::chrono;
using namespace std;

#define DIMS 50

void update_gradient_matrix(std::vector<EntityGradient> &gradients,
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
        for(uint16_t dimidx = 0; dimidx < DIMS; ++dimidx) {
            if (signs[dimidx] != 0) {
                gradients[gidx].dimensions[dimidx] += signs[dimidx] > 0 ? pos : neg;
            }
        }
        gradients[gidx].n++;
    }
}

float dist_l1(float* head, float* rel, float* tail,
        float *matrix) {
    float result = 0.0;
    for (uint16_t i = 0; i < DIMS; ++i) {
        auto value = head[i] + rel[i] - tail[i];
        matrix[i] = value;
        result += abs(value);
    }
    return result;
}

int main(int argc, const char** argv) {
    //Load a batch of triples
    std::vector<uint64_t> triples;
    std::vector<uint64_t> negatives;
    ifstream ifs;

    ifs.open(argv[1]);
    char buffer[24];
    while(true) {
        ifs.read(buffer, 24);
        if (!ifs)
            break;
        //Parse the triple and copy to the array
        long s = *(long*)(buffer);
        long p = *(long*)(buffer + 8);
        long o = *(long*)(buffer + 16);
        triples.push_back(s);
        triples.push_back(p);
        triples.push_back(o);
    }
    ifs.close();

    //Load a batch of negative values
    ifs.open(argv[2]);
    while(true) {
        ifs.read(buffer, 24);
        if (!ifs)
            break;
        //Parse the triple and copy to the array
        long s = *(long*)(buffer);
        long o = *(long*)(buffer + 16);
        negatives.push_back(s);
        negatives.push_back(o);
    }
    ifs.close();

    //Load the entity and relation embeddings
    ifs.open(argv[3]);
    std::vector<double> embe;
    while(true) {
        ifs.read(buffer, 8);
        if (!ifs)
            break;
        double v = *(double*)(buffer);
        embe.push_back(v);
    }
    ifs.close();
    ifs.open(argv[4]);
    std::vector<double> embr;
    while(true) {
        ifs.read(buffer, 8);
        if (!ifs)
            break;
        double v = *(double*)(buffer);
        embr.push_back(v);
    }
    ifs.close();

    //Prepare the datastructures
    uint64_t violations = 0;
    float margin = 2.0;

    const uint64_t sizebatch = triples.size() / 3;
    std::vector<uint64_t> output1;
    std::vector<uint64_t> output2;
    std::vector<uint64_t> output3;
    uint32_t j = 0;
    for(uint32_t i = 0; i < sizebatch; ++i) {
        output1.push_back(triples[j++]);
        output2.push_back(triples[j++]);
        output3.push_back(triples[j++]);
    }
    std::vector<uint64_t> sneg;
    std::vector<uint64_t> oneg;
    bool test = true;
    for(uint32_t i = 0; i < negatives.size(); i+=2) {
        if (test)
            sneg.push_back(negatives[i]);
        else
            oneg.push_back(negatives[i+1]);
        test = !test;
    }
    std::vector<std::unique_ptr<float>> posSignMatrix;
    std::vector<std::unique_ptr<float>> neg1SignMatrix;
    std::vector<std::unique_ptr<float>> neg2SignMatrix;
    for(uint16_t i = 0; i < sizebatch; ++i) {
        posSignMatrix.push_back(std::unique_ptr<float>(new float[DIMS]));
        neg1SignMatrix.push_back(std::unique_ptr<float>(new float[DIMS]));
        neg2SignMatrix.push_back(std::unique_ptr<float>(new float[DIMS]));
    }

    //Convert them to float
    std::vector<float> tmpembe;
    std::vector<float> tmpembr;
    for(auto t : embe) {
        tmpembe.push_back(t);
    }
    for(auto t : embr) {
        tmpembr.push_back(t);
    }
    std::unique_ptr<Embeddings<float>> E = std::unique_ptr<Embeddings<float>>(new Embeddings<float>(embe.size() / DIMS, DIMS, tmpembe));
    std::unique_ptr<Embeddings<float>> R = std::unique_ptr<Embeddings<float>>(new Embeddings<float>(embr.size() / DIMS, DIMS, tmpembr));


    //*****Calculate the gradient and compare it*******
    std::vector<EntityGradient> gradientsE; //The gradients for all entities
    std::vector<EntityGradient> gradientsR; //The gradients for all entities

    std::vector<uint32_t> posSubjsUpdate1; //List that contains the positive subjects to update
    std::vector<uint32_t> posObjsUpdate1; //List that contains the positive objects to update
    std::vector<uint32_t> negObjsUpdate1; //List that contains the negative objects to update

    std::vector<uint32_t> posSubjsUpdate2; //List that contains the positive subjects to update
    std::vector<uint32_t> negSubjsUpdate2; //List that contains the negative subjects to update
    std::vector<uint32_t> posObjsUpdate2; //List that contains the positive objects to update

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
            violations += 1;
            posSubjsUpdate1.push_back(i);
            posObjsUpdate1.push_back(i);
            negObjsUpdate1.push_back(i);
            posRels.push_back(i);
        }
        if (diffp - diff2n + margin > 0) {
            violations += 1;
            posObjsUpdate2.push_back(i);
            posSubjsUpdate2.push_back(i);
            negSubjsUpdate2.push_back(i);
            posRels.push_back(i);
        }
    }

    //Sort the triples in the batch by subjects and objects
    std::sort(posSubjsUpdate1.begin(), posSubjsUpdate1.end(), _TranseSorter(output1));
    std::sort(posSubjsUpdate2.begin(), posSubjsUpdate2.end(), _TranseSorter(output1));
    std::sort(posObjsUpdate1.begin(), posObjsUpdate1.end(), _TranseSorter(output3));
    std::sort(posObjsUpdate2.begin(), posObjsUpdate2.end(), _TranseSorter(output3));
    std::sort(negObjsUpdate1.begin(), negObjsUpdate1.end(), _TranseSorter(oneg));
    std::sort(negSubjsUpdate2.begin(), negSubjsUpdate2.end(), _TranseSorter(sneg));
    std::sort(posRels.begin(), posRels.end(), _TranseSorter(output2));

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
    for(auto idx : posRels) {
        uint64_t t = output2[idx];
        allrels.push_back(t);
    }
    std::sort(allrels.begin(), allrels.end());
    it = std::unique(allrels.begin(), allrels.end());
    allrels.resize(std::distance(allrels.begin(), it));

    //Initialize gradient matrix
    for(uint16_t i = 0; i < allterms.size(); ++i) {
        gradientsE.push_back(EntityGradient(allterms[i], DIMS));
    }
    for(uint16_t i = 0; i < allrels.size(); ++i) {
        gradientsR.push_back(EntityGradient(allrels[i], DIMS));
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
    update_gradient_matrix(gradientsR, posSignMatrix, posRels,
            output2, 1, -1);
    /*update_gradient_matrix(gradientsR, neg1SignMatrix, posRels,
            output2, -1, 1);
    update_gradient_matrix(gradientsR, neg2SignMatrix, posRels,
            output2, -1, 1);*/

    return 0;
}
