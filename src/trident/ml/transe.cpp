#include <trident/ml/transe.h>

void TranseLearner::update_gradient_matrix(std::vector<EntityGradient> &gradients,
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

bool TranseLearner::shouldUpdate(uint32_t idx) {
    /*uint64_t median = E->getMedianUpdatesLastEpoch();
    uint64_t allUpdatesLastEpoch = E->getAllUpdatesLastEpoch();
    uint32_t updatesLastEpoch = E->getUpdatesLastEpoch(idx);
    if (updatesLastEpoch < median) {
        return false;
    } else {
        return true;
    }*/
    return true;
    //std::cout << "s=" << output1[i] << "thisupdate=" << E->getUpdatesLastEpoch(output1[i]) << " allupdates=" << E->getAllUpdatesLastEpoch() <<  " nenties=" << E->getUpdatedEntitiesLastEpoch() << " median=" << E->getMedianUpdatesLastEpoch() << endl;
}

void TranseLearner::process_batch_withnegs(BatchIO &io, std::vector<uint64_t> &oneg,
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
            if (shouldUpdate(output1[i])) {
                posSubjsUpdate1.push_back(i);
            }
            if (shouldUpdate(output3[i])) {
                posObjsUpdate1.push_back(i);
            }
            if (shouldUpdate(oneg[i])) {
                negObjsUpdate1.push_back(i);
            }
            posRels1.push_back(i);
        }
        if (diffp - diff2n + margin > 0) {
            io.violations += 1;
            if (shouldUpdate(output3[i])) {
                posObjsUpdate2.push_back(i);
            }
            if (shouldUpdate(output1[i])) {
                posSubjsUpdate2.push_back(i);
            }
            if (shouldUpdate(sneg[i])) {
                negSubjsUpdate2.push_back(i);
            }
            posRels2.push_back(i);
        }
    }

    //Sort the triples in the batch by subjects and objects
    std::sort(posSubjsUpdate1.begin(), posSubjsUpdate1.end(), _PairwiseSorter(output1));
    std::sort(posSubjsUpdate2.begin(), posSubjsUpdate2.end(), _PairwiseSorter(output1));
    std::sort(posObjsUpdate1.begin(), posObjsUpdate1.end(), _PairwiseSorter(output3));
    std::sort(posObjsUpdate2.begin(), posObjsUpdate2.end(), _PairwiseSorter(output3));
    std::sort(negObjsUpdate1.begin(), negObjsUpdate1.end(), _PairwiseSorter(oneg));
    std::sort(negSubjsUpdate2.begin(), negSubjsUpdate2.end(), _PairwiseSorter(sneg));
    std::sort(posRels1.begin(), posRels1.end(), _PairwiseSorter(output2));
    std::sort(posRels2.begin(), posRels2.end(), _PairwiseSorter(output2));

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

    //Update the gradients
    update_gradients(io, gradientsE, gradientsR);
}
