#include <trident/ml/distmul.h>

struct _DistMulSorter {
    const std::vector<uint64_t> &vec;
    _DistMulSorter(const std::vector<uint64_t> &vec) : vec(vec) {}
    bool operator() (const uint16_t v1, const uint16_t v2) const {
        return vec[v1] < vec[v2];
    }
};


void DistMulLearner::update_gradient_matrix(std::vector<EntityGradient> &gradients,
        std::vector<std::unique_ptr<float>> &gradmatrix,
        std::vector<uint16_t> &inputTripleID,
        std::vector<uint64_t> &inputTerms) {

    uint16_t gidx = 0;
    for(uint16_t i = 0; i < inputTripleID.size(); ++i) {
        uint16_t tripleId = inputTripleID[i];
        const uint64_t t = inputTerms[tripleId];
        while (gradients[gidx].id < t && gidx < gradients.size()) {
            gidx += 1;
        }
        //I must reach the point when the element exists
        assert(gidx < gradients.size() && gradients[gidx].id == t);
        float *update = gradmatrix[tripleId].get();
        for(uint16_t dimidx = 0; dimidx < dim; ++dimidx) {
            gradients[gidx].dimensions[dimidx] -= update[dimidx];
        }
        gradients[gidx].n++;
    }
}

void DistMulLearner::process_batch(BatchIO &io,
        const uint16_t epoch,
        const uint16_t nbatches) {

    const uint32_t sizebatch = io.field1.size();
    std::vector<uint64_t> &output1 = io.field1;
    std::vector<uint64_t> &output2 = io.field2;
    std::vector<uint64_t> &output3 = io.field3;
    std::vector<EntityGradient> gradientsE; //The gradients for all entities
    std::vector<EntityGradient> gradientsR; //The gradients for all entities
    std::vector<uint64_t> allterms;
    std::vector<uint64_t> allrels;

    /*** Collect all entities and relations ***/
    for(uint32_t i = 0; i < sizebatch; ++i) {
        allterms.push_back(output1[i]);
        allterms.push_back(output3[i]);
        allrels.push_back(output2[i]);
    }
    std::sort(allterms.begin(), allterms.end());
    auto it = std::unique(allterms.begin(), allterms.end());
    allterms.resize(std::distance(allterms.begin(), it));
    std::sort(allrels.begin(), allrels.end());
    it = std::unique(allrels.begin(), allrels.end());
    allrels.resize(std::distance(allrels.begin(), it));

    //Initialize the gradient matrices
    for(uint16_t i = 0; i < allterms.size(); ++i) {
        gradientsE.push_back(EntityGradient(allterms[i], dim));
    }
    for(uint16_t i = 0; i < allrels.size(); ++i) {
        gradientsR.push_back(EntityGradient(allrels[i], dim));
    }

    std::vector<std::unique_ptr<float>> &gradsH = io.posSignMatrix;
    std::vector<std::unique_ptr<float>> &gradsT = io.neg1SignMatrix;
    std::vector<std::unique_ptr<float>> &gradsR = io.neg2SignMatrix;

    //Used to resort the batch by entity
    std::vector<uint16_t> positions;

    /*** Get corresponding embeddings ***/
    for(uint32_t s = 0; s < sizebatch; ++s) {
        double* sp = E->get(output1[s]);
        double* pp = R->get(output2[s]);
        double* op = E->get(output3[s]);

        float *h = gradsH[s].get();
        float *t = gradsT[s].get();
        float *r = gradsR[s].get();
        for(uint16_t i = 0; i < dim; ++i) {
            //First term
            const float term_h1 = pp[i] * op[i];
            const float term_t1 = pp[i] * sp[i];
            const float term_r1 = sp[i] * op[i];

            //Second term
            float e_num = e_score(h[i],r[i],t[i]);
            float e_denom_h2 = 0;
            float e_denom_t2 = 0;
            float e_denom_r2 = 0;
            if (numneg == 0) {
                for(uint16_t j = 0; j < ne; ++j) {
                    float e_neg = E->get(j)[i];
                    e_denom_h2 += e_score(e_neg, r[i], t[i]);
                    e_denom_t2 += e_score(h[i], r[i], e_neg);
                }
                for(uint16_t j = 0; j < nr; ++j) {
                    float r_neg = R->get(j)[i];
                    e_denom_r2 += e_score(h[i], r_neg, t[i]);
                }
            } else {
                BOOST_LOG_TRIVIAL(error) << "Not implemented yet";
            }

            //Gradients
            const float grad_h = -term_h1 + e_num / e_denom_h2;
            const float grad_t = -term_t1 + e_num / e_denom_t2;
            const float grad_r = -term_r1 + e_num / e_denom_r2;
            h[i] = grad_h;
            t[i] = grad_t;
            r[i] = grad_r;
        }

        positions.push_back(s);
    }

    //Create a single embedding for each entity and relation that appears in
    //the batch
    std::sort(positions.begin(), positions.end(), _DistMulSorter(output1));
    //Update the subjects
    update_gradient_matrix(gradientsE, gradsH, positions, output1);

    //Update the objects
    std::sort(positions.begin(), positions.end(), _DistMulSorter(output3));
    update_gradient_matrix(gradientsE, gradsT, positions, output3);

    //Update the relations
    std::sort(positions.begin(), positions.end(), _DistMulSorter(output2));
    update_gradient_matrix(gradientsR, gradsR, positions, output2);

    //Update the gradients
    if (adagrad) {
        BOOST_LOG_TRIVIAL(error) << "Adagrad not supported (yet)";
        throw 10;
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
