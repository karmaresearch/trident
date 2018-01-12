#include <trident/ml/distmul.h>

struct _DistMulSorter {
    const std::vector<uint64_t> &vec;
    _DistMulSorter(const std::vector<uint64_t> &vec) : vec(vec) {}
    bool operator() (const uint16_t v1, const uint16_t v2) const {
        return vec[v1] < vec[v2];
    }
};


void DistMulLearner::update_gradient_matrix(std::vector<EntityGradient> &gradients,
        std::vector<std::unique_ptr<double>> &gradmatrix,
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
        double *update = gradmatrix[tripleId].get();
        for(uint16_t dimidx = 0; dimidx < dim; ++dimidx) {
            gradients[gidx].dimensions[dimidx] += update[dimidx];
        }
        gradients[gidx].n++;
    }
}

double DistMulLearner::sumNeg(double *h, double *r, double *t, uint16_t dim,
        int so, std::vector<double*> &negs) {
    double out = 0;
    for(auto &neg : negs) {
        double score = 0;
        for(uint16_t i = 0; i < dim; ++i) {
            if (so == 0) {
                score += r[i] * t[i] * neg[i];
            } else {
                score += r[i] * h[i] * neg[i];
            }
        }
        out += exp(score);
    }
    return out;
}

double DistMulLearner::derNeg_r(int idx, double *h, double *r, double *t,
        uint16_t dim, int so, std::vector<double*> &negs) {
    double out = 0;
    for(auto &neg : negs) {
        double score = 0;
        for(uint16_t i = 0; i < dim; ++i) {
            if (so == 0) {
                score += r[i] * t[i] * neg[i];
            } else {
                score += r[i] * h[i] * neg[i];
            }
        }
        if (so == 0) {
            out += neg[idx] * t[idx] * exp(score);
        } else {
            out += neg[idx] * h[idx] * exp(score);
        }
    }
    return out;
}

double DistMulLearner::derNeg_t(int idx, double *h, double *r, double *t,
        uint16_t dim, std::vector<double*> &negs) {
    double out = 0;
    for(auto &neg : negs) {
        double score = 0;
        for(uint16_t i = 0; i < dim; ++i) {
            score += r[i] * h[i] * neg[i];
        }
        out += neg[idx] * r[idx] * exp(score);
    }
    return out;
}

double DistMulLearner::derNeg_h(int idx, double *h, double *r, double *t,
        uint16_t dim, std::vector<double*> &negs) {
    double out = 0;
    for(auto &neg : negs) {
        double score = 0;
        for(uint16_t i = 0; i < dim; ++i) {
            score += r[i] * t[i] * neg[i];
        }
        out += neg[idx] * r[idx] * exp(score);
    }
    return out;
}

double DistMulLearner::escore(double *h, double *r, double *t, uint16_t dim) {
    double score_num = 0;
    for(uint16_t i = 0; i < dim; ++i) {
        score_num += h[i] * r[i] * t[i];
    }
    double num = exp(score_num);
    return num;
}

double DistMulLearner::softmax(double *h, double *r, double *t, uint16_t dim,
        int so,
        std::vector<double*> &negs) {
    double num = escore(h, r, t, dim);
    double den = sumNeg(h, r, t, dim, so, negs);
    return num / (den + num);
}

double DistMulLearner::loss(double *h, double *r, double *t, uint16_t dim,
        std::vector<double*> &negs1,
        std::vector<double*> &negs2) {
    double s1 = softmax(h, r, t, dim, 0, negs1);
    double s2 = softmax(h, r, t, dim, 1, negs2);
    return -log(s1) - log(s2);
}

double DistMulLearner::loss(std::vector<uint64_t> &output1,
        std::vector<uint64_t> &output2,
        std::vector<uint64_t> &output3,
        uint16_t dim,
        std::vector<uint64_t> &negativeHeadEntities,
        std::vector<uint64_t> &negativeTailEntities) {
    double out = 0.0;
    uint32_t sizebatch = output1.size();
    uint16_t negsPerTriple = negativeHeadEntities.size() / sizebatch;
    for(uint32_t s = 0; s < sizebatch; ++s) {
        double* sp = E->get(output1[s]);
        double* pp = R->get(output2[s]);
        double* op = E->get(output3[s]);

        //Calculate the softmax function
        std::vector<double*> negsp;
        std::vector<double*> negsn;
        for(uint16_t i = 0; i < negsPerTriple; ++i) {
            uint64_t idp = negativeHeadEntities[negsPerTriple * s + i];
            uint64_t idn = negativeTailEntities[negsPerTriple * s + i];
            negsp.push_back(E->get(idp));
            negsn.push_back(E->get(idn));
        }
        out += loss(sp, pp, op, dim, negsp, negsn);
    }
    return out;
}

void DistMulLearner::getRandomEntities(uint16_t n, std::vector<double*> &negs,
        std::vector<uint64_t> &entities) {
    negs.clear();
    //Used to generate negative entity values
    std::uniform_int_distribution<uint32_t> dis(0, ne - 1);
    for(uint16_t i = 0; i < n; ++i) {
        uint32_t id = dis(gen);
        negs.push_back(E->get(id));
        entities.push_back(id);
    }
}

void DistMulLearner::process_batch(BatchIO &io,
        const uint32_t epoch,
        const uint16_t nbatches) {

    const uint32_t sizebatch = io.field1.size();
    std::vector<uint64_t> &output1 = io.field1;
    std::vector<uint64_t> &output2 = io.field2;
    std::vector<uint64_t> &output3 = io.field3;
    std::vector<EntityGradient> gradientsE; //The gradients for all entities
    std::vector<EntityGradient> gradientsR; //The gradients for all entities
    std::vector<uint64_t> allterms;
    std::vector<uint64_t> allrels;

    if (io.support1_d.size() < io.batchsize) {
        //Create support embeddings
        for(uint16_t i = 0; i < io.batchsize; ++i) {
            io.support1_d.push_back(std::unique_ptr<double>(new double[dim]));
            io.support2_d.push_back(std::unique_ptr<double>(new double[dim]));
            io.support3_d.push_back(std::unique_ptr<double>(new double[dim]));
            for(uint16_t j = 0; j < numneg; ++j) {
                io.support4_d.push_back(std::unique_ptr<double>(new double[dim]));
                io.support5_d.push_back(std::unique_ptr<double>(new double[dim]));
            }
        }
    }

    //Support data structures
    std::vector<std::unique_ptr<double>> &gradsH = io.support1_d;
    std::vector<std::unique_ptr<double>> &gradsT = io.support2_d;
    std::vector<std::unique_ptr<double>> &gradsR = io.support3_d;
    std::vector<std::unique_ptr<double>> &gradsNegH = io.support4_d;
    std::vector<std::unique_ptr<double>> &gradsNegT = io.support5_d;

    //Used to resort the batch by entity
    std::vector<uint16_t> positions;
    std::vector<uint16_t> positionsNeg;
    std::vector<uint64_t> negativeHeadEntities;
    std::vector<uint64_t> negativeTailEntities;

    double totalLoss = 0.0;

    /*** Get corresponding embeddings ***/
    for(uint32_t s = 0; s < sizebatch; ++s) {
        double* sp = E->get(output1[s]);
        double* pp = R->get(output2[s]);
        double* op = E->get(output3[s]);

        //Calculate the softmax function
        std::vector<double*> negsp;
        std::vector<double*> negsn;
        getRandomEntities(numneg, negsp, negativeHeadEntities);
        double es = escore(sp, pp, op, dim);
        double sumneg1 = sumNeg(sp, pp, op, dim, 0, negsp);
        double softm_h = es / (sumneg1 + es);
        getRandomEntities(numneg, negsn, negativeTailEntities);
        double sumneg2 = sumNeg(sp, pp, op, dim, 1, negsn);
        double softm_t = es / (sumneg2 + es);

        //Update the positive entities
        double *h = gradsH[s].get();
        double *t = gradsT[s].get();
        double *r = gradsR[s].get();
        for(uint16_t i = 0; i < dim; ++i) {
            const double term_h1 = pp[i] * op[i];
            const double term_t1 = pp[i] * sp[i];
            const double term_r1 = sp[i] * op[i];

            const double grad_h = term_h1 * softm_h +
                (term_h1 * es + derNeg_t(i, sp, pp, op, dim, negsn)) / (sumneg2 + es) -
                2 * term_h1;
            const double grad_t = term_t1 * softm_t +
                (term_t1 * es + derNeg_h(i, sp, pp, op, dim, negsp)) / (sumneg1 + es) -
                2 * term_t1;
            const double grad_r =
                (term_r1 * es + derNeg_r(i, sp, pp, op, dim, 0, negsp)) / (es + sumneg1) +
                (term_r1 * es + derNeg_r(i, sp, pp, op, dim, 1, negsn)) / (es + sumneg2) -
                2 * term_r1;
            h[i] = grad_h;
            t[i] = grad_t;
            r[i] = grad_r;
        }

        //Calculate the gradients for the negative entities
        for(uint16_t j = 0; j < numneg; ++j) {
            const uint32_t startidx = s * numneg;
            double *nh = gradsNegH[startidx + j].get();
            double *nt = gradsNegT[startidx + j].get();
            for(uint16_t i = 0; i < dim; ++i) {
                //Gradients!
                const double term_h1 = pp[i] * op[i];
                const double term_t1 = pp[i] * sp[i];
                const double gradh = term_h1 * escore(negsp[j], pp, op, dim) / (sumneg1 + es);
                const double gradt = term_t1 * escore(sp, pp, negsp[j], dim) / (sumneg2 + es);
                nh[i] = gradh;
                nt[i] = gradt;
            }
        }

        //Add indices that later will allow us to update the embeddings
        positions.push_back(s);
        for(uint16_t i = 0; i < numneg; ++i) {
            positionsNeg.push_back(s * numneg + i);
        }
        //Calculate the total loss
        totalLoss += loss(sp, pp, op, dim, negsp, negsn);
    }
    io.loss = totalLoss / sizebatch;
    if (totalLoss != totalLoss) {
        throw 10;
    }

    /*** Collect all entities and relations ***/
    for(uint32_t i = 0; i < sizebatch; ++i) {
        allterms.push_back(output1[i]);
        allterms.push_back(output3[i]);
        allrels.push_back(output2[i]);
    }
    for(auto &el : negativeHeadEntities) {
        allterms.push_back(el);
    }
    for(auto &el : negativeTailEntities) {
        allterms.push_back(el);
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

    //Create a single embedding for each entity and relation that appears in
    //the batch
    std::sort(positions.begin(), positions.end(), _DistMulSorter(output1));
    //Update the subjects
    update_gradient_matrix(gradientsE, gradsH, positions, output1);

    //Update the objects
    std::sort(positions.begin(), positions.end(), _DistMulSorter(output3));
    update_gradient_matrix(gradientsE, gradsT, positions, output3);

    //Update the negative subjects
    std::sort(positionsNeg.begin(), positionsNeg.end(),
            _DistMulSorter(negativeHeadEntities));
    update_gradient_matrix(gradientsE, gradsNegH, positionsNeg, negativeHeadEntities);

    //Update the negative objects
    std::sort(positionsNeg.begin(), positionsNeg.end(),
            _DistMulSorter(negativeTailEntities));
    update_gradient_matrix(gradientsE, gradsNegT, positionsNeg, negativeTailEntities);

    //Update the relations
    std::sort(positions.begin(), positions.end(), _DistMulSorter(output2));
    update_gradient_matrix(gradientsR, gradsR, positions, output2);

    //Check gradients E
    /*** DEBUG ***/
    /*const double delta = 0.0001;
      long good_h, bad_h;
      good_h = bad_h = 0;
      long idx = 0;
      cout << "gradients size " << gradientsE.size() << endl;
      for(auto &i : gradientsE) {
      double *emb = E->get(i.id);
      for(uint16_t j = 0; j < dim; ++j) {
      double orig = emb[j];
      emb[j] = orig + delta;
      double loss_plus = loss(output1, output2, output3, dim,
      negativeHeadEntities, negativeTailEntities);
      emb[j] = orig - delta;
      double loss_minus = loss(output1, output2, output3, dim,
      negativeHeadEntities, negativeTailEntities);
      double res = (loss_plus - loss_minus) / (2*delta);
      if (abs(res - i.dimensions[j]) > 0.01) {
      bad_h++;
      } else {
      good_h++;
      }
      emb[j] = orig;
      }
      idx++;
      cout << idx << " " << good_h << " " << bad_h << endl;
      }
      cout << "good_h=" << good_h << " bad_h=" << bad_h;*/
    /*** END DEBUG ***/

    //Update the gradients
    if (adagrad) {
        LOG(ERRORL) << "Adagrad not supported (yet)";
        throw 10;
    } else {
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


    /*** //rel
      const float delta = 0.001;
      double orig = pp[i];
      pp[i] = orig + delta;
      double soft_plus = loss(sp, pp, op, dim, negsp, negsn);
      pp[i] = orig - delta;
      double soft_min = loss(sp, pp, op, dim, negsp, negsn);
      pp[i] = orig;
      double res = (soft_plus - soft_min) / (2 * delta);
      if (abs(res - grad_r) > 0.0001) {
      }
    //head
    orig = sp[i];
    sp[i] = orig + delta;
    soft_plus = loss(sp, pp, op, dim, negsp, negsn);
    sp[i] = orig - delta;
    soft_min = loss(sp, pp, op, dim, negsp, negsn);
    sp[i] = orig;
    res = (soft_plus - soft_min) / (2 * delta);
    if (abs(res - grad_h) > 0.0001) {
    bad_h++;
    } else {
    good_h++;
    }
    //tail
    orig = op[i];
    op[i] = orig + delta;
    soft_plus = loss(sp, pp, op, dim, negsp, negsn);
    op[i] = orig - delta;
    soft_min = loss(sp, pp, op, dim, negsp, negsn);
    op[i] = orig;
    res = (soft_plus - soft_min) / (2 * delta);
    if (abs(res - grad_t) > 0.0001) {
    bad_t++;
    } else {
    good_t++;
    } ***/

}
