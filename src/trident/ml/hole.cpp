#include <trident/ml/hole.h>
#include <trident/utils/fft.h>
#include <unordered_map>

void HoleLearner::update_gradient_matrix(
        std::unordered_map<uint64_t, EntityGradient> &gradients,
        EntityGradient& eg1,
        EntityGradient& eg2,
        uint64_t term
        )
{
    //for (int i = 0; i < gradients.size(); ++i) {
        //bool found = false;
        //if (gradients[i].id == term) {
            //assert(gradients[i].dimensions.size() == eg1.dimensions.size());
            //assert(gradients[i].dimensions.size() == eg2.dimensions.size());
            //found = true;
            for (int j = 0; j < gradients[term].dimensions.size(); ++j) {
                gradients[term].dimensions[j] += eg1.dimensions[j] + eg2.dimensions[j];
            }
            //break;
        //}
    //}
    //assert(found == true);
}

float HoleLearner::score(double* head, double* rel, double* tail) {
    /** Python code
    np.sum(self.R[ps] * ccorr(self.E[ss], self.E[os]), axis=1)
    */
    vector<double> out;
    ccorr(head, tail, dim, out);

    float sum = 0;
    for (int i = 0; i < dim; ++i) {
        sum += (rel[i] * out[i]);
    }
    return sum;
}

void HoleLearner::process_batch_withnegs(BatchIO &io, std::vector<uint64_t> &oneg,
        std::vector<uint64_t> &sneg) {
    std::vector<uint64_t> &output1 = io.field1;
    std::vector<uint64_t> &output2 = io.field2;
    std::vector<uint64_t> &output3 = io.field3;

    LOG(INFOL) << "$$$ Processing a batch with negs";
    // TODO: purpose of support1, support2 etc
    // is to store sign matrices ?
    // can be used for something else ? like gradient values

    //if (io.support1.size() < io.batchsize) {
    //    for(uint16_t i = 0; i < io.batchsize; ++i) {
    //        io.support1.push_back(std::unique_ptr<float>(new float[dim]));
    //        io.support2.push_back(std::unique_ptr<float>(new float[dim]));
    //        io.support3.push_back(std::unique_ptr<float>(new float[dim]));   }

    //}
    //std::vector<std::unique_ptr<float>> &posSignMatrix = io.support1;
    //std::vector<std::unique_ptr<float>> &neg1SignMatrix = io.support2;
    //std::vector<std::unique_ptr<float>> &neg2SignMatrix = io.support3;
    const uint32_t sizebatch = io.field1.size();

    //Support data structures
    std::unordered_map<uint32_t, uint64_t> posSubjsUpdate1; // pos->sub entity
    std::unordered_map<uint32_t, uint64_t> posObjsUpdate1; // pos->obj entity
    std::unordered_map<uint32_t, uint64_t> negObjsUpdate1;
    std::unordered_map<uint32_t, uint64_t> posSubjsUpdate2;
    std::unordered_map<uint32_t, uint64_t> negSubjsUpdate2;
    std::unordered_map<uint32_t, uint64_t> posObjsUpdate2;
    std::unordered_map<uint32_t, uint64_t> posRels1;
    std::unordered_map<uint32_t, uint64_t> posRels2;

    std::unordered_map<uint32_t, float> violatedScorePosAll;
    std::unordered_map<uint32_t, float> violatedScoreNegSub;
    std::unordered_map<uint32_t, float> violatedScoreNegObj;

    //std::vector<EntityGradient> gradientsE;
    //std::vector<EntityGradient> gradientsR;
    std::unordered_map<uint64_t, EntityGradient> gradientsE;
    std::unordered_map<uint64_t, EntityGradient> gradientsR;

    std::vector<uint32_t> violatedPositions;
    std::vector<uint64_t> allterms;
    std::vector<uint64_t> allrels;

    std::chrono::system_clock::time_point start = std::chrono::system_clock::now();
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

    //Initialize gradient matrix
    // PORT: this code is from __init__ of hole.py
    // The last two lines of __init__ initialize the entity and relation
    // embeddings. we try to do the same here
    for(uint16_t i = 0; i < allterms.size(); ++i) {
        EntityGradient entityTemp(allterms[i], dim);
        gradientsE.insert(make_pair(allterms[i], entityTemp));
    }
    for(uint16_t i = 0; i < allrels.size(); ++i) {
        EntityGradient relationTemp(allrels[i], dim);
        gradientsR.insert(make_pair(allrels[i], relationTemp));
    }
    std::chrono::duration<double> duration = std::chrono::system_clock::now() - start;
    LOG(DEBUGL) << "Time to initialize gradients = " << duration.count() * 1000 << " ms";

    //LOG(DEBUGL) << "UNM$$ starting the batch...";
    for(uint32_t i = 0; i < sizebatch; ++i) {
        start = std::chrono::system_clock::now();
        uint64_t subject = output1[i];
        uint64_t predicate = output2[i];
        uint64_t object = output3[i];
        //Get corresponding embeddings
        // PORT:
        // This code is first two calls of unzip_triples() in _pairwise_gradients
        // We do not generate negative ids for relations/predicates
        // hence there is no "pn" (Predicate Negative) in the C++ code
        double* sp = E->get(output1[i]);
        double* pp = R->get(output2[i]);
        double* op = E->get(output3[i]);
        double* on = E->get(oneg[i]);
        double* sn = E->get(sneg[i]);

        //LOG(DEBUGL) << "<" << subject << " , " << predicate << " , " << object << ">";
        //Get the distances
        // pp = pn i.e. predicates are not randomly generated for
        // making a sample negative sample
        auto scorePosAll = sigmoid(score(sp, pp, op));
        // PORT:
        // this code is calculating pscores and nscores from _pairwise_gradients
        // Why am I calculating two negative scores is something I don't remember
        // This could be wrong.
        auto scoreNegObj = sigmoid(score(sp, pp, on));
        auto scoreNegSub = sigmoid(score(sn, pp, op));

        // to update and
        // use update_gradient function to compute CCORR
        if (scorePosAll - scoreNegObj + margin > 0) {
            io.violations += 1;
            violatedPositions.push_back(i);
            // PORT:
            // following temp variables are not used
            posSubjsUpdate1[i] = subject;
            posObjsUpdate1[i] = object;
            negObjsUpdate1[i] = oneg[i];
            posRels1[i] = predicate;
            // PORT:
            // sigmoid_given_fun is the function g_give_f from class Sigmoid from
            // file skge/actfun.py
            violatedScorePosAll[i] = -sigmoid_given_fun(scorePosAll);
            violatedScoreNegObj[i] = -sigmoid_given_fun(scoreNegObj);

            // Predicate/Relation gradients
            // PORT:
            // calculate gpscores * ccorr (E[sp], E[op])
            // and
            // calculate gnscore * ccorr (E[sn], E[on])
            EntityGradient grp(predicate, dim);
            std::chrono::system_clock::time_point start_ccorr = std::chrono::system_clock::now();
            ccorr(sp, op, dim, grp.dimensions);
            std::chrono::duration<double> duration_ccorr = std::chrono::system_clock::now() - start_ccorr;
            LOG(DEBUGL) << "Time to compute ccorr = " << duration_ccorr.count() * 1000 << " ms";
            for (int d = 0; d < dim; ++d) {
                grp.dimensions[d] *= violatedScorePosAll[i];
            }
            EntityGradient grn(predicate, dim);
            start_ccorr = std::chrono::system_clock::now();
            ccorr(sn, on, dim, grn.dimensions);
            duration_ccorr = std::chrono::system_clock::now() - start_ccorr;
            LOG(DEBUGL) << "Time to compute ccorr = " << duration_ccorr.count() * 1000 << " ms";
            for (int d = 0; d < dim; ++d) {
                grn.dimensions[d] *= violatedScoreNegObj[i];
            }

            std::chrono::system_clock::time_point start_grad = std::chrono::system_clock::now();
            // PORT:
            // calculate Sm.dot prodcut of gradient matrix for relations
            update_gradient_matrix(gradientsR, grp, grn, predicate);
            std::chrono::duration<double> duration_grad = std::chrono::system_clock::now() - start_grad;
            LOG(DEBUGL) << "Time to update gradient matrix = " << duration_grad.count() * 1000 << " ms";

            // Object gradients (i.e. when score is violated because of negative object entity)
            // PORT:
            // compute cconv and gejp = gpscores * cconv (E[sp], E[pp])
            EntityGradient gejp(object, dim);
            start_ccorr = std::chrono::system_clock::now();
            cconv(sp, pp, dim, gejp.dimensions);
            duration_ccorr = std::chrono::system_clock::now() - start_ccorr;
            LOG(DEBUGL) << "Time to compute cconv = " << duration_ccorr.count() * 1000 << " ms";
            for (int d = 0; d < dim; ++d) {
                gejp.dimensions[d] *= violatedScorePosAll[i];
            }

            // PORT:
            // compute gejn = gnscores * cconv (E[sn], E[pn])
            // in our case pp = pn
            EntityGradient gejn(object, dim);
            start_ccorr = std::chrono::system_clock::now();
            cconv(sn, pp, dim, gejn.dimensions);
            duration_ccorr = std::chrono::system_clock::now() - start_ccorr;
            LOG(DEBUGL) << "Time to compute cconv = " << duration_ccorr.count() * 1000 << " ms";
            for (int d = 0; d < dim; ++d) {
                gejn.dimensions[d] *= violatedScoreNegObj[i];
            }

            start_grad = std::chrono::system_clock::now();
            update_gradient_matrix(gradientsE, gejp, gejn, object);
            duration_grad = std::chrono::system_clock::now() - start_grad;
            LOG(DEBUGL) << "Time to update gradient matrix = " << duration_grad.count() * 1000 << " ms";

        }
        if (scorePosAll - scoreNegSub + margin > 0) {
            io.violations += 1;
            violatedPositions.push_back(i);
            posObjsUpdate2[i] = object;
            posSubjsUpdate2[i] = subject;
            negSubjsUpdate2[i] = sneg[i];
            posRels2[i] = predicate;
            violatedScorePosAll[i] = -sigmoid_given_fun(scorePosAll);
            violatedScoreNegSub[i] = -sigmoid_given_fun(scoreNegSub);

            // Predicate/Relation gradients
            EntityGradient grp(predicate, dim);
            std::chrono::system_clock::time_point start_ccorr = std::chrono::system_clock::now();
            ccorr(sp, op, dim, grp.dimensions);
            std::chrono::duration<double> duration_ccorr = std::chrono::system_clock::now() - start_ccorr;
            LOG(DEBUGL) << "Time to compute ccorr = " << duration_ccorr.count() * 1000 << " ms";

            for (int d = 0; d < dim; ++d) {
                grp.dimensions[d] *= violatedScorePosAll[i];
            }
            EntityGradient grn(predicate, dim);
            start_ccorr = std::chrono::system_clock::now();
            ccorr(sn, on, dim, grn.dimensions);
            duration_ccorr = std::chrono::system_clock::now() - start_ccorr;
            LOG(DEBUGL) << "Time to compute ccorr = " << duration_ccorr.count() * 1000 << " ms";
            for (int d = 0; d < dim; ++d) {
                grn.dimensions[d] *= violatedScoreNegSub[i];
            }
            std::chrono::system_clock::time_point start_grad = std::chrono::system_clock::now();
            update_gradient_matrix(gradientsR, grp, grn, predicate);
            std::chrono::duration<double> duration_grad = std::chrono::system_clock::now() - start_grad;
            LOG(DEBUGL) << "Time to update gradient matrix = " << duration_grad.count() * 1000 << " ms";

            // Subject gradients
            EntityGradient geip(subject, dim);
            start_ccorr = std::chrono::system_clock::now();
            ccorr(pp, op, dim, geip.dimensions);
            duration_ccorr = std::chrono::system_clock::now() - start_ccorr;
            LOG(DEBUGL) << "Time to compute ccorr = " << duration_ccorr.count() * 1000 << " ms";
            for (int d = 0; d < dim; ++d) {
                geip.dimensions[d] *= violatedScorePosAll[i];
            }

            EntityGradient gein(subject, dim);
            start_ccorr = std::chrono::system_clock::now();
            cconv(sn, pp, dim, gein.dimensions);
            duration_ccorr = std::chrono::system_clock::now() - start_ccorr;
            LOG(DEBUGL) << "Time to compute cconv = " << duration_ccorr.count() * 1000 << " ms";
            for (int d = 0; d < dim; ++d) {
                gein.dimensions[d] *= violatedScoreNegSub[i];
            }
            start_grad = std::chrono::system_clock::now();
            update_gradient_matrix(gradientsE, geip, gein, subject);
            duration_grad = std::chrono::system_clock::now() - start_grad;
            LOG(DEBUGL) << "Time to update gradient matrix = " << duration_grad.count() * 1000 << " ms";
        }

        duration = std::chrono::system_clock::now() - start;
        LOG(DEBUGL) << "Time to process sample (" << i <<")  = " << duration.count() * 1000 << " ms";
    }

    //Update the gradients
    vector<EntityGradient> vecGradientsE;
    vector<EntityGradient> vecGradientsR;
    std::transform(gradientsE.begin(), gradientsE.end(),
                    std::back_inserter(vecGradientsE),
                    [] (pair<const uint64_t, EntityGradient> &kv) {return kv.second;}
                    );
    std::transform(gradientsR.begin(), gradientsR.end(),
                    std::back_inserter(vecGradientsR),
                    [] (pair<const uint64_t, EntityGradient> &kv) {return kv.second;}
                    );
    update_gradients(io, vecGradientsE, vecGradientsR);
}
