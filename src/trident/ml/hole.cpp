#include <trident/ml/hole.h>
#include <trident/utils/fft.h>
#include <unordered_map>

inline double sigmoid(double x) {
    return 1.0 / (1.0 + std::exp(-x));
}

inline double sigmoid_given_fun(double x) {
    return x * (1.0 - x);
}

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
	    gradients[term].n++;
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

    float violatedScorePos;
    float violatedScoreNeg;

    std::unordered_map<uint64_t, EntityGradient> gradientsE;
    std::unordered_map<uint64_t, EntityGradient> gradientsR;

    // std::vector<uint32_t> violatedPositions;
    std::vector<uint64_t> allterms(sizebatch*4);

    std::chrono::system_clock::time_point start = std::chrono::system_clock::now();
    for(uint32_t i = 0; i < sizebatch; ++i) {
        allterms[4*i] = output1[i];
        allterms[4*i+1] = output3[i];
	allterms[4*i+2] = sneg[i];
	allterms[4*i+3] = oneg[i];
    }

    //Initialize gradient matrix
    // PORT: this code is from __init__ of hole.py
    // The last two lines of __init__ initialize the entity and relation
    // embeddings. we try to do the same here
    
    // The updates will be collected in gradientsE/gradientsR, and then applied.
    for (int i = 0; i < allterms.size(); i++) {
	if (gradientsE.find(allterms[i]) == gradientsE.end()) {
	    EntityGradient entityTemp(allterms[i], dim);
	    gradientsE.insert(std::make_pair(allterms[i], entityTemp));
	}
    }
    for (int i = 0; i < sizebatch; i++) {
	if (gradientsR.find(output2[i]) == gradientsR.end()) {
	    EntityGradient relationTemp(output2[i], dim);
	    gradientsR.insert(std::make_pair(output2[i], relationTemp));
	}
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
        double* sp = E->get(subject);
        double* pp = R->get(predicate);
        double* op = E->get(object);
        double* on = E->get(oneg[i]);
        double* sn = E->get(sneg[i]);
	double *pn = pp;

        //LOG(DEBUGL) << "<" << subject << " , " << predicate << " , " << object << ">";
        //Get the distances
        // pp = pn i.e. predicates are not randomly generated for
        // making a sample negative sample
        auto scorePos = sigmoid(score(sp, pp, op));
        // PORT:
        // this code is calculating pscores and nscores from _pairwise_gradients
        auto scoreNeg = sigmoid(score(sn, pn, on));

        // to update and
        // use update_gradient function to compute CCORR
	// LOG(INFOL) << "scoreNeg = " << scoreNeg << ", scorePos = " << scorePos;
	if (scoreNeg + margin > scorePos) {
            io.violations += 1;
            // violatedPositions.push_back(i);
            // PORT:
            // sigmoid_given_fun is the function g_give_f from class Sigmoid from
            // file skge/actfun.py
            violatedScorePos = -sigmoid_given_fun(scorePos);
            violatedScoreNeg = sigmoid_given_fun(scoreNeg);
	    // LOG(INFOL) << "violatedScoreNeg = " << violatedScoreNeg << ", violatedScorePos = " << violatedScorePos;

            // Predicate/Relation gradients
            // PORT:
            // calculate gpscores * ccorr (E[sp], E[op])
            // and
            // calculate gnscore * ccorr (E[sn], E[on])
            EntityGradient grp(predicate, dim);
            // std::chrono::system_clock::time_point start_ccorr = std::chrono::system_clock::now();
            ccorr(sp, op, dim, grp.dimensions);
            // std::chrono::duration<double> duration_ccorr = std::chrono::system_clock::now() - start_ccorr;
            // LOG(DEBUGL) << "Time to compute ccorr = " << duration_ccorr.count() * 1000 << " ms";
            for (int d = 0; d < dim; ++d) {
                grp.dimensions[d] *= violatedScorePos;
            }
	    // LOG(INFOL) << "grp.dimensions[0] = " << grp.dimensions[0];
            EntityGradient grn(predicate, dim);
            // start_ccorr = std::chrono::system_clock::now();
            ccorr(sn, on, dim, grn.dimensions);
            // duration_ccorr = std::chrono::system_clock::now() - start_ccorr;
            // LOG(DEBUGL) << "Time to compute ccorr = " << duration_ccorr.count() * 1000 << " ms";
            for (int d = 0; d < dim; ++d) {
                grn.dimensions[d] *= violatedScoreNeg;
            }
	    // LOG(INFOL) << "grn.dimensions[0] = " << grn.dimensions[0];

            // std::chrono::system_clock::time_point start_grad = std::chrono::system_clock::now();
            // PORT:
            // calculate Sm.dot prodcut of gradient matrix for relations
            update_gradient_matrix(gradientsR, grp, grn, predicate);
            // std::chrono::duration<double> duration_grad = std::chrono::system_clock::now() - start_grad;
            // LOG(DEBUGL) << "Time to update gradient matrix = " << duration_grad.count() * 1000 << " ms";

            // Object gradients (i.e. when score is violated because of negative object entity)
            // PORT:
            // compute cconv and gejp = gpscores * cconv (E[sp], E[pp])
            EntityGradient gejp(object, dim);
            // start_ccorr = std::chrono::system_clock::now();
            cconv(sp, pp, dim, gejp.dimensions);
            // duration_ccorr = std::chrono::system_clock::now() - start_ccorr;
            // LOG(DEBUGL) << "Time to compute cconv = " << duration_ccorr.count() * 1000 << " ms";
            for (int d = 0; d < dim; ++d) {
                gejp.dimensions[d] *= violatedScorePos;
            }

            // PORT:
            // compute gejn = gnscores * cconv (E[sn], E[pn])
            // in our case pp = pn
            EntityGradient gejn(object, dim);
            // start_ccorr = std::chrono::system_clock::now();
            cconv(sn, pn, dim, gejn.dimensions);
            // duration_ccorr = std::chrono::system_clock::now() - start_ccorr;
            // LOG(DEBUGL) << "Time to compute cconv = " << duration_ccorr.count() * 1000 << " ms";
            for (int d = 0; d < dim; ++d) {
                gejn.dimensions[d] *= violatedScoreNeg;
            }

            // Subject gradients
            EntityGradient geip(subject, dim);
            // start_ccorr = std::chrono::system_clock::now();
            ccorr(pp, op, dim, geip.dimensions);
            // duration_ccorr = std::chrono::system_clock::now() - start_ccorr;
            // LOG(DEBUGL) << "Time to compute ccorr = " << duration_ccorr.count() * 1000 << " ms";
            for (int d = 0; d < dim; ++d) {
                geip.dimensions[d] *= violatedScorePos;
            }

            EntityGradient gein(subject, dim);
            // start_ccorr = std::chrono::system_clock::now();
            cconv(pn, on, dim, gein.dimensions);
            // duration_ccorr = std::chrono::system_clock::now() - start_ccorr;
            // LOG(DEBUGL) << "Time to compute cconv = " << duration_ccorr.count() * 1000 << " ms";
            for (int d = 0; d < dim; ++d) {
                gein.dimensions[d] *= violatedScoreNeg;
            }
            // start_grad = std::chrono::system_clock::now();
            update_gradient_matrix(gradientsE, geip, gein, subject);
            // duration_grad = std::chrono::system_clock::now() - start_grad;
            // LOG(DEBUGL) << "Time to update gradient matrix = " << duration_grad.count() * 1000 << " ms";
            // start_grad = std::chrono::system_clock::now();
            update_gradient_matrix(gradientsE, gejp, gejn, object);
            // duration_grad = std::chrono::system_clock::now() - start_grad;
            // LOG(DEBUGL) << "Time to update gradient matrix = " << duration_grad.count() * 1000 << " ms";
        }

//        if (scorePos - (scoreNegSub + margin) < 0) {
//            io.violations += 1;
//            violatedPositions.push_back(i);
//            violatedScorePos = -sigmoid_given_fun(scorePos);
//            violatedScoreNegSub = -sigmoid_given_fun(scoreNegSub);
//
//            // Predicate/Relation gradients
//            EntityGradient grp(predicate, dim);
//            std::chrono::system_clock::time_point start_ccorr = std::chrono::system_clock::now();
//            ccorr(sp, op, dim, grp.dimensions);
//            std::chrono::duration<double> duration_ccorr = std::chrono::system_clock::now() - start_ccorr;
//            LOG(DEBUGL) << "Time to compute ccorr = " << duration_ccorr.count() * 1000 << " ms";
//
//            for (int d = 0; d < dim; ++d) {
//                grp.dimensions[d] *= violatedScorePos;
//            }
//            EntityGradient grn(predicate, dim);
//            start_ccorr = std::chrono::system_clock::now();
//            ccorr(sn, on, dim, grn.dimensions);
//            duration_ccorr = std::chrono::system_clock::now() - start_ccorr;
//            LOG(DEBUGL) << "Time to compute ccorr = " << duration_ccorr.count() * 1000 << " ms";
//            for (int d = 0; d < dim; ++d) {
//                grn.dimensions[d] *= violatedScoreNegSub;
//            }
//            std::chrono::system_clock::time_point start_grad = std::chrono::system_clock::now();
//            update_gradient_matrix(gradientsR, grp, grn, predicate);
//            std::chrono::duration<double> duration_grad = std::chrono::system_clock::now() - start_grad;
//            LOG(DEBUGL) << "Time to update gradient matrix = " << duration_grad.count() * 1000 << " ms";
//
//            // Subject gradients
//            EntityGradient geip(subject, dim);
//            start_ccorr = std::chrono::system_clock::now();
//            ccorr(pp, op, dim, geip.dimensions);
//            duration_ccorr = std::chrono::system_clock::now() - start_ccorr;
//            LOG(DEBUGL) << "Time to compute ccorr = " << duration_ccorr.count() * 1000 << " ms";
//            for (int d = 0; d < dim; ++d) {
//                geip.dimensions[d] *= violatedScorePos;
//            }
//
//            EntityGradient gein(subject, dim);
//            start_ccorr = std::chrono::system_clock::now();
//            cconv(sn, pp, dim, gein.dimensions);
//            duration_ccorr = std::chrono::system_clock::now() - start_ccorr;
//            LOG(DEBUGL) << "Time to compute cconv = " << duration_ccorr.count() * 1000 << " ms";
//            for (int d = 0; d < dim; ++d) {
//                gein.dimensions[d] *= violatedScoreNegSub;
//            }
//            start_grad = std::chrono::system_clock::now();
//            update_gradient_matrix(gradientsE, geip, gein, subject);
//            duration_grad = std::chrono::system_clock::now() - start_grad;
//            LOG(DEBUGL) << "Time to update gradient matrix = " << duration_grad.count() * 1000 << " ms";
//        }

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

/*
void HoleLearner::update_gradients(BatchIO &io,
	std::vector<EntityGradient> &ge,
	std::vector<EntityGradient> &gr) {
    //Update the gradients of the entities and relations
    if (adagrad) {
	LOG(ERRORL) << "adagrad not supported";
    } else { //sgd
        for (auto &i : ge) {
            double *emb = E->get(i.id);
            auto n = i.n;
            if (n > 0) {
		LOG(INFOL) << "Update E " << i.id << ", emb[0] was " << emb[0] << ", dimensions[0] = " << i.dimensions[0] << ", n = " << n;
                for(int j = 0; j < dim; ++j) {
                    emb[j] = learningrate * emb[j] + i.dimensions[j] / n;
                }
		LOG(INFOL) << "Update E " << i.id << ", emb[0] becomes " << emb[0];
                E->incrUpdates(i.id);
            }
        }
        for (auto &i : gr) {
            double *emb = R->get(i.id);
            auto n = i.n;
            if (n > 0) {
		LOG(INFOL) << "Update R " << i.id << ", emb[0] was " << emb[0];
                for(int j = 0; j < dim; ++j) {
                    emb[j] = learningrate * emb[j] + i.dimensions[j] / n;
                }
		LOG(INFOL) << "Update R " << i.id << ", emb[0] becomes " << emb[0];
                R->incrUpdates(i.id);
            }
        }
    }
}
*/
