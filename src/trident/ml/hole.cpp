#include <trident/ml/hole.h>
#include <trident/utils/fft.h>
#include <unordered_map>

inline double sigmoid(double x) {
    if (x < -30) {
	return 0;
    }
    if (x > 30) {
	return 1;
    }
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
	// LOG(DEBUGL) << "i = " << i << ", rel = " << rel[i] << ", tail = " << tail[i] << ", out = " << out[i];
        sum += (rel[i] * out[i]);
    }
    // LOG(DEBUGL) << "Score = " << sum;
    return sum;
}

void HoleLearner::update(BatchIO &io,
	double *sp, double *pp, double *op,
	double *sn, double *on,
	double violatedScorePos, double violatedScoreNeg,
	uint64_t subject, uint64_t predicate, uint64_t object,
	std::unordered_map<uint64_t, EntityGradient> &gradientsE,
	std::unordered_map<uint64_t, EntityGradient> &gradientsR) {

    io.violations += 1;
    // violatedPositions.push_back(i);
    // PORT:
    // sigmoid_given_fun is the function g_give_f from class Sigmoid from
    // file skge/actfun.py
    // LOG(INFOL) << "violatedScoreNeg = " << violatedScoreNeg << ", violatedScorePos = " << violatedScorePos;

    // Predicate/Relation gradients
    // PORT:
    // calculate gpscores * ccorr (E[sp], E[op])
    // and
    // calculate gnscore * ccorr (E[sn], E[op])
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
    // calculate Sm.dot product of gradient matrix for relations
    update_gradient_matrix(gradientsR, grp, grn, predicate);
    // std::chrono::duration<double> duration_grad = std::chrono::system_clock::now() - start_grad;
    // LOG(DEBUGL) << "Time to update gradient matrix = " << duration_grad.count() * 1000 << " ms";

    // Object gradients (i.e. when score is violated because of negative object entity)
    // PORT:
    // compute cconv and gejp = gpscores * cconv (E[sp], R[pp])
    // start_ccorr = std::chrono::system_clock::now();
    if (sp != sn) {
	EntityGradient gejp(object, dim);
	EntityGradient gejn(object, dim);
	cconv(sp, pp, dim, gejp.dimensions);
	// duration_ccorr = std::chrono::system_clock::now() - start_ccorr;
	// LOG(DEBUGL) << "Time to compute cconv = " << duration_ccorr.count() * 1000 << " ms";
	for (int d = 0; d < dim; ++d) {
	    gejp.dimensions[d] *= violatedScorePos;
	}

	// PORT:
	// compute gejn = gnscores * cconv (E[sn], R[pp])
	// start_ccorr = std::chrono::system_clock::now();
	cconv(sn, pp, dim, gejn.dimensions);
	// duration_ccorr = std::chrono::system_clock::now() - start_ccorr;
	// LOG(DEBUGL) << "Time to compute cconv = " << duration_ccorr.count() * 1000 << " ms";
	for (int d = 0; d < dim; ++d) {
	    gejn.dimensions[d] *= violatedScoreNeg;
	}
	update_gradient_matrix(gradientsE, gejp, gejn, object);
    }

    // Subject gradients
    if (op != on) {
	EntityGradient geip(subject, dim);
	EntityGradient gein(subject, dim);
	// start_ccorr = std::chrono::system_clock::now();
	ccorr(pp, op, dim, geip.dimensions);
	// duration_ccorr = std::chrono::system_clock::now() - start_ccorr;
	// LOG(DEBUGL) << "Time to compute ccorr = " << duration_ccorr.count() * 1000 << " ms";
	for (int d = 0; d < dim; ++d) {
	    geip.dimensions[d] *= violatedScorePos;
	}

	// start_ccorr = std::chrono::system_clock::now();
	ccorr(pp, on, dim, gein.dimensions);
	// duration_ccorr = std::chrono::system_clock::now() - start_ccorr;
	// LOG(DEBUGL) << "Time to compute cconv = " << duration_ccorr.count() * 1000 << " ms";
	for (int d = 0; d < dim; ++d) {
	    gein.dimensions[d] *= violatedScoreNeg;
	}
	update_gradient_matrix(gradientsE, geip, gein, subject);
    }
}

void HoleLearner::process_batch_withnegs(BatchIO &io, std::vector<uint64_t> &oneg,
        std::vector<uint64_t> &sneg) {
    std::vector<uint64_t> &output1 = io.field1;
    std::vector<uint64_t> &output2 = io.field2;
    std::vector<uint64_t> &output3 = io.field3;

    //LOG(INFOL) << "$$$ Processing a batch with negs";
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
    // LOG(DEBUGL) << "Time to initialize gradients = " << duration.count() * 1000 << " ms";

    //LOG(DEBUGL) << "UNM$$ starting the batch...";
    // LOG(DEBUGL) << "starting batch of size " << sizebatch;
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

        // LOG(DEBUGL) << "<" << subject << " , " << predicate << " , " << object << ">";
        //Get the distances
        // pp = pn i.e. predicates are not randomly generated for making a sample negative sample
        auto scorePos = sigmoid(score(sp, pp, op));
	// Note: there are two negative samples: <sn, pp, op> and <sp, pp, on>.
        auto scoreNegSub = sigmoid(score(sn, pp, op));
        auto scoreNegObj = sigmoid(score(sp, pp, on));

	// LOG(DEBUGL) << "scoreNegSub = " << scoreNegSub << ", scoreNegObj = " << scoreNegObj << ", scorePos = " << scorePos;

	violatedScorePos = -sigmoid_given_fun(scorePos);

	if (sneg[i] != UINT64_MAX && scoreNegSub + margin > scorePos) {
            violatedScoreNeg = sigmoid_given_fun(scoreNegSub);
	    update(io, sp, pp, op, sn, op, violatedScorePos, violatedScoreNeg, subject, predicate, object, gradientsE, gradientsR);
	}
	if (oneg[i] != UINT64_MAX && scoreNegObj + margin > scorePos) {
            violatedScoreNeg = sigmoid_given_fun(scoreNegObj);
	    update(io, sp, pp, op, sp, on, violatedScorePos, violatedScoreNeg, subject, predicate, object, gradientsE, gradientsR);
	}

        duration = std::chrono::system_clock::now() - start;
        // LOG(DEBUGL) << "Time to process sample (" << i <<")  = " << duration.count() * 1000 << " ms";
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

void HoleLearner::update_gradients(BatchIO &io,
	std::vector<EntityGradient> &ge,
	std::vector<EntityGradient> &gr) {
    //Update the gradients of the entities and relations
    // LOG(INFOL) << "update_gradients called";
    if (adagrad) {
        for(auto &i : ge) {
	    if (i.n == 0) {
		continue;
	    }
            double *emb = E->get(i.id);

            if (gradDebugger) {
                gradDebugger->add(io.epoch, i.id, i.dimensions, i.n);
            }
            if (E->isLocked(i.id)) {
                io.conflicts++;
                E->incrConflict(i.id);
            }
            E->lock(i.id);
            E->incrUpdates(i.id);

            double *pent = pe2.get() + i.id * dim;
            double sum = 0.0; //used for normalization
	    // LOG(INFOL) << "Update E " << i.id << ", emb[0] was " << emb[0] << ", dimensions[0] = " << i.dimensions[0] << ", n = " << i.n;
            for(uint16_t j = 0; j < dim; ++j) {
                const double g = (double)i.dimensions[j] / i.n;
                pent[j] += g * g;
                double spent = sqrt(pent[j]);
                double maxv = max(spent, (double)1e-7);
                emb[j] -= learningrate * g / maxv;
                sum += emb[j] * emb[j];
            }
            //using normless1
            if (sum > 1) {
		for(uint16_t j = 0; j < dim; ++j) {
		    emb[j] = emb[j] / sum;
		}
	    }
	    // LOG(INFOL) << "Update E " << i.id << ", emb[0] becomes " << emb[0] << ", pent[0] = " << pent[0];

            E->unlock(i.id);
        }
        for(auto &i : gr) {
	    if (i.n == 0) {
		continue;
	    }
            double *emb = R->get(i.id);

            if (R->isLocked(i.id)) {
                io.conflicts++;
                R->incrConflict(i.id);
            }
            R->lock(i.id);
            R->incrUpdates(i.id);

            double *pr = pr2.get() + i.id * dim;
            for(uint16_t j = 0; j < dim; ++j) {
                const double g = (double)i.dimensions[j] / i.n;
                pr[j] += g * g;
                double maxv = max(sqrt(pr[j]), (double)1e-7);
                emb[j] -= learningrate * g / maxv;
            }

            R->unlock(i.id);
        }
    } else { //sgd
        for (auto &i : ge) {
            double *emb = E->get(i.id);
            auto n = i.n;
            if (n > 0) {
		// LOG(INFOL) << "Update E " << i.id << ", emb[0] was " << emb[0] << ", dimensions[0] = " << i.dimensions[0] << ", n = " << n;
		double sum = 0.0;
                for(int j = 0; j < dim; ++j) {
                    emb[j] -= learningrate * i.dimensions[j] / n;
		    sum += emb[j] * emb[j];
                }

		// normless1
		if (sum > 1) {
		    for(int j = 0; j < dim; ++j) {
			emb[j] /= sum;
		    }
		}

		// LOG(INFOL) << "Update E " << i.id << ", emb[0] becomes " << emb[0];

                E->incrUpdates(i.id);
            }
        }
        for (auto &i : gr) {
            double *emb = R->get(i.id);
            auto n = i.n;
            if (n > 0) {
		// LOG(INFOL) << "Update R " << i.id << ", emb[0] was " << emb[0];
                for(int j = 0; j < dim; ++j) {
                    emb[j] -= learningrate * i.dimensions[j] / n;
                }
		// LOG(INFOL) << "Update R " << i.id << ", emb[0] becomes " << emb[0];
                R->incrUpdates(i.id);
            }
        }
    }
}
