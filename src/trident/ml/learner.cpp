#include <trident/ml/learner.h>
#include <kognac/utils.h>

#include <iostream>
#include <cmath>

using namespace std;

LearnParams::LearnParams() {
    //Changeable by the user
    epochs = 100;
    dim = 50;
    margin = 1.0;
    learningrate = 0.1;
    batchsize = 1000;
    adagrad = false;
    nthreads = 1;
    nevalthreads = 1;
    nstorethreads = 1;
    evalits = 10;
    storeits = 10;
    storefolder = "";
    compresstorage = false;
    filetrace = "";
    valid = 0;
    test = 0;
    numneg = 10;
    feedbacks = false;
    feedbacks_threshold = 10;
    feedbacks_minfulle = 20;
    regeneratebatch = true;

    //Non changeable by the user
    ne = 0;
    nr = 0;
}

std::string LearnParams::changeable_tostring() {
    std::string out = "";
    out += "epochs=" + to_string(epochs);
    out += ";dim=" + to_string(dim);
    out += ";margin=" + to_string(margin);
    out += ";learningrate=" + to_string(learningrate);
    out += ";batchsize=" + to_string(batchsize);
    out += ";adagrad=" + to_string(adagrad);
    out += ";nthreads=" + to_string(nthreads);
    out += ";nstorethreads=" + to_string(nevalthreads);
    out += ";nstorethreads=" + to_string(nstorethreads);
    out += ";evalits=" + to_string(evalits);
    out += ";storeits=" + to_string(storeits);
    out += ";storefolder=" + storefolder;
    out += ";compresstorage=" + to_string(compresstorage);
    out += ";filetrace=" + filetrace;
    out += ";valid=" + to_string(valid);
    out += ";test=" + to_string(test);
    out += ";numneg=" + to_string(numneg);
    out += ";feedbacks=" + to_string(feedbacks);
    out += ";feedbacks_threshold=" + to_string(feedbacks_threshold);
    out += ";feedbacks_minfulle=" + to_string(feedbacks_minfulle);
    out += ";regeneratebatch=" + to_string(regeneratebatch);
    return out;
}



std::string LearnParams::tostring() {
    std::string out = changeable_tostring();
    out += ";ne=" + to_string(ne);
    out += ";nr=" + to_string(nr);
    return out;
}

void Learner::setup(const uint16_t nthreads,
        std::shared_ptr<Embeddings<double>> E,
        std::shared_ptr<Embeddings<double>> R,
        std::unique_ptr<double> pe,
        std::unique_ptr<double> pr) {
    this->E = E;
    this->R = R;
    this->pe2 = std::move(pe);
    this->pr2 = std::move(pr);
}

void Learner::setup(const uint16_t nthreads) {
    LOG(DEBUGL) << "Creating E ...";
    std::shared_ptr<Embeddings<double>> E = std::shared_ptr<Embeddings<double>>(new Embeddings<double>(ne, dim));
    //Initialize it
    E->init(nthreads, true);
    LOG(DEBUGL) << "Creating R ...";
    std::shared_ptr<Embeddings<double>> R = std::shared_ptr<Embeddings<double>>(new Embeddings<double>(nr, dim));
    R->init(nthreads, false);

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

float Learner::dist_l1(double* head, double* rel, double* tail,
        float *matrix) {
    float result = 0.0;
    for (uint16_t i = 0; i < dim; ++i) {
        auto value = head[i] + rel[i] - tail[i];
        matrix[i] = value;
        result += abs(value);
    }
    return result;
}

void Learner::store_model(string path,
        const bool compressstorage,
        const uint16_t nthreads) {
    LOG(DEBUGL) << "Start serialization ...";
    Utils::create_directories(path);
    LOG(DEBUGL) << "Serializing R ...";
    R->store(path + "/R", compressstorage, nthreads);
    LOG(DEBUGL) << "Serializing E ...";
    E->store(path + "/E", compressstorage, nthreads);
    LOG(DEBUGL) << "Serialization done";
}

void Learner::update_gradients(BatchIO &io,
        std::vector<EntityGradient> &ge,
        std::vector<EntityGradient> &gr) {
    //Update the gradients of the entities and relations
    if (adagrad) {
        for(auto &i : ge) {
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
            for(uint16_t j = 0; j < dim; ++j) {
                const double g = (double)i.dimensions[j] / i.n;
                pent[j] += g * g;
                double spent = sqrt(pent[j]);
                double maxv = max(spent, (double)1e-7);
                emb[j] -= learningrate * g / maxv;
                sum += emb[j] * emb[j];
            }
            //normalization step
            sum = sqrt(sum);
            for(uint16_t j = 0; j < dim; ++j) {
                emb[j] = emb[j] / sum;
            }

            E->unlock(i.id);
        }
        for(auto &i : gr) {
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
        for (auto &i : gr) {
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
