#include <boost/chrono.hpp>

#include <sstream>
#include <fstream>
#include <iostream>
#include <vector>
#include <cstdlib>

#include <trident/ml/transe.h>
#include <trident/ml/embeddings.h>

#include <kognac/utils.h>

#define DIMS 50

namespace timens = boost::chrono;
using namespace std;

int main(int argc, const char** argv) {
    auto files = Utils::getFiles(argv[1]);
    for(auto f : files) {
        cout << "Testing " << f << endl;
        //Load the datastructures
        std::vector<uint64_t> triples;
        std::vector<uint64_t> negatives;
        ifstream ifs;
        ifs.open(f);
        char buffer[24];
        ifs.read(buffer, 4);
        int nentries = 0;
        nentries = *(int*)buffer;
        for(int j = 0; j < nentries; ++j) {
            ifs.read(buffer, 24);
            //Parse the triple and copy to the array
            long s = *(long*)(buffer);
            long p = *(long*)(buffer + 8);
            long o = *(long*)(buffer + 16);
            triples.push_back(s);
            triples.push_back(p);
            triples.push_back(o);
        }
        //Load a batch of negative values
        ifs.read(buffer, 4);
        nentries = *(int*)buffer;
        for(int j = 0; j < nentries; ++j) {
            ifs.read(buffer, 24);
            //Parse the triple and copy to the array
            long s = *(long*)(buffer);
            long o = *(long*)(buffer + 16);
            negatives.push_back(s);
            negatives.push_back(o);
        }

        //Load the entity and relation embeddings
        ifs.read(buffer, 4);
        int nents = *(int*)buffer;
        std::vector<double> embe_old;
        for(int j = 0; j < nents * DIMS; ++j) {
            ifs.read(buffer, 8);
            double v = *(double*)(buffer);
            embe_old.push_back(v);
        }
        ifs.read(buffer, 4);
        int nrels = *(int*)buffer;
        std::vector<double> embr_old;
        for(int j = 0; j < nrels * DIMS; ++j) {
            ifs.read(buffer, 8);
            double v = *(double*)(buffer);
            embr_old.push_back(v);
        }
        //new
        std::vector<double> embe_new;
        for(int j = 0; j < nents * DIMS; ++j) {
            ifs.read(buffer, 8);
            double v = *(double*)(buffer);
            embe_new.push_back(v);
        }
        ifs.read(buffer, 4);
        std::vector<double> embr_new;
        for(int j = 0; j < nrels * DIMS; ++j) {
            ifs.read(buffer, 8);
            double v = *(double*)(buffer);
            embr_new.push_back(v);
        }
        ifs.close();

        //Load OLD embeddings
        /*std::vector<float> tmpembe;
        std::vector<float> tmpembr;
        for(auto t : embe_old) {
            tmpembe.push_back(t);
        }
        for(auto t : embr_old) {
            tmpembr.push_back(t);
        }*/
        std::shared_ptr<Embeddings<double>> E_old = std::shared_ptr<Embeddings<double>>(new Embeddings<double>(embe_old.size() / DIMS, DIMS, embe_old));
        std::shared_ptr<Embeddings<double>> R_old = std::shared_ptr<Embeddings<double>>(new Embeddings<double>(embr_old.size() / DIMS, DIMS, embr_old));


        //Done with the reading
        Transe tr(150, nents, nrels, DIMS, 2.0, 0.1, 544, true);
        tr.setup(1, E_old, R_old);
        BatchIO io(544, DIMS);
        for(int i = 0; i < triples.size(); i+=3) {
            io.field1.push_back(triples[i]);
            io.field2.push_back(triples[i+1]);
            io.field3.push_back(triples[i+2]);
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
        tr.process_batch(io, oneg, sneg);

        //Load NEW embeddings
        /*std::vector<float> tmpembe_new;
        std::vector<float> tmpembr_new;
        for(auto t : embe_new) {
            tmpembe_new.push_back(t);
        }
        for(auto t : embr_new) {
            tmpembr_new.push_back(t);
        }*/
        std::unique_ptr<Embeddings<double>> E_new = std::unique_ptr<Embeddings<double>>(new Embeddings<double>(embe_new.size() / DIMS, DIMS, embe_new));
        std::unique_ptr<Embeddings<double>> R_new = std::unique_ptr<Embeddings<double>>(new Embeddings<double>(embr_new.size() / DIMS, DIMS, embr_new));


        //Check E
        std::shared_ptr<Embeddings<double>> Ecur = tr.getE();
        for(int i = 0; i < nents; ++i) {
            double *enew1 = E_new->get(i);
            double *enew2 = Ecur->get(i);
            bool broken = false;
            for(int j = 0; j < DIMS; ++j) {
                long e1 = enew1[j] * 6;
                long e2 = enew2[j] * 6;
                if (e1 != e2) {
                    cout << "emb " << i << " d=" << j << " " << enew1[j] << " " << enew2[j] << endl;
                    broken = true;
                }
            }
            if (broken)
                return 0;
            else
                cout << "emb " << i << " is ok" << endl;
        }
        //
        //TODO Check R
    }
    return 0;
}
