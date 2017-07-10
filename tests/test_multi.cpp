#include <boost/chrono.hpp>


#include <trident/kb/querier.h>
#include <trident/kb/kb.h>

#include <sstream>
#include <fstream>
#include <iostream>
#include <vector>
#include <cstdlib>
#include <thread>
#include <boost/log/trivial.hpp>

namespace timens = boost::chrono;
using namespace std;


void testKB(Querier *q, std::vector<uint64_t> *v) {
    for(int j = 0; j < 10; j++) {
        for(int i = 0; i < v->size(); i+=3) {
            long s = v->at(i);
            long p = v->at(i+1);
            long o = v->at(i+2);
            if (!q->exists(s,p,o)) {
                cout << "Not found " << s << " " << p << " " << o << endl;
            }
        }
    }
    delete q;
}

int main(int argc, const char** argv) {
    KBConfig config;
    KB kb(argv[1], true, false, false, config);
    //Load the triples
    std::vector<uint64_t> v;
    Querier *q = kb.query();
    auto itr = q->get(IDX_SPO, -1, -1, -1);
    while (itr->hasNext()) {
        itr->next();
        long s = itr->getKey();
        long p = itr->getValue1();
        long o = itr->getValue2();
        v.push_back(s);
        v.push_back(p);
        v.push_back(o);
    }
    delete q;
    cout << "got " << v.size() << " triples" << endl;

    std::vector<std::thread> threads;
    int nthreads = 2;
    for(int i = 0; i < nthreads; ++i) {
        Querier *q = kb.query();
        threads.push_back(std::thread(&testKB, q, &v));
    }
    for(int i = 0; i < nthreads; ++i) {
        threads[i].join();
    }
    return 0;
}
