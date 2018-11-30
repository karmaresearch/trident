#ifndef _TRANSE_BINARY_TESTER_H
#define _TRANSE_BINARY_TESTER_H

#include <trident/ml/tester.h>

template<typename K>
class TranseBinaryTester : public Tester<K> {
    public:
        TranseBinaryTester(std::shared_ptr<Embeddings<K>> E,
               std::shared_ptr<Embeddings<K>> R, Querier* q) : Tester<K>(E, R, q) {
        }

        TranseBinaryTester(std::shared_ptr<Embeddings<K>> E,
               std::shared_ptr<Embeddings<K>> R) : Tester<K>(E, R) {
        }

        double closeness(K *v1, uint64_t entity, uint16_t dim) {
            double res = 0;
            Embeddings<K> *pE = (this->E).get();
            K* v2 = pE->get(entity);
            return (double)(((uint64_t)*v1) ^ ((uint64_t)*v2));
        }

        void predictO(uint64_t sub, uint16_t dims, uint64_t pred, uint16_t dimp, K* o) {
            Embeddings<K> *pE = (this->E).get();
            Embeddings<K> *pR = (this->R).get();
            K* s = pE->get(sub);
            K* p = pR->get(pred);
            *o = (double) (((uint64_t)*s) | ((uint64_t)*p));
        }

        void predictS(K *s, uint64_t pred, uint16_t dimp, uint64_t obj, uint16_t dimo) {
            Embeddings<K> *pE = (this->E).get();
            Embeddings<K> *pR = (this->R).get();
            K* o = pE->get(obj);
            K* p = pR->get(pred);
            *s = (double)(~(((uint64_t)*o) | ((uint64_t)*p)));
        }
};

#endif
