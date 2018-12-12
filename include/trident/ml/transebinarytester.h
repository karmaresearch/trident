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
            double ret;
            for (uint16_t i = 0; i < dim; ++i) {
                uint64_t result = (((uint64_t)v1[i]) ^ ((uint64_t)v2[i]));
                ret += (64 - __builtin_popcount(result));
            }
            return ret;
        }

        void predictO(uint64_t sub, uint16_t dims, uint64_t pred, uint16_t dimp, K* o) {
            Embeddings<K> *pE = (this->E).get();
            Embeddings<K> *pR = (this->R).get();
            K* s = pE->get(sub);
            K* p = pR->get(pred);
            for (uint16_t i = 0; i < dims; ++i) {
                o[i] = (double) (((uint64_t)s[i]) | ((uint64_t)p[i]));
            }
        }

        void predictS(K *s, uint64_t pred, uint16_t dimp, uint64_t obj, uint16_t dimo) {
            Embeddings<K> *pE = (this->E).get();
            Embeddings<K> *pR = (this->R).get();
            K* o = pE->get(obj);
            K* p = pR->get(pred);
            for (uint16_t i = 0; i < dimp; ++i) {
                s[i] = (double)(~(((uint64_t)o[i]) | ((uint64_t)p[i])));
            }
        }
};

#endif
