#ifndef _DISTMUL_TESTER_H
#define _DISTMUL_TESTER_H

#include <trident/ml/tester.h>

template<typename K>
class DistMulTester : public Tester<K> {
    public:
        DistMulTester(std::shared_ptr<Embeddings<K>> E,
               std::shared_ptr<Embeddings<K>> R) : Tester<K>(E, R) {
        }

        double closeness(K *v1, K *v2, uint16_t dim) {
            double res = 0;
            for(uint16_t i = 0; i < dim; ++i) {
                res += abs(v1[i] - v2[i]);
            }
            return res;
        }

        void predictO(K *s, uint16_t dims, K *p, uint16_t dimp, K* o) {
            for (uint16_t i = 0; i < dims; ++i) {
                o[i] = s[i] + p[i];
            }
        }

        void predictS(K *s, K *p, uint16_t dimp, K* o, uint16_t dimo) {
            for (uint16_t i = 0; i < dimo; ++i) {
                s[i] = o[i] - p[i];
            }
        }
};

#endif
