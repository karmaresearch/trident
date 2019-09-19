#ifndef _HOLE_TESTER_H
#define _HOLE_TESTER_H

#include <trident/ml/tester.h>
#include <trident/utils/fft.h>

template<typename K>
class HoleTester : public Tester<K> {
    public:
        HoleTester(std::shared_ptr<Embeddings<K>> E,
               std::shared_ptr<Embeddings<K>> R, Querier* q) : Tester<K>(E, R, q) {
        }

        HoleTester(std::shared_ptr<Embeddings<K>> E,
               std::shared_ptr<Embeddings<K>> R) : Tester<K>(E, R) {
        }

        // v1 already contains scores
        double closeness(K *v1, uint64_t entity, uint16_t dim) {
            return v1[entity];
        }

        void predictO(uint64_t sub, uint16_t dims, uint64_t pred, uint16_t dimp, K* o) {
            Embeddings<K> *pE = (this->E).get();
            Embeddings<K> *pR = (this->R).get();
            K* s = pE->get(sub);
            K* p = pR->get(pred);
            std::vector<std::vector<double>> ER;
            // for all entities, compute CCORR(p, e)
	    // Note: for optimization: we only have to do this once per predicate? Now we do it for
	    // every test triple. --Ceriel
            for (int s = 0;  s < pE->getN(); ++s) {
                vector<double> out;
                ccorr(p, pE->get(s), dims, out);
                ER.push_back(out);
            }

            // o[j]'s contain the scores for all entities
            for (int j = 0; j < ER.size(); ++j){
                o[j] = 0.0;
                for (uint16_t i = 0; i < dims; ++i) {
                    o[j] += ER[j][i] * s[i];
                }
            }
        }

        void predictS(K *s, uint64_t pred, uint16_t dimp, uint64_t obj, uint16_t dimo) {
            Embeddings<K> *pE = (this->E).get();
            Embeddings<K> *pR = (this->R).get();
            K* p = pR->get(pred);
            // for all entities, compute CCORR(p, e)
            vector<double> ERO;
            for (int s = 0;  s < pE->getN(); ++s) {
                if (s == obj) {
                    ccorr(p, pE->get(s), dimo, ERO);
                    break;
                }
            }
	    // Why not just ccorr(p, pE->get(obj), dimo, ERO)??? --Ceriel

            assert(ERO.size() == dimo);

            // s[i]'s contain scores of all entities
            for (int i = 0; i < pE->getN(); ++i) {
                s[i] = 0.0;
                for (int d = 0; d < dimo; ++d) {
                    s[i] += pE->get(i)[d] * ERO[d];
                }
            }
     }
};

#endif
