#ifndef _EMBEDDINGS_H
#define _EMBEDDINGS_H

#include <boost/log/trivial.hpp>

#include <vector>
#include <random>
#include <math.h>
#include <cstdint>

template<typename K>
class Embeddings {
    private:
        const uint32_t n;
        const uint16_t dim;
        std::vector<K> raw;

    public:
        Embeddings(const uint32_t n, const uint16_t dim): n(n), dim(dim) {
            raw.resize(n * dim);
        }

        K* get(const uint32_t n) {
            return raw.data() + n * dim;
        }

        void init() {
            std::random_device rd;
            std::mt19937 gen(rd());
            K min = -6.0 / sqrt(dim);
            K max = 6.0 / sqrt(dim);
            BOOST_LOG_TRIVIAL(debug) << "min=" << min << " max=" << max;
            std::uniform_real_distribution<> dis(min, max);
            K* data = raw.data();
            for (uint32_t i = 0; i < n; i++) {
                for(uint16_t j = 0; j < dim; ++j) {
                    raw[i * dim + j] = dis(gen);
                }
            }
        }
};

#endif
