#ifndef _EMBEDDINGS_H
#define _EMBEDDINGS_H

#include <boost/log/trivial.hpp>

#include <vector>
#include <random>
#include <math.h>
#include <cstdint>
#include <thread>

template<typename K>
class Embeddings {
    private:
        const uint32_t n;
        const uint16_t dim;
        std::vector<K> raw;

        static void init_seq(K* begin,
                K*end, K min, K max) {
            std::random_device rd;
            std::mt19937 gen(rd());
            std::uniform_real_distribution<> dis(min, max);
            while (begin != end) {
                *begin = dis(gen);
                begin++;
            }
        }

    public:
        Embeddings(const uint32_t n, const uint16_t dim): n(n), dim(dim) {
            raw.resize(n * dim);
        }

        K* get(const uint32_t n) {
            return raw.data() + n * dim;
        }

        void init(const uint16_t nthreads) {
            K min = -6.0 / sqrt(dim);
            K max = 6.0 / sqrt(dim);
            BOOST_LOG_TRIVIAL(debug) << "min=" << min << " max=" << max;
            if (nthreads > 1) {
                uint64_t batchPerThread = raw.size() / nthreads;
                std::vector<std::thread> threads;
                K* begin = raw.data();
                K* end = raw.data() + raw.size();
                while (begin < end) {
                    K* tmpend = (begin + batchPerThread) < end ? begin + batchPerThread : end;
                    threads.push_back(std::thread(Embeddings::init_seq, begin, tmpend,
                                min, max));
                    begin += batchPerThread;
                }
                for(uint16_t i = 0; i < threads.size(); ++i) {
                    threads[i].join();
                }
            } else {
                init_seq(raw.data(), raw.data() + n * dim, min, max);
            }
        }
};

#endif
