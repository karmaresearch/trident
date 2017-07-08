#ifndef _EMBEDDINGS_H
#define _EMBEDDINGS_H

#include <boost/log/trivial.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <boost/archive/text_iarchive.hpp>
#include <boost/serialization/vector.hpp>

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
            raw.resize((size_t)n * dim);
        }

        Embeddings(const uint32_t n, const uint16_t dim, std::vector<K> &emb): n(n), dim(dim) {
            raw = emb;
        }

        K* get(const uint32_t n) {
            return raw.data() + n * dim;
        }

        uint16_t getDim() const {
            return dim;
        }

        uint32_t getN() const {
            return n;
        }

        std::vector<K> &getAllEmbeddings() {
            return raw;
        }

        const K* getPAllEmbeddings() {
            return raw.data();
        }

        void init(const uint16_t nthreads) {
            K min = -6.0 / sqrt(n + dim);
            K max = 6.0 / sqrt(n + dim);
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
