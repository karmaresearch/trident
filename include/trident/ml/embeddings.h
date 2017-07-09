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
                K*end, uint16_t dim, K min, K max,
                const bool normalization) {
            std::random_device rd;
            std::mt19937 gen(rd());
            std::uniform_real_distribution<> dis(min, max);
            assert(((end - begin) % dim) == 0);
            uint64_t count = 0;
            double sum = 0.0;
            while (begin != end) {
                *begin = dis(gen);

                //Normalization
                if (normalization) {
                    sum += *begin * *begin;
                    if (((count + 1) % dim) == 0) {
                        //Normalize the previous row
                        sum = sqrt(sum);
                        for(uint16_t i = 0; i < dim; ++i) {
                            *(begin - i) /= sum;
                        }
                        sum = 0.0;
                    }
                    count++;
                }

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

        void init(const uint16_t nthreads, const bool normalization) {
            K min = -6.0 / sqrt(n + dim);
            K max = 6.0 / sqrt(n + dim);
            if (nthreads > 1) {
                uint64_t batchPerThread = n / nthreads;
                std::vector<std::thread> threads;
                K* begin = raw.data();
                K* end = raw.data() + raw.size();
                while (begin < end) {
                    uint64_t offset = batchPerThread * dim;
                    K* tmpend = (begin + offset) < end ? begin + offset : end;
                    threads.push_back(std::thread(Embeddings::init_seq, begin, tmpend,
                                dim, min, max, normalization));
                    begin += offset;
                }
                for(uint16_t i = 0; i < threads.size(); ++i) {
                    threads[i].join();
                }
            } else {
                init_seq(raw.data(), raw.data() + n * dim, dim, min, max, normalization);
            }
        }
};

#endif
