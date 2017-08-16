#ifndef _EMBEDDINGS_H
#define _EMBEDDINGS_H

#include <boost/log/trivial.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <boost/archive/text_iarchive.hpp>
#include <boost/iostreams/filter/gzip.hpp>
#include <boost/iostreams/filtering_stream.hpp>
#include <boost/serialization/vector.hpp>

#include <vector>
#include <random>
#include <math.h>
#include <cstdint>
#include <thread>
#include <mutex>
#include <fstream>

template<typename K>
class Embeddings {
    private:
        const uint32_t n;
        const uint16_t dim;
        std::vector<K> raw;

        std::vector<uint8_t> locks;
        std::vector<uint32_t> conflicts;
        std::vector<uint32_t> updates;

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
            locks.resize(n);
            conflicts.resize(n);
            updates.resize(n);
        }

        Embeddings(const uint32_t n, const uint16_t dim,
                std::vector<K> &emb): n(n), dim(dim) {
            raw = emb;
        }

        K* get(const uint32_t n) {
            return raw.data() + n * dim;
        }

        void lock(uint32_t idx) {
            locks[idx]++;
        }

        void unlock(uint32_t idx) {
            locks[idx]--;
        }

        bool isLocked(uint32_t idx) {
            return locks[idx]>0;
        }

        void incrConflict(uint32_t idx) {
            conflicts[idx]++;
        }

        void incrUpdates(uint32_t idx) {
            updates[idx]++;
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

        /*const K* getPAllEmbeddings() {
          return raw.data();
          }*/

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
                    threads.push_back(std::thread(Embeddings::init_seq,
                                begin, tmpend,
                                dim, min, max, normalization));
                    begin += offset;
                }
                for(uint16_t i = 0; i < threads.size(); ++i) {
                    threads[i].join();
                }
            } else {
                init_seq(raw.data(), raw.data() + n * dim, dim, min, max,
                        normalization);
            }
        }

        static void _store_params(std::string path, bool compress,
                const uint16_t dim, const double *b, const double *e,
                const uint32_t *cb, const uint32_t *up) {
            std::ofstream ofs;
            ofs.open(path, std::ofstream::out);
            boost::iostreams::filtering_stream<boost::iostreams::output> out;
            if (compress) {
                out.push(boost::iostreams::gzip_compressor());
            }
            out.push(ofs);
            uint64_t counter = 0;
            while (b != e) {
                if (counter % dim == 0) {
                    //# Conflicts
                    ofs.write((char*)cb, 4);
                    cb++;
                    //# Updates
                    ofs.write((char*)up, 4);
                    up++;
                }
                ofs.write((char*)b, 8);
                b++;
                counter += 1;
            }
            BOOST_LOG_TRIVIAL(debug) << "Done";
        }

        void store(std::string path, bool compress, uint16_t nthreads) {
            if (nthreads == 1) {
                std::ofstream ofs;
                if (compress) {
                    path = path + ".gz";
                }
                ofs.open(path, std::ofstream::out);
                boost::iostreams::filtering_stream<
                    boost::iostreams::output> out;
                if (compress) {
                    out.push(boost::iostreams::gzip_compressor());
                }
                out.push(ofs);
                out.write((char*)&n, 4);
                out.write((char*)&dim, 2);
                K* start  = raw.data();
                K* end = raw.data() + raw.size();
                while (start != end) {
                    out.write((char*)start, 8);
                    start++;
                }
            } else {
                {
                    std::ofstream ofs;
                    std::string metapath = path;
                    if (compress) {
                        metapath = metapath + "-meta.gz";
                    } else {
                        metapath = metapath + "-meta";
                    }
                    ofs.open(metapath, std::ofstream::out);
                    boost::iostreams::filtering_stream<
                        boost::iostreams::output> out;
                    if (compress) {
                        out.push(boost::iostreams::gzip_compressor());
                    }
                    out.push(ofs);
                    out.write((char*)&n, 4);
                    out.write((char*)&dim, 2);
                }
                K* data = raw.data();
                uint32_t *c = conflicts.data();
                uint32_t *u = updates.data();
                std::vector<std::thread> threads;
                uint64_t batchsize = ((long)n * dim) / nthreads;
                uint64_t begin = 0;
                uint16_t idx = 0;
                for(uint16_t i = 0; i < nthreads; ++i) {
                    std::string localpath = path + "." + std::to_string(idx);
                    if (compress)
                        localpath = localpath + ".gz";
                    uint64_t end = begin + batchsize;
                    if (end > ((long)n * dim) || i == nthreads - 1) {
                        end = (long)n * dim;
                    }
                    BOOST_LOG_TRIVIAL(debug) << "Storing " <<
                        (end - begin) << " values in " << localpath << " ...";
                    if (begin < end) {
                        threads.push_back(std::thread(
                                    Embeddings::_store_params,
                                    localpath, compress, dim,
                                    data + begin, data + end,
                                    c, u));
                        c += batchsize / dim;
                        u += batchsize / dim;
                        idx++;
                    }
                    begin = end;
                }
                for(uint16_t i = 0; i < threads.size(); ++i) {
                    threads[i].join();
                }
            }
        }
};

#endif
