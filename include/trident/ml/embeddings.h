#ifndef _EMBEDDINGS_H
#define _EMBEDDINGS_H

#include <trident/utils/tridentutils.h>

#include <kognac/utils.h>
#include <kognac/logs.h>

#include <boost/archive/text_oarchive.hpp>
#include <boost/archive/text_iarchive.hpp>
#include <boost/iostreams/filter/gzip.hpp>
#include <boost/iostreams/filtering_stream.hpp>
#include <boost/serialization/vector.hpp>
#include <boost/algorithm/string/predicate.hpp>

#include <vector>
#include <random>
#include <math.h>
#include <cstdint>
#include <thread>
#include <mutex>
#include <fstream>

typedef enum { L1, L2, COSINE, KG } DIST;

template<typename K>
class Embeddings {
    private:
        const uint32_t n;
        const uint16_t dim;
        std::vector<K> raw;

        std::vector<uint8_t> locks;
        std::vector<uint32_t> conflicts;
        std::vector<uint32_t> updates;

        //Statistics
        std::vector<uint32_t> updatesLastEpoch;
        std::vector<uint32_t> updatesThisEpoch;
        uint64_t allUpdatesLastEpoch;
        uint32_t updatedEntitiesLastEpoch;
        uint64_t medianLastEpoch;

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

            //Stats
            updatesLastEpoch.resize(n);
            updatesThisEpoch.resize(n);
            medianLastEpoch = allUpdatesLastEpoch = updatedEntitiesLastEpoch = 0;
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
            updatesThisEpoch[idx]++;
        }

        /*** STATS ***/
        uint64_t getAllUpdatesLastEpoch() {
            return allUpdatesLastEpoch;
        }

        uint32_t getUpdatedEntitiesLastEpoch() {
            return updatedEntitiesLastEpoch;
        }
        uint64_t getMedianUpdatesLastEpoch() {
            return medianLastEpoch;

        }
        /*** END STATS ***/

        uint16_t getDim() const {
            return dim;
        }

        uint32_t getN() const {
            return n;
        }

        void postprocessUpdates() {
            updatesLastEpoch = updatesThisEpoch;
            uint64_t counter = 0;
            uint32_t updatedEntities = 0;
            std::vector<uint64_t> nonzeroupdates;
            for(uint32_t i = 0; i < updatesThisEpoch.size(); ++i) {
                if (updatesThisEpoch[i] > 0) {
                    updatedEntities++;
                    nonzeroupdates.push_back(updatesThisEpoch[i]);
                }
                counter += updatesThisEpoch[i];
                updatesThisEpoch[i] = 0;
            }
            updatedEntitiesLastEpoch = updatedEntities;
            allUpdatesLastEpoch = counter;
            std::sort(nonzeroupdates.begin(), nonzeroupdates.end());
            if (nonzeroupdates.size() > 0) {
                if (nonzeroupdates.size() % 2 == 0) {
                    medianLastEpoch = (nonzeroupdates[nonzeroupdates.size() / 2] + nonzeroupdates[nonzeroupdates.size() / 2 + 1]) / 2;
                } else {
                    medianLastEpoch = nonzeroupdates[nonzeroupdates.size() / 2];
                }
            }
        }

        uint32_t getUpdatesLastEpoch(uint32_t idx) {
            return updatesLastEpoch[idx];
        }

        /*std::vector<K> &getAllEmbeddings() {
          return raw;
          }*/

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
            LOG(DEBUG) << "Done";
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
                K* data = raw.data();
                uint32_t *c = conflicts.data();
                uint32_t *u = updates.data();
                std::vector<std::thread> threads;
                uint64_t batchsize = ((long)n * dim) / nthreads;

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
                    out.write((char*)&batchsize, 4);
                    out.write((char*)&n, 4);
                    out.write((char*)&dim, 2);
                }
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
                    LOG(DEBUG) << "Storing " <<
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

        static std::shared_ptr<Embeddings<K>> load(std::string path) {
            std::vector<std::string> files = Utils::getFilesWithPrefix(
                    Utils::parentDir(path), Utils::filename(path));

            //There should be a file that ends with -meta
            bool metafound = false;
            uint32_t n;
            uint16_t dim;
            uint32_t embperblock = 0;
            for (auto f : files) {
                if (boost::algorithm::ends_with(f, "-meta")) {
                    //Get the metadata
                    std::ifstream ifs;
                    ifs.open(f, std::ifstream::in);
                    boost::iostreams::filtering_stream<boost::iostreams::input> in;
                    in.push(ifs);
                    char buffer[10];
                    in.read(buffer, 10);
                    embperblock = *(uint32_t*) buffer;
                    n = *(uint32_t*)(buffer + 4);
                    dim = *(uint16_t*)(buffer + 8);
                    metafound = true;
                    break;
                }
            }

            if (!metafound) {
                LOG(WARN) << "Embedding file " << path << " not found";
                return std::shared_ptr<Embeddings<double>>();
            } else {
                //Count the files with a number as extension
                std::shared_ptr<Embeddings<double>> emb(new Embeddings(n, dim));

                //Fields
                double *raw = emb->raw.data();
                uint32_t *up = emb->updates.data();
                uint32_t *conf = emb->conflicts.data();

                uint32_t countfile = 0;
                std::string filetoload = path + "." + std::to_string(countfile);
                while (Utils::exists(filetoload)) {
                    std::ifstream ifs;
                    ifs.open(filetoload, std::ifstream::in);
                    boost::iostreams::filtering_stream<boost::iostreams::input> in;
                    in.push(ifs);
                    const uint16_t sizeline = 8 + dim * 8;
                    std::unique_ptr<char> buffer = std::unique_ptr<char>(new char[sizeline]); //one line
                    while(true) {
                        in.read(buffer.get(), sizeline);
                        if (in.eof()) {
                            break;
                        }
                        //Parse the line
                        *conf = *(uint32_t*)buffer.get();
                        *up = *(uint32_t*)(buffer.get()+4);
                        memcpy((char*)raw, buffer.get() + 8, dim * 8);
                        conf += 1;
                        up += 1;
                        raw += dim;
                    }
                    countfile += 1;
                    filetoload = path + "." + std::to_string(countfile);
                }
                return emb;
            }
        }

        /* std::pair<std::shared_ptr<Embeddings<double>>,std::shared_ptr<Embeddings<double>>>
           Learner::loadModel(string path) {
           ifstream ifs;
           ifs.open(path);
           boost::iostreams::filtering_stream<boost::iostreams::input> inp;
           inp.push(boost::iostreams::gzip_decompressor());
           inp.push(ifs);
           boost::archive::text_iarchive ia(inp);
           uint16_t dim;
           ia >> dim;
           uint32_t nr;
           ia >> nr;
           std::vector<double> emb_r;
           ia >> emb_r;
           uint32_t ne;
           ia >> ne;
        //Load R
        std::shared_ptr<Embeddings<double>> R = std::shared_ptr<Embeddings<double>>(
        new Embeddings<double>(nr, dim, emb_r)
        );

        std::vector<double> emb_e;
        emb_e.resize(ne * dim);
        fs::path bpath(path);
        string dirname = bpath.parent_path().string();
        std::vector<string> files_e = Utils::getFilesWithPrefix(
        dirname,
        bpath.filename().string() + ".");

        //Load the files one by one
        uint32_t idxe = 0;
        uint16_t processedfiles = 0;
        while (processedfiles < files_e.size()) {
        string file = dirname + "/" + bpath.filename().string() + "." + to_string(processedfiles);
        LOG(DEBUG) << "Processing file " << file;
        ifstream ifs2;
        ifs2.open(file);
        boost::iostreams::filtering_stream<boost::iostreams::input> inp2;
        inp2.push(boost::iostreams::gzip_decompressor());
        inp2.push(ifs2);
        boost::archive::text_iarchive ia(inp2);
        std::vector<double> values;
        ia >> values;
        //Copy the values into emb_e
        for(size_t i = 0; i < values.size(); ++i) {
        emb_e[idxe++] = values[i];
        }
        processedfiles++;
        }

        //Load E
        std::shared_ptr<Embeddings<double>> E = std::shared_ptr<Embeddings<double>>(
        new Embeddings<double>(ne,dim,emb_e)
        );
        return std::make_pair(E,R);
        }*/

};

#endif
