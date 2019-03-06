#ifndef _EMBEDDINGS_H
#define _EMBEDDINGS_H

#include <trident/utils/tridentutils.h>
#include <trident/utils/memoryfile.h>

#include <kognac/utils.h>
#include <kognac/logs.h>

#include <zstr/zstr.hpp>

#include <vector>
#include <random>
#include <math.h>
#include <cstdint>
#include <thread>
#include <mutex>
#include <fstream>
#include <algorithm>

// L3, L4, L5 are custom Variance based distances
// KL is KL divergence
typedef enum { L1 = 1, L2, L3, L4, L5, KL, COSINE, KG} DIST;

template<typename K>
class Embeddings {
    private:
        const uint32_t n;
        const uint16_t dim;

        bool ismem;
        std::string rawFilename;
        std::unique_ptr<MemoryMappedFile> fraw;
        std::vector<K> mraw;

        K* raw;

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
            LOG(DEBUGL) << "Must init " << (end - begin) << " params ...";
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

        //In this case, I force the usage of memory-mapped files
        Embeddings(const uint32_t n, const uint16_t dim, std::string pathFile): n(n), dim(dim) {
            ismem = false;
            rawFilename = pathFile;
            uint64_t rawsize = Utils::fileSize(pathFile);
            LOG(DEBUGL) << "Loading memory mapped file with " << rawsize << " bytes ...";
            fraw = std::unique_ptr<MemoryMappedFile>(new MemoryMappedFile(rawFilename, false, 0, rawsize));
            raw = (K*)fraw->getData();
            LOG(DEBUGL) << "Creating remaining data structures ...";
            locks.resize(n);
            conflicts.resize(n);
            updates.resize(n);

            //Stats
            updatesLastEpoch.resize(n);
            updatesThisEpoch.resize(n);
            medianLastEpoch = allUpdatesLastEpoch = updatedEntitiesLastEpoch = 0;
            LOG(DEBUGL) << "done";
        }

    public:
        Embeddings(const uint32_t n, const uint16_t dim, bool mem = true): n(n), dim(dim) {
            ismem = mem;
            if (mem) {
                mraw.resize((int64_t)n * dim);
                raw = mraw.data();
            } else {
                char fn[] = "mytemp.XXXXXX";
                int fd = mkstemp(fn);
                rawFilename = std::string(fn);
                if (fd == -1) {
                    LOG(ERRORL) << "Could not create temporary file " << rawFilename;
                    throw 10;
                }
                uint64_t rawsize = (int64_t)n * dim * sizeof(K);
                LOG(DEBUGL) << "Resizing the file to " << rawsize << " bytes ...";
                Utils::resizeFile(rawFilename, rawsize);
                LOG(DEBUGL) << "Creating memory mapped file ...";
                fraw = std::unique_ptr<MemoryMappedFile>(new MemoryMappedFile(rawFilename, false, 0, rawsize));
                raw = (K*)fraw->getData();
            }
            LOG(DEBUGL) << "Creating remaining data structures ...";
            locks.resize(n);
            conflicts.resize(n);
            updates.resize(n);

            //Stats
            updatesLastEpoch.resize(n);
            updatesThisEpoch.resize(n);
            medianLastEpoch = allUpdatesLastEpoch = updatedEntitiesLastEpoch = 0;
            LOG(DEBUGL) << "done";
        }

        K* get(const uint32_t n) {
            if (n >= this->n) {
                LOG(ERRORL) << n << " too high";
                throw 10;
            }
            return raw + (int64_t)n * dim;
        }

        K* getRaw() {
            return raw;
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
                K* begin = raw;
                K* end = raw + n;
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
                init_seq(getRaw(), getRaw() + ((size_t) n) * dim, dim, min, max,
                        normalization);
            }
        }

        /*static void _store_params(std::string path, bool compress,
          const uint16_t dim, const double *b, const double *e,
          const uint32_t *cb, const uint32_t *up) {
          if (compress) {
          LOG(ERRORL) << "Not implemented";
          throw 10;
          }
          std::ofstream ofs;
          ofs.open(path, std::ofstream::out);
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
        LOG(DEBUGL) << "Done";
        }

        void store(std::string path, bool compress, uint16_t nthreads) {
        K* data = getRaw();
        uint32_t *c = conflicts.data();
        uint32_t *u = updates.data();
        std::vector<std::thread> threads;
        uint64_t batchsize = (int64_t)n * dim / nthreads;

        {
        std::string metapath = path;
        metapath = metapath + "-meta";
        std::ofstream ofs;
        ofs.open(metapath, std::ofstream::out);
        ofs.write((char*)&batchsize, 4);
        ofs.write((char*)&n, 4);
        ofs.write((char*)&dim, 2);
        }

        uint64_t begin = 0;
        uint16_t idx = 0;
        for(uint16_t i = 0; i < nthreads; ++i) {
        std::string localpath = path + "." + std::to_string(idx);
        if (compress)
        localpath = localpath + ".gz";
        uint64_t end = begin + batchsize;
        if (end > (int64_t)n * dim || i == nthreads - 1) {
        end = (int64_t)n * dim;
        }
        LOG(DEBUGL) << "Storing " <<
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
        }*/

        void store(std::string path, bool compress, uint16_t nthreads) {
            if (compress) {
                LOG(WARNL) << "Storing embeddings in compressed format is disabled";
            }
            if (nthreads > 1) {
                LOG(WARNL) << "Storing the embeddings does not use parallelism";
            }

            uint64_t batchsize = (int64_t)n * dim / nthreads;
            //Store metadata
            {
                std::string metapath = path;
                metapath = metapath + "-meta";
                std::ofstream ofs;
                ofs.open(metapath, std::ofstream::out);
                ofs.write((char*)&batchsize, 4);
                ofs.write((char*)&n, 4);
                ofs.write((char*)&dim, 2);
            }

            if (ismem) {
                //Dump the content of the array into a file as-is
                ofstream dest(path, ios::binary);
                dest.write((char*)mraw.data(), n * dim * sizeof(K));
                dest.close();
            } else {
                //Flush the content to file
                LOG(DEBUGL) << "Flushing the content to disk ...";
                fraw->flush(0, fraw->getLength());

                //Copy the memory-mapped file
                LOG(DEBUGL) << "Copying the embeddings to " << path;
                Utils::copy(rawFilename, path);
            }

            //TODO: Copy data about conflicts, updates

            LOG(DEBUGL) << "done.";
        }

        static std::shared_ptr<Embeddings<K>> load(std::string path) {
            //There should be a file that ends with -meta
            uint32_t n;
            uint16_t dim;
            uint32_t embperblock = 0;
            //Get the metadata
            std::ifstream ifs;
            ifs.open(path + "-meta", std::ifstream::in);
            char buffer[10];
            ifs.read(buffer, 10);
            embperblock = *(uint32_t*) buffer;
            n = *(uint32_t*)(buffer + 4);
            dim = *(uint16_t*)(buffer + 8);

            //Count the files with a number as extension
            std::shared_ptr<Embeddings<double>> emb(new Embeddings(n, dim, path));
            //TODO: load info about conflicts, updates
            return emb;
        }

        /*static std::shared_ptr<Embeddings<K>> load(std::string path) {
          std::vector<std::string> files = Utils::getFilesWithPrefix(
          Utils::parentDir(path), Utils::filename(path));

        //There should be a file that ends with -meta
        bool metafound = false;
        uint32_t n;
        uint16_t dim;
        uint32_t embperblock = 0;
        for (auto f : files) {
        if (Utils::ends_with(f, "-meta")) {
        //Get the metadata
        std::ifstream ifs;
        ifs.open(f, std::ifstream::in);
        char buffer[10];
        ifs.read(buffer, 10);
        embperblock = *(uint32_t*) buffer;
        n = *(uint32_t*)(buffer + 4);
        dim = *(uint16_t*)(buffer + 8);
        metafound = true;
        break;
        }
        }

        if (!metafound) {
        LOG(WARNL) << "Embedding file " << path << " not found";
        return std::shared_ptr<Embeddings<double>>();
        } else {
        //Count the files with a number as extension
        std::shared_ptr<Embeddings<double>> emb(new Embeddings(n, dim));

        //Fields
        double *raw = emb->getRaw();
        uint32_t *up = emb->updates.data();
        uint32_t *conf = emb->conflicts.data();

        uint32_t countfile = 0;
        std::string filetoload = path + "." + std::to_string(countfile);
        while (Utils::exists(filetoload)) {
        std::ifstream ifs;
        ifs.open(filetoload, std::ifstream::in);
        const uint16_t sizeline = 8 + dim * 8;
        std::unique_ptr<char> buffer = std::unique_ptr<char>(new char[sizeline]); //one line
        while(true) {
        ifs.read(buffer.get(), sizeline);
        if (ifs.eof()) {
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
         }*/

        static std::shared_ptr<Embeddings<K>> loadBinary(std::string path) {
            const uint16_t sizeline = 25;
            std::unique_ptr<char> buffer = std::unique_ptr<char>(new char[sizeline]);
            ifstream ifs;
            ifs.open(path, std::ifstream::in);
            ifs.read(buffer.get(), 8);
            uint64_t nSubgraphs = *(uint64_t*)buffer.get();
            LOG(DEBUGL) << "# subgraphs : " << nSubgraphs;
            for (int i = 0; i < nSubgraphs; ++i) {
                ifs.read(buffer.get(), 25);
                /*int type = (int)buffer.get()[0];
                  uint64_t ent = *(uint64_t*) (buffer.get() + 1);
                  uint64_t rel = *(uint64_t*) (buffer.get() + 9);
                  uint64_t siz = *(uint64_t*) (buffer.get() + 17);*/
            }
            memset(buffer.get(), 0, 25);
            ifs.read(buffer.get(), 18);
            uint32_t n = (uint32_t)nSubgraphs;
            uint16_t dim = Utils::decode_short(buffer.get());
            //uint64_t mincard = Utils::decode_long(buffer.get() , 2);
            //uint64_t nextBytes = Utils::decode_long(buffer.get(), 10);
            LOG(INFOL) << dim << " , n = " << n << " nsub = " << nSubgraphs;

            const uint16_t sizeOriginalEmbeddings = dim * 8;
            std::unique_ptr<char> buffer2 = std::unique_ptr<char>(new char[sizeOriginalEmbeddings]);
            for (int i = 0; i < nSubgraphs; ++i) {
                ifs.read(buffer2.get(), dim*8);
            }

            memset(buffer.get(), 0, 25);
            ifs.read(buffer.get(), 2);
            uint16_t compSize = Utils::decode_short(buffer.get());
            LOG(INFOL) << compSize;

            if (compSize % 64 == 0) {
                compSize /= 64;
            }
            std::shared_ptr<Embeddings<double>> emb(new Embeddings(n, compSize));
            //Fields
            double *raw = emb->getRaw();
            const uint16_t sizeCompressedEmbeddings = compSize * 8;
            std::unique_ptr<char> buffer3 = std::unique_ptr<char>(new char[sizeCompressedEmbeddings]);
            for (int i = 0; i < nSubgraphs; ++i) {
                ifs.read(buffer3.get(), compSize*8);
                memcpy((char*)raw, buffer3.get(), compSize * 8);
                raw += compSize;
            }

            ifs.close();
            return emb;
        }
};

#endif
