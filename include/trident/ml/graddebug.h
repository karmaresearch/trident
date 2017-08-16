#ifndef _GRAD_DEBUG_H
#define _GRAD_DEBUG_H

#include <trident/ml/embeddings.h>

#include <vector>
#include <unordered_set>

class GradTracer {
    private:
        struct _Update {
            uint16_t epoch;
            uint32_t startidx;
            uint16_t n;
        };
        const uint16_t dim;

        std::vector<std::vector<_Update>> updates_meta;
        std::vector<std::vector<float>> updates_values;
        const bool all;
        std::vector<uint32_t> entities;
        std::unordered_set<uint32_t> s_entities;
    public:
        GradTracer(uint32_t ne, int store_ents, uint16_t dim) : dim(dim),
        all(store_ents == -1) {
            if (!all) {
                std::random_device rd;
                std::mt19937 gen(rd());
                std::uniform_int_distribution<> dis(0, ne -1);
                for(int j = 0; j < store_ents; ++j) {
                    uint32_t ent = dis(gen);
                    entities.push_back(ent);
                    s_entities.insert(ent);
                }
            }
            updates_meta.resize(ne);
            updates_values.resize(ne);
        }

        void add(uint16_t epoch, uint32_t id,
                std::vector<float> &features,
                uint32_t n);

        void store(std::string file);
};

#endif
