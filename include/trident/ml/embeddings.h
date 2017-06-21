#ifndef _EMBEDDINGS_H
#define _EMBEDDINGS_H

#include <vector>

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
};

#endif
