#include <trident/kb/partial.h>
#include <trident/iterators/reorderitr.h>
#include <kognac/consts.h>
#include <fstream>
#include <ios>

Partial::Partial(int idx, std::string baseDir) {
    // Read index if present
    std::string ifile = baseDir + DIR_SEP + "partialIndex" + std::to_string(idx);
    std::string dfile = baseDir + DIR_SEP + "partialData" + std::to_string(idx);
    std::ifstream is(ifile, std::ios::in | std::ios::binary);
    if (is.is_open()) {
        STriple t;
        uint64_t off;

        while (! is.eof()) {
            is.read(reinterpret_cast<char*>(&t.s), sizeof(int64_t));
            is.read(reinterpret_cast<char*>(&t.p), sizeof(int64_t));
            is.read(reinterpret_cast<char*>(&t.o), sizeof(int64_t));
            is.read(reinterpret_cast<char*>(&off), sizeof(uint64_t));
            index[t] = off;
        }
        is.close();
    }
    indexStream.open(ifile, std::ios::app | std::ios::binary);
    if (! indexStream.is_open()) {
        abort();
    }
    dataStream.open(dfile, std::ios::in|std::ios::app|std::ios::binary);
    if (! dataStream.is_open()) {
        abort();
    }
}

void Partial::dump(ReOrderItr *it) {
    it->dump(indexStream, dataStream, index);
}

ReOrderItr *Partial::getIterator(Querier *q, const int idx, const int64_t s, const int64_t p, const int64_t o) {
    STriple triple(s, p, o);
    auto i = index.find(triple);
    if (i != index.end()) {
        return new ReOrderItr(q, idx, s, p, o, dataStream, i->second);
    }
    return NULL;
}
