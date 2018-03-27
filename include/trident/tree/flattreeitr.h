#ifndef _FLAT_TREEITR_H
#define _FLAT_TREEITR_H

#include <trident/tree/treeitr.h>

class FlatTreeItr: public TreeItr {
    private:
        char *start;
        const char *end;
        const int sizeblock;
        int64_t idx;
        const bool unlabeled;
        const bool undirected;

    public:
        FlatTreeItr(char *start, char *end, int sizeblock,
                bool unlabeled, bool undirected) : start(start),
        end(end), sizeblock(sizeblock), idx(0), unlabeled(unlabeled),
        undirected(undirected) {}

        bool hasNext() {
            return start < end;
        }

        int64_t next(TermCoordinates *value) {
            if (start < end) {
                value->clear();
                FlatRoot::__set(IDX_SOP, start + 5, value);
                if (!unlabeled || !undirected) {
                    FlatRoot::__set(IDX_OSP, start + 18, value);
                }
                if (!unlabeled) {
                    FlatRoot::__set(IDX_SPO, start + 31, value);
                    FlatRoot::__set(IDX_OPS, start + 44, value);
                    FlatRoot::__set(IDX_POS, start + 57, value);
                    FlatRoot::__set(IDX_PSO, start + 70, value);
                }

                start += sizeblock;
                return idx++;
            } else {
                return 0;
            }
        }
};

#endif
