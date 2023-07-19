#include <trident/iterators/compositetermitr.h>

void CompositeTermItr::add(PairItr *itr) {
        currentCount = 0;
        children.push_back(itr);
        if (itr->hasNext()) {
            itr->next();
            activechildren.push_back(itr);
            nc = true;
        }
        if (activechildren.size() > 1) {
            std::sort(activechildren.begin(), activechildren.end(), _sorter);
        }
    }


