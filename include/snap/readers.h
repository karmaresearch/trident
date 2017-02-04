#ifndef _READERS_H
#define _READERS_H

#include <trident/binarytables/factorytables.h>

#include <vector>

class KB;
class SnapReaders {
    public:
        typedef long (*pReader)(const char*, const long);
        const static pReader readers[256];
        static std::vector<const char*> f_sop;
        static std::vector<const char*> f_osp;
        static void loadAllFiles(KB *kb);
};

#endif
