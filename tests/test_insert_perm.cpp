#include <trident/kb/kb.h>
#include <trident/kb/kbconfig.h>
#include <trident/loader.h>
#include <trident/kb/memoryopt.h>

int main(int argc, const char** argv) {
    KBConfig config;
    config.setParamInt(DICTPARTITIONS, 1);
    MemoryOptimizer::optimizeForWriting(1000000000, config);
    KB kb(argv[2], false, false, true, config);

    //Insert stuff
    TreeWriter *writer = new TreeWriter(argv[3]);

    Loader::sortAndInsert(0,
                          6,
                          false,
                          argv[1],
                          NULL,
                          writer,
                          kb.insert(),
                          false,
                          false,
                          false,
                          NULL,
                          0,
                          true);

}
