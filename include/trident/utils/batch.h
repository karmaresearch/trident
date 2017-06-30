#ifndef  BATCH_H_
#define BATCH_H_

#include <string>
#include <vector>

#include <boost/interprocess/file_mapping.hpp>
#include <boost/interprocess/mapped_region.hpp>

using namespace std;
namespace bip = boost::interprocess;

class BatchCreator {
    private:
        const string kbdir;
        const uint64_t batchsize;
        const uint16_t nthreads;
        const float valid;
        const float test;

        bip::file_mapping mapping;
        bip::mapped_region mapped_rgn;
        const char* rawtriples;
        uint64_t ntriples;
        std::vector<uint64_t> indices;
        uint64_t currentidx;

        void createInputForBatch(const float valid, const float test);

    public:
        BatchCreator(string kbdir, uint64_t batchsize, uint16_t nthreads,
                const float valid, const float test);

        BatchCreator(string kbdir, uint64_t batchsize, uint16_t nthreads) :
            BatchCreator(kbdir, batchsize, nthreads, 0, 0) {
            }

        void start();

        bool getBatch(std::vector<uint64_t> &output);

        bool getBatch(std::vector<uint64_t> &output1,
                std::vector<uint64_t> &output2,
                std::vector<uint64_t> &output3);

        string getValidPath();

        string getTestPath();

        static void loadTriples(string path, std::vector<uint64_t> &output);
};

#endif
