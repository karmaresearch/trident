#ifndef BATCH_H_
#define BATCH_H_

#include <trident/ml/feedback.h>
#include <trident/utils/memoryfile.h>

#include <string>
#include <vector>
#include <random>

using namespace std;

class BatchCreator {
    private:
        class KBBatch {
            private:
                typedef void (*pReader)(
                        const uint64_t sizetable,
                        const uint8_t offset,
                        const char *start,
                        const uint64_t rowId,
                        uint64_t &v1,
                        uint64_t &v2);

                struct PredCoordinates {
                    uint64_t pred;
                    uint64_t boundary;
                    uint8_t offset; //Used only for the column label
                    uint64_t nfirstterms; //Same as before
                    const char *buffer;
                    pReader reader;
                };

                std::unique_ptr<KB> kb;
                std::unique_ptr<Querier> querier;
                std::vector<PredCoordinates> predicates;
                std::vector<const char*> allposfiles;

            public:

                KBBatch(string kbdir);

                void populateCoordinates();

                void getAt(uint64_t pos,
                        uint64_t &s,
                        uint64_t &p,
                        uint64_t &o);

                uint64_t ntriples();

                ~KBBatch();
        };

        const string kbdir;
        const uint64_t batchsize;
        //const uint16_t nthreads;
        const float valid;
        const float test;
        const bool createBatchFile;
        std::unique_ptr<KBBatch> kbbatch;

        const bool filter;
        const std::shared_ptr<Feedback> feedback;

        int usedIndex;
        std::unique_ptr<MemoryMappedFile> mappedFile;
        const char* rawtriples;
        uint64_t ntriples;
        std::vector<uint64_t> indices;
        uint64_t currentidx;
        std::default_random_engine engine;

        int64_t findFirstOccurrence(int64_t start, int64_t end, uint64_t x, int offset);

        int64_t findLastOccurrence(int64_t start, int64_t end, uint64_t x, int offset);

        bool shouldBeUsed(int64_t s, int64_t p, int64_t o);

        void createInputForBatch(bool createTraining,
                const float valid,
                const float test);

        void populateIndicesFromQuery(int64_t s, int64_t p, int64_t o);

    public:
        BatchCreator(string kbdir, uint64_t batchsize, uint16_t nthreads,
                const float valid, const float test, const bool filter,
                const bool createBatchFile,
                shared_ptr<Feedback> feedback);

        BatchCreator(string kbdir, uint64_t batchsize, uint16_t nthreads) :
            BatchCreator(kbdir, batchsize, nthreads, 0, 0, false, true,
                    std::shared_ptr<Feedback>()) {
            }

        BatchCreator(string kbdir, uint64_t batchsize, uint16_t nthreads,
                const float valid, const float test) :
            BatchCreator(kbdir, batchsize, nthreads, valid, test, false, true,
                    std::shared_ptr<Feedback>()) {
            }

        void start(int64_t s = -1, int64_t p = -1, int64_t o = -1);

        bool getBatch(std::vector<uint64_t> &output);

        bool getBatch(std::vector<uint64_t> &output1,
                std::vector<uint64_t> &output2,
                std::vector<uint64_t> &output3);

        bool getBatchNr(uint64_t nr, std::vector<uint64_t> &output1,
                std::vector<uint64_t> &output2,
                std::vector<uint64_t> &output3);

        uint64_t getNBatches();

        string getValidPath();

        string getTestPath();

        std::shared_ptr<Feedback> getFeedback();

        uint64_t getBatchSize() {
            return batchsize;
        }

        static string getValidPath(string kbdir);

        static string getTestPath(string kbdir);

        static void loadTriples(string path, std::vector<uint64_t> &output);
};

#endif
