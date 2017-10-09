#ifndef _FLAT_TREE_H
#define _FLAT_TREE_H

#include <trident/tree/root.h>

class FlatRoot : public Root {
    private:
        class FlatTreeWriter {
            private:
                ofstream ofs;
                const bool unlabeled;
                const bool undirected;
                std::unique_ptr<char> zeros;

            public:
                FlatTreeWriter(string f, bool unlabeled, bool undirected) :
                    unlabeled(unlabeled), undirected(undirected) {
                    ofs.open(f);
                    zeros = std::unique_ptr<char>(new char[83]);
                    memset(zeros.get(), 0, 83);
                }

                void write(const long key,
                        long n_sop,
                        char strat_sop,
                        short file_sop,
                        long pos_sop,
                        long n_osp,
                        char strat_osp,
                        short file_osp,
                        long pos_osp);

                void writeOnlyKey(const long key);

                void write(const long key,
                        long n_sop,
                        char strat_sop,
                        short file_sop,
                        long pos_sop,

                        long n_osp,
                        char strat_osp,
                        short file_osp,
                        long pos_osp,

                        long n_spo,
                        char strat_spo,
                        short file_spo,
                        long pos_spo,

                        long n_ops,
                        char strat_ops,
                        short file_ops,
                        long pos_ops,

                        long n_pos,
                        char strat_pos,
                        short file_pos,
                        long pos_pos,

                        long n_pso,
                        char strat_pso,
                        short file_pso,
                        long pos_pso);

                ~FlatTreeWriter() {
                    ofs.flush();
                    ofs.close();
                }
        };

        static char rewriteNewColumnStrategy(const char *table);

        static void writeFirstPerm(string sop, Root *root,
                bool unlabeled, bool undirected, string output);

        static void writeOtherPerm(string otherperm, string output,
                int offset,
                int blocksize);

        const bool unlabeled;
        const bool undirected;
        std::unique_ptr<MemoryMappedFile> file;
        char *raw;
        int sizeblock;

    public:
        FlatRoot(string path, bool unlabeled, bool undirected);

        bool get(nTerm key, TermCoordinates *value);

        static void loadFlatTree(string sop, string osp,
                string spo, string ops,
                string pos, string pso,
                string output,
                Root *tree,
                bool unlabeled,
                bool undirected);

        ~FlatRoot();
};
#endif
