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
		bool closeDone = false;
                std::unique_ptr<char> zeros;

            public:
                FlatTreeWriter(string f, bool unlabeled, bool undirected) :
                    unlabeled(unlabeled), undirected(undirected) {
                        ofs.open(f, ios_base::binary);
                        zeros = std::unique_ptr<char>(new char[83]);
                        memset(zeros.get(), 0, 83);
                    }

                void write(const int64_t key,
                        int64_t n_sop,
                        char strat_sop,
                        short file_sop,
                        int64_t pos_sop,
                        int64_t n_osp,
                        char strat_osp,
                        short file_osp,
                        int64_t pos_osp);

                void writeOnlyKey(const int64_t key);

                void write(const int64_t key,
                        int64_t n_sop,
                        char strat_sop,
                        short file_sop,
                        int64_t pos_sop,

                        int64_t n_osp,
                        char strat_osp,
                        short file_osp,
                        int64_t pos_osp,

                        int64_t n_spo,
                        char strat_spo,
                        short file_spo,
                        int64_t pos_spo,

                        int64_t n_ops,
                        char strat_ops,
                        short file_ops,
                        int64_t pos_ops,

                        int64_t n_pos,
                        char strat_pos,
                        short file_pos,
                        int64_t pos_pos,

                        int64_t n_pso,
                        char strat_pso,
                        short file_pso,
                        int64_t pos_pso);

		void done() {
                    ofs.flush();
                    ofs.close();
		    closeDone = true;
		}

                ~FlatTreeWriter() {
		    if (! closeDone) {
			ofs.flush();
			ofs.close();
		    }
                }
        };

        static char rewriteNewColumnStrategy(const char *table);

        static void writeFirstPerm(string sop, Root *root,
                bool unlabeled, bool undirected, string output);

        static void writeOtherPerm(string otherperm, string output,
                int offset,
                int blocksize,
                bool unlabeled);

        const bool unlabeled;
        const bool undirected;
        std::unique_ptr<MemoryMappedFile> file;
        char *raw;
        int sizeblock;
        size_t len;

    public:
        FlatRoot(string path, bool unlabeled, bool undirected);

        bool get(nTerm key, TermCoordinates *value);

        TreeItr *itr();

        static void loadFlatTree(string sop, string osp,
                string spo, string ops,
                string pos, string pso,
                string output,
                Root *tree,
                bool unlabeled,
                bool undirected);

        static void __set(int permid, char *block, TermCoordinates *value);

        ~FlatRoot();
};
#endif
