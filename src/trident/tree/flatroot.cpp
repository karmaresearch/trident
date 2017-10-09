#include <trident/tree/flatroot.h>
#include <trident/binarytables/storagestrat.h>

FlatRoot::FlatRoot(string path, bool unlabeled, bool undirected) :
    unlabeled(unlabeled), undirected(undirected) {
        file = std::unique_ptr<MemoryMappedFile>(new MemoryMappedFile(path, true));
        raw = file->getData();
        if (unlabeled) {
            if (undirected)
                sizeblock = 18;
            else
                sizeblock = 31;
        } else {
            sizeblock = 83;
        }
    }

void __set(int permid, char *block, TermCoordinates *value) {
    const short file = *((short*)(block + 6));
    const long pos = *((long*)(block + 8)) & 0XFFFFFFFFFFl;
    const long nels = *((long*)(block)) & 0XFFFFFFFFFFl;
    const char strat = *(block + 5);
    value->set(permid, file, pos, nels, strat);
}

bool FlatRoot::get(nTerm key, TermCoordinates *value) {
    char *start = raw + key * sizeblock;
    value->clear();
    __set(IDX_SOP, start + 5, value);
    if (!unlabeled || !undirected) {
        __set(IDX_OSP, start + 18, value);
    }
    if (!unlabeled) {
        __set(IDX_SPO, start + 31, value);
        __set(IDX_OPS, start + 44, value);
        __set(IDX_POS, start + 57, value);
        __set(IDX_PSO, start + 70, value);
    }
    return true;
}

char FlatRoot::rewriteNewColumnStrategy(const char *table) {
    const uint8_t header1 = (uint8_t) table[0];
    const uint8_t bytesPerFirstEntry = (header1 >> 3) & 7;
    const uint8_t header2 = (uint8_t) table[1];
    const uint8_t bytesPerStartingPoint =  header2 & 7;
    const uint8_t bytesPerCount = (header2 >> 3) & 7;
    const uint8_t remBytes = bytesPerCount + bytesPerStartingPoint;
    if (bytesPerFirstEntry < 1 || bytesPerFirstEntry > 5) {
        LOG(ERRORL) << "I calculated a maximum of 5 bytes per first entry" << (int) bytesPerFirstEntry;
        throw 10;
    }
    if (remBytes > 16 || remBytes < 2) {
        LOG(ERRORL) << "remBytes range wrong " << (int) remBytes;
        throw 10;
    }
    char out = 0;
    if (bytesPerFirstEntry < 3) {
        out = StorageStrat::setStorageType(out, 1);
        if (bytesPerFirstEntry == 2) {
            out |= 1 << 4;
        }
        out |= remBytes - 2;
    } else if (bytesPerFirstEntry < 5) {
        out = StorageStrat::setStorageType(out, 2);
        if (bytesPerFirstEntry == 4) {
            out |= 1 << 4;
        }
        out |= remBytes - 2;
    } else {
        out = StorageStrat::setStorageType(out, NEWCOLUMN_ITR);
        if (bytesPerFirstEntry == 6) {
            out |= 1 << 4;
        }
        out |= remBytes - 2;
    }
    return out;
}

void FlatRoot::writeFirstPerm(string sop, Root *root, bool unlabeled,
        bool undirected, string output) {
    long keyToAdd = -1;
    std::vector<string> files = Utils::getFiles(sop);
    if (files.size() == 0) {
        //Nothing to do, exit
        return;
    }
    const int maxPossibleIdx = files.size();
    TermCoordinates coord;
    TreeItr *itr = root->itr();
    std::unique_ptr<FlatTreeWriter> ftw =
        std::unique_ptr<FlatTreeWriter>(new FlatTreeWriter(output, unlabeled,
                    undirected));

    for (int i = 0; i < maxPossibleIdx; ++i) {
        if (i > std::numeric_limits<short>::max()) {
            LOG(ERRORL) << "Too many idx files in sop. Cannot create a flat tree";
            throw 10;
        }
        string fidx = sop + "/" + to_string(i) + ".idx";
        if (Utils::exists(fidx)) {
            //Open it, and read the coordinates
            char tmpbuffer[16];
            ifstream ifs;
            ifs.open(fidx);
            ifs.read(tmpbuffer, 8);
            const long nentries = Utils::decode_long(tmpbuffer);

            ifstream rawfile;
            bool rawfileOpened = false;

            for(long entry = 0; entry < nentries; ++entry) {
                ifs.read(tmpbuffer, 11);
                const long key = Utils::decode_longFixedBytes(tmpbuffer + 5, 5);
                const long pos_sop = Utils::decode_longFixedBytes(tmpbuffer, 5);
                char strat_sop = tmpbuffer[10];
                if (StorageStrat::getStorageType(strat_sop) == NEWCOLUMN_ITR) {
                    //Open the file
                    if (!rawfileOpened) {
                        rawfile.open(sop + "/" + to_string(i));
                        rawfileOpened = true;
                    }
                    rawfile.seekg(pos_sop);
                    char tmpbuffer[2];
                    rawfile.read(tmpbuffer, 2);
                    strat_sop = rewriteNewColumnStrategy(tmpbuffer);
                }
                const short file_sop = i;
                long ntree_sop = -1;
                long ntree_osp = -1;
                long ntree_spo = -1;
                long ntree_ops = -1;
                long ntree_pos = -1;
                long ntree_pso = -1;
                while (true) {
                    if(!itr->hasNext()) {
                        LOG(DEBUGL) << "Cannot happen. (loadFlatTree)";
                        throw 10;
                    }
                    const long treeKey = itr->next(&coord);
                    if (treeKey > key) {
                        LOG(DEBUGL) << "Cannot happen (2). (loadFlatTree)";
                        throw 10;
                    }
                    if (coord.exists(IDX_SOP)) {
                        ntree_sop = coord.getNElements(IDX_SOP);
                    } else {
                        ntree_sop = 0;
                    }
                    if (coord.exists(IDX_OSP)) {
                        ntree_osp = coord.getNElements(IDX_OSP);
                    } else {
                        ntree_osp = 0;
                    }

                    if (unlabeled) {
                        ntree_spo = ntree_ops = ntree_pos = ntree_pso = 0;
                    } else {
                        if (coord.exists(IDX_SPO)) {
                            ntree_spo = coord.getNElements(IDX_SPO);
                        } else {
                            ntree_spo = 0;
                        }
                        if (coord.exists(IDX_OPS)) {
                            ntree_ops = coord.getNElements(IDX_OPS);
                        } else {
                            ntree_ops = 0;
                        }
                        if (coord.exists(IDX_POS)) {
                            ntree_pos = coord.getNElements(IDX_POS);
                        } else {
                            ntree_pos = 0;
                        }
                        if (coord.exists(IDX_PSO)) {
                            ntree_pso = coord.getNElements(IDX_PSO);
                        } else {
                            ntree_pso = 0;
                        }

                        if (ntree_osp == 0 && ntree_sop == 0) {
                            LOG(DEBUGL) << "Cannot happen (3). (loadFlatTree)";
                            throw 10;
                        }
                    }

                    //First make sure we have a contiguous array (graph can be disconnected)
                    while (++keyToAdd < treeKey) {
                        ftw->writeOnlyKey(keyToAdd);
                    }
                    if (keyToAdd != treeKey) {
                        LOG(DEBUGL) << "Cannot happen (10). (loadFlatTree)";
                        throw 10;
                    }

                    if (treeKey < key) {
                        //Write the current treekey
                        if (unlabeled) {
                            ftw->write(treeKey, ntree_sop, 0, 0, 0, ntree_osp, 0, 0, 0);
                        } else {
                            ftw->write(treeKey, ntree_sop, 0, 0, 0,
                                    ntree_osp, 0, 0, 0,
                                    ntree_spo, 0, 0, 0,
                                    ntree_ops, 0, 0, 0,
                                    ntree_pos, 0, 0, 0,
                                    ntree_pso, 0, 0, 0);
                        }
                    } else {
                        break;
                    }
                }
                ftw->write(key, ntree_sop, strat_sop, file_sop, pos_sop, ntree_osp, 0, 0, 0);
            }
            ifs.close();
            if (rawfileOpened) {
                rawfile.close();
            }
        }
    }
    while (itr->hasNext()) {
        const long treeKey = itr->next(&coord);
        if (coord.exists(IDX_SOP)) {
            LOG(DEBUGL) << "Cannot happen (4). (loadFlatTree)";
            throw 10;
        }
        long ntree_osp = 0;
        if (coord.exists(IDX_OSP)) {
            ntree_osp = coord.getNElements(IDX_OSP);
        } else {
            LOG(DEBUGL) << "Cannot happen (5). (loadFlatTree)";
            throw 10;
        }
        //First make sure we have a contiguous array (graph can be disconnected)
        while (++keyToAdd < treeKey) {
            ftw->write(keyToAdd, 0, 0, 0, 0, 0, 0, 0, 0);
        }
        ftw->write(treeKey, 0, 0, 0, 0, ntree_osp, 0, 0, 0);
    }
    delete itr;
}

void FlatRoot::writeOtherPerm(string otherperm, string output,
        int offset,
        int blocksize) {
    std::vector<string> files = Utils::getFiles(otherperm);
    if (files.size() == 0) {
        //Nothing to do, exit
        return;
    }
    const int maxPossibleIdx = files.size();

    MemoryMappedFile mf(output, false);
    char *rawbuffer = mf.getData();

    for (int i = 0; i < maxPossibleIdx; ++i) {
        if (i > std::numeric_limits<short>::max()) {
            LOG(DEBUGL) << "Too many idx files in " << otherperm << ". Cannot create a flat tree";
            throw 10;
        }
        string fidx = otherperm + "/" + to_string(i) + ".idx";
        if (Utils::exists(fidx)) {
            //Open it, and read the coordinates
            char tmpbuffer[16];
            ifstream ifs;
            ifs.open(fidx);
            ifs.read(tmpbuffer, 8);
            const long nentries = Utils::decode_long(tmpbuffer);

            ifstream rawfile;
            bool rawfileOpened = false;

            for(long entry = 0; entry < nentries; ++entry) {
                ifs.read(tmpbuffer, 11);
                const long key = Utils::decode_longFixedBytes(tmpbuffer + 5, 5);
                const long pos = Utils::decode_longFixedBytes(tmpbuffer, 5);
                char strat = tmpbuffer[10];
                const short file = i;

                //overwrite strat and pos
                char *baseblock = rawbuffer + key * blocksize;
                if (StorageStrat::getStorageType(strat) == NEWCOLUMN_ITR) {
                    //Open the file
                    if (!rawfileOpened) {
                        rawfile.open(otherperm + "/" + to_string(i));
                        rawfileOpened = true;
                    }
                    rawfile.seekg(pos);
                    char tmpbuffer[2];
                    rawfile.read(tmpbuffer, 2);
                    strat = rewriteNewColumnStrategy(tmpbuffer);
                }

                baseblock[offset++] = strat;
                char *cfile = (char *)&file;
                baseblock[offset++] = cfile[0];
                baseblock[offset++] = cfile[1];
                char *cpos = (char *)&pos;
                baseblock[offset++] = cpos[0];
                baseblock[offset++] = cpos[1];
                baseblock[offset++] = cpos[2];
                baseblock[offset++] = cpos[3];
                baseblock[offset++] = cpos[4];
            }
            if (rawfileOpened) {
                rawfile.close();
            }
            ifs.close();
        }
    }
    mf.flushAll();
}

void FlatRoot::loadFlatTree(string sop,
        string osp,
        string spo,
        string ops,
        string pos,
        string pso,
        string flatfile,
        Root *root,
        bool unlabeled,
        bool undirected) {

    writeFirstPerm(sop, root, unlabeled, undirected, flatfile);

    //OSP
    if (unlabeled) {
        if (!undirected) {
            writeOtherPerm(osp, flatfile, 23, 31);
        }
        //if undirected do nothing nothing
    } else {
        writeOtherPerm(osp, flatfile, 23, 83);
        writeOtherPerm(spo, flatfile, 36, 83);
        writeOtherPerm(ops, flatfile, 49, 83);
        writeOtherPerm(pos, flatfile, 62, 83);
        writeOtherPerm(pso, flatfile, 75, 83);
    }
}


FlatRoot::~FlatRoot() {
}

void FlatRoot::FlatTreeWriter::writeOnlyKey(const long key) {
    char *cKey = (char*) &key;
    ofs.write(cKey, 5);
    //Write zeros
    if (!unlabeled) {
        ofs.write(zeros.get(), 78);
    } else {
        if (!undirected) {
            ofs.write(zeros.get(), 26);
        } else {
            ofs.write(zeros.get(), 13);
        }
    }
}

void FlatRoot::FlatTreeWriter::write(const long key,
        long n_sop,
        char strat_sop,
        short file_sop,
        long pos_sop,
        long n_osp,
        char strat_osp,
        short file_osp,
        long pos_osp) {
    if (!unlabeled) {
        LOG(ERRORL) << "This method should not be invoked for a labeled graph";
        throw 10;
    }

    char supportBuffer[40];
    //key (5 bytes), [nelements (5bytes) strat (1 byte), file (2 bytes), pos (5 bytes)]*2. Written little endian
    char *cKey = (char*) &key;
    supportBuffer[0] = cKey[0];
    supportBuffer[1] = cKey[1];
    supportBuffer[2] = cKey[2];
    supportBuffer[3] = cKey[3];
    supportBuffer[4] = cKey[4];

    if (n_sop > 0) {
        char *cnels = (char*) &n_sop;
        supportBuffer[5] = cnels[0];
        supportBuffer[6] = cnels[1];
        supportBuffer[7] = cnels[2];
        supportBuffer[8] = cnels[3];
        supportBuffer[9] = cnels[4];
        supportBuffer[10] = strat_sop;
        char *cfile = (char*) &file_sop;
        supportBuffer[11] = cfile[0];
        supportBuffer[12] = cfile[1];
        cnels = (char*) &pos_sop;
        supportBuffer[13] = cnels[0];
        supportBuffer[14] = cnels[1];
        supportBuffer[15] = cnels[2];
        supportBuffer[16] = cnels[3];
        supportBuffer[17] = cnels[4];
    } else {
        supportBuffer[5] = 0;
        supportBuffer[6] = 0;
        supportBuffer[7] = 0;
        supportBuffer[8] = 0;
        supportBuffer[9] = 0;
    }
    if (n_osp > 0) {
        char *cnels = (char*) &n_osp;
        supportBuffer[18] = cnels[0];
        supportBuffer[19] = cnels[1];
        supportBuffer[20] = cnels[2];
        supportBuffer[21] = cnels[3];
        supportBuffer[22] = cnels[4];
        supportBuffer[23] = strat_osp;
        char *cfile = (char*) &file_osp;
        supportBuffer[24] = cfile[0];
        supportBuffer[25] = cfile[1];
        cnels = (char*) &pos_osp;
        supportBuffer[26] = cnels[0];
        supportBuffer[27] = cnels[1];
        supportBuffer[28] = cnels[2];
        supportBuffer[29] = cnels[3];
        supportBuffer[30] = cnels[4];
    } else {
        supportBuffer[18] = 0;
        supportBuffer[19] = 0;
        supportBuffer[20] = 0;
        supportBuffer[21] = 0;
        supportBuffer[22] = 0;
    }
    if (!undirected) {
        ofs.write(supportBuffer, 31);
    } else {
        ofs.write(supportBuffer, 18); //I only use SOP, since the other is always 0
    }
}

void FlatRoot::FlatTreeWriter::write(const long key,
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
        long pos_pso
        ) {

            if (unlabeled) {
                LOG(ERRORL) << "This method should not be invoked for a unlabeled graph";
                throw 10;
            }

            char supportBuffer[83];
            char *cKey = (char*) &key;
            supportBuffer[0] = cKey[0];
            supportBuffer[1] = cKey[1];
            supportBuffer[2] = cKey[2];
            supportBuffer[3] = cKey[3];
            supportBuffer[4] = cKey[4];

            if (n_sop > 0) {
                char *cnels = (char*) &n_sop;
                supportBuffer[5] = cnels[0];
                supportBuffer[6] = cnels[1];
                supportBuffer[7] = cnels[2];
                supportBuffer[8] = cnels[3];
                supportBuffer[9] = cnels[4];
                supportBuffer[10] = strat_sop;
                char *cfile = (char*) &file_sop;
                supportBuffer[11] = cfile[0];
                supportBuffer[12] = cfile[1];
                cnels = (char*) &pos_sop;
                supportBuffer[13] = cnels[0];
                supportBuffer[14] = cnels[1];
                supportBuffer[15] = cnels[2];
                supportBuffer[16] = cnels[3];
                supportBuffer[17] = cnels[4];
            } else {
                supportBuffer[5] = 0;
                supportBuffer[6] = 0;
                supportBuffer[7] = 0;
                supportBuffer[8] = 0;
                supportBuffer[9] = 0;
            }
            if (n_osp > 0) {
                char *cnels = (char*) &n_osp;
                supportBuffer[18] = cnels[0];
                supportBuffer[19] = cnels[1];
                supportBuffer[20] = cnels[2];
                supportBuffer[21] = cnels[3];
                supportBuffer[22] = cnels[4];
                supportBuffer[23] = strat_osp;
                char *cfile = (char*) &file_osp;
                supportBuffer[24] = cfile[0];
                supportBuffer[25] = cfile[1];
                cnels = (char*) &pos_osp;
                supportBuffer[26] = cnels[0];
                supportBuffer[27] = cnels[1];
                supportBuffer[28] = cnels[2];
                supportBuffer[29] = cnels[3];
                supportBuffer[30] = cnels[4];
            } else {
                supportBuffer[18] = 0;
                supportBuffer[19] = 0;
                supportBuffer[20] = 0;
                supportBuffer[21] = 0;
                supportBuffer[22] = 0;
            }

            int idx = 23;
            if (n_spo > 0) {
                char *cnels = (char*) &n_spo;
                supportBuffer[idx++] = cnels[0];
                supportBuffer[idx++] = cnels[1];
                supportBuffer[idx++] = cnels[2];
                supportBuffer[idx++] = cnels[3];
                supportBuffer[idx++] = cnels[4];
                supportBuffer[idx++] = strat_spo;
                char *cfile = (char*) &file_spo;
                supportBuffer[idx++] = cfile[0];
                supportBuffer[idx++] = cfile[1];
                cnels = (char*) &pos_spo;
                supportBuffer[idx++] = cnels[0];
                supportBuffer[idx++] = cnels[1];
                supportBuffer[idx++] = cnels[2];
                supportBuffer[idx++] = cnels[3];
                supportBuffer[idx++] = cnels[4];
            } else {
                supportBuffer[idx++] = 0;
                supportBuffer[idx++] = 0;
                supportBuffer[idx++] = 0;
                supportBuffer[idx++] = 0;
                supportBuffer[idx++] = 0;
                idx += 13;
            }

            if (n_ops > 0) {
                char *cnels = (char*) &n_ops;
                supportBuffer[idx++] = cnels[0];
                supportBuffer[idx++] = cnels[1];
                supportBuffer[idx++] = cnels[2];
                supportBuffer[idx++] = cnels[3];
                supportBuffer[idx++] = cnels[4];
                supportBuffer[idx++] = strat_ops;
                char *cfile = (char*) &file_ops;
                supportBuffer[idx++] = cfile[0];
                supportBuffer[idx++] = cfile[1];
                cnels = (char*) &pos_ops;
                supportBuffer[idx++] = cnels[0];
                supportBuffer[idx++] = cnels[1];
                supportBuffer[idx++] = cnels[2];
                supportBuffer[idx++] = cnels[3];
                supportBuffer[idx++] = cnels[4];
            } else {
                supportBuffer[idx++] = 0;
                supportBuffer[idx++] = 0;
                supportBuffer[idx++] = 0;
                supportBuffer[idx++] = 0;
                supportBuffer[idx++] = 0;
                idx += 13;
            }

            if (n_pos > 0) {
                char *cnels = (char*) &n_pos;
                supportBuffer[idx++] = cnels[0];
                supportBuffer[idx++] = cnels[1];
                supportBuffer[idx++] = cnels[2];
                supportBuffer[idx++] = cnels[3];
                supportBuffer[idx++] = cnels[4];
                supportBuffer[idx++] = strat_pos;
                char *cfile = (char*) &file_pos;
                supportBuffer[idx++] = cfile[0];
                supportBuffer[idx++] = cfile[1];
                cnels = (char*) &pos_pos;
                supportBuffer[idx++] = cnels[0];
                supportBuffer[idx++] = cnels[1];
                supportBuffer[idx++] = cnels[2];
                supportBuffer[idx++] = cnels[3];
                supportBuffer[idx++] = cnels[4];
            } else {
                supportBuffer[idx++] = 0;
                supportBuffer[idx++] = 0;
                supportBuffer[idx++] = 0;
                supportBuffer[idx++] = 0;
                supportBuffer[idx++] = 0;
                idx += 13;
            }

            if (n_pso > 0) {
                char *cnels = (char*) &n_pso;
                supportBuffer[idx++] = cnels[0];
                supportBuffer[idx++] = cnels[1];
                supportBuffer[idx++] = cnels[2];
                supportBuffer[idx++] = cnels[3];
                supportBuffer[idx++] = cnels[4];
                supportBuffer[idx++] = strat_pso;
                char *cfile = (char*) &file_pso;
                supportBuffer[idx++] = cfile[0];
                supportBuffer[idx++] = cfile[1];
                cnels = (char*) &pos_pso;
                supportBuffer[idx++] = cnels[0];
                supportBuffer[idx++] = cnels[1];
                supportBuffer[idx++] = cnels[2];
                supportBuffer[idx++] = cnels[3];
                supportBuffer[idx++] = cnels[4];
            } else {
                supportBuffer[idx++] = 0;
                supportBuffer[idx++] = 0;
                supportBuffer[idx++] = 0;
                supportBuffer[idx++] = 0;
                supportBuffer[idx++] = 0;
                idx += 13;
            }

            assert(idx == 83);
            ofs.write(supportBuffer, 83);
        }
