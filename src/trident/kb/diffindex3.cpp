/*
 * Copyright 2017 Jacopo Urbani
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
**/


#include <trident/kb/diffindex.h>
#include <trident/tree/root.h>
#include <trident/tree/coordinates.h>
#include <trident/iterators/emptyitr.h>
#include <trident/iterators/diffscanitr.h>
#include <trident/kb/memoryopt.h>
#include <trident/kb/querier.h>
#include <trident/binarytables/storagestrat.h>

#include <kognac/lz4io.h>

#include <cstdlib>

using namespace std;

DiffIndex3::DiffIndex3(std::string dir, const char **globalbuffers,
                       KBConfig &config, TypeUpdate type) :
    DiffIndex(type, DiffIndex::DIFF3), dir(dir) {
    std::chrono::system_clock::time_point startDiff = std::chrono::system_clock::now();
    PropertyMap map;
    map.setBool(TEXT_KEYS, false);
    map.setBool(TEXT_VALUES, false);
    map.setBool(COMPRESSED_NODES, false);
    map.setInt(LEAF_SIZE_PREALL_FACTORY, 0);
    map.setInt(LEAF_SIZE_FACTORY, config.getParamInt(TREE_MAXLEAVESCACHE));
    map.setInt(MAX_NODES_IN_CACHE, config.getParamInt(TREE_MAXNODESINCACHE));
    map.setInt(NODE_MIN_BYTES, config.getParamInt(TREE_NODEMINBYTES));
    map.setLong(CACHE_MAX_SIZE, config.getParamLong(TREE_MAXSIZECACHETREE));
    map.setInt(FILE_MAX_SIZE, config.getParamInt(TREE_MAXFILESIZE));
    map.setInt(MAX_N_OPENED_FILES, config.getParamInt(TREE_MAXNFILES));
    map.setInt(MAX_EL_PER_NODE, config.getParamInt(TREE_MAXELEMENTSNODE));

    map.setInt(LEAF_MAX_INTERNAL_LINES,
               config.getParamInt(TREE_MAXINTERNALLINES));
    map.setInt(LEAF_MAX_PREALL_INTERNAL_LINES, 0);
    map.setInt(LEAF_ARRAYS_FACTORY_SIZE, config.getParamInt(TREE_FACTORYSIZE));
    map.setInt(LEAF_ARRAYS_PREALL_FACTORY_SIZE, 0);

    map.setInt(NODE_KEYS_FACTORY_SIZE,
               config.getParamInt(TREE_NODE_KEYS_FACTORY_SIZE));
    map.setInt(NODE_KEYS_PREALL_FACTORY_SIZE, 0);

    s = std::unique_ptr<Root>(new Root(dir + "/s", NULL, true, map));
    p = std::unique_ptr<Root>(new Root(dir + "/p", NULL, true, map));
    o = std::unique_ptr<Root>(new Root(dir + "/o", NULL, true, map));
    roots[IDX_SPO] = roots[IDX_SOP] = s.get();
    roots[IDX_POS] = roots[IDX_PSO] = p.get();
    roots[IDX_OPS] = roots[IDX_OSP] = o.get();
    strat = NULL;
    std::chrono::duration<double> sec = std::chrono::system_clock::now() - startDiff;
    LOG(DEBUG) << "Load diff tree: " << sec.count() * 1000 << "ms.";

    if (Utils::exists(dir + "/s/p0")) { //The update has its data locally stored
        memset(buffers, 0, sizeof(const char*) * 6);
    } else {
        for (int i = 0; i < 6; ++i)
            buffers[i] = globalbuffers[i];
    }

    ifstream f;
    f.open(dir + "/s/stats");
    char buffer[56];
    f.read(buffer, 56);
    size = Utils::decode_long(buffer);
    nkeys_s = Utils::decode_long(buffer + 8);
    nuniquefirstterms[IDX_SPO] = Utils::decode_long(buffer + 16);
    nfirstterms[IDX_SPO] = Utils::decode_long(buffer + 24);
    nuniquefirstterms[IDX_SOP] = Utils::decode_long(buffer + 32);
    nfirstterms[IDX_SOP] = Utils::decode_long(buffer + 40);
    nuniquekeys[IDX_SPO] = nuniquekeys[IDX_SOP] = Utils::decode_long(buffer + 48);
    f.close();

    f.open(dir + "/p/stats");
    f.read(buffer, 56);
    nkeys_p = Utils::decode_long(buffer + 8);
    nuniquefirstterms[IDX_POS] = Utils::decode_long(buffer + 16);
    nfirstterms[IDX_POS] = Utils::decode_long(buffer + 24);
    nuniquefirstterms[IDX_PSO] = Utils::decode_long(buffer + 32);
    nfirstterms[IDX_PSO] = Utils::decode_long(buffer + 40);
    nuniquekeys[IDX_PSO] = nuniquekeys[IDX_POS] = Utils::decode_long(buffer + 48);
    f.close();

    f.open(dir + "/o/stats");
    f.read(buffer, 56);
    nkeys_o = Utils::decode_long(buffer + 8);
    nuniquefirstterms[IDX_OPS] = Utils::decode_long(buffer + 16);
    nfirstterms[IDX_OPS] = Utils::decode_long(buffer + 24);
    nuniquefirstterms[IDX_OSP] = Utils::decode_long(buffer + 32);
    nfirstterms[IDX_OSP] = Utils::decode_long(buffer + 40);
    nuniquekeys[IDX_OSP] = nuniquekeys[IDX_OPS] = Utils::decode_long(buffer + 48);
    f.close();
    LOG(DEBUG) << "Load diff index with " << size << " triples. N. subjects " << nkeys_s << " N. predicates " << nkeys_p << " N. objects " << nkeys_o << " unique first terms (spo) " << nuniquefirstterms[IDX_SPO] << " (sop) " << nuniquefirstterms[IDX_SOP] << " (pos) " << nuniquefirstterms[IDX_POS] << " (pso) " << nuniquefirstterms[IDX_PSO] << " (ops) " << nuniquefirstterms[IDX_OPS] << " (osp) " << nuniquefirstterms[IDX_OSP] << " first terms (spo) " << nfirstterms[IDX_SPO] << " (sop) " << nfirstterms[IDX_SOP] << " (pos) " << nfirstterms[IDX_POS] << " (pso) " << nfirstterms[IDX_PSO] << " (ops) " << nfirstterms[IDX_OPS] << " (osp) " << nfirstterms[IDX_OSP];
}

EmptyItr _diEmpty;

PairItr *DiffIndex3::getScan(int idx, DiffScanItr *itr) {
    TreeItr *treeitr;
    if (idx == IDX_SPO || idx == IDX_SOP) {
        treeitr = s->itr();
    } else if (idx == IDX_POS || idx == IDX_PSO) {
        treeitr = p->itr();
    } else {
        treeitr = o->itr();
    }
    itr->init(treeitr, idx, this);
    return itr;
}

PairItr *DiffIndex3::getIterator(int idx, long key, TermCoordinates &coord) {

    if (!buffers[idx]) {
        switch (idx) {
        case IDX_SPO:
            spo_f = std::unique_ptr<ROMappedFile>(new ROMappedFile(dir + "/s/p0"));
            buffers[idx] = spo_f->getBuffer();
            break;
        case IDX_SOP:
            sop_f = std::unique_ptr<ROMappedFile>(new ROMappedFile(dir + "/s/p1"));
            buffers[idx] = sop_f->getBuffer();
            break;
        case IDX_POS:
            pos_f = std::unique_ptr<ROMappedFile>(new ROMappedFile(dir + "/p/p0"));
            buffers[idx] = pos_f->getBuffer();
            break;
        case IDX_PSO:
            pso_f = std::unique_ptr<ROMappedFile>(new ROMappedFile(dir + "/p/p1"));
            buffers[idx] = pso_f->getBuffer();
            break;
        case IDX_OPS:
            ops_f = std::unique_ptr<ROMappedFile>(new ROMappedFile(dir + "/o/p0"));
            buffers[idx] = ops_f->getBuffer();
            break;
        case IDX_OSP:
            osp_f = std::unique_ptr<ROMappedFile>(new ROMappedFile(dir + "/o/p1"));
            buffers[idx] = osp_f->getBuffer();
            break;
        }
    }

    long nelements = coord.getNElements(idx);
    size_t idxArray = (coord.getFileIdx(idx) << 16) + coord.getMark(idx);
    char strategy = coord.getStrategy(idx);
    PairItr *itr = strat->getBinaryTable(strategy);
    AbsNewTable *newitr = (AbsNewTable*) itr;
    const char *begin = buffers[idx] + idxArray;
    const char *end;
    if (newitr->getTypeItr() == NEWROW_ITR) {
        end =  begin + nelements *
               (newitr->getReaderSize1() +
                newitr->getReaderSize2() +
                newitr->getReaderCountSize());
    } else { //NEW COLUMN
        int size = Utils::decode_int(begin);
        end = begin + size;
        begin = begin + 4;
    }
    newitr->setup(begin, end);
    return newitr;
}

PairItr *DiffIndex3::getIterator(int idx,
                                 long first,
                                 long second,
                                 long third,
                                 long &nfirstterms) {
    if (first < 0) {
        LOG(ERROR) << "This method should not be called for full scans";
        throw 10;
    }

    TermCoordinates coordinates;
    if (roots[idx]->get(first, &coordinates)) {
        if (!buffers[idx]) {
            switch (idx) {
            case IDX_SPO:
                spo_f = std::unique_ptr<ROMappedFile>(new ROMappedFile(dir + "/s/p0"));
                buffers[idx] = spo_f->getBuffer();
                break;
            case IDX_SOP:
                sop_f = std::unique_ptr<ROMappedFile>(new ROMappedFile(dir + "/s/p1"));
                buffers[idx] = sop_f->getBuffer();
                break;
            case IDX_POS:
                pos_f = std::unique_ptr<ROMappedFile>(new ROMappedFile(dir + "/p/p0"));
                buffers[idx] = pos_f->getBuffer();
                break;
            case IDX_PSO:
                pso_f = std::unique_ptr<ROMappedFile>(new ROMappedFile(dir + "/p/p1"));
                buffers[idx] = pso_f->getBuffer();
                break;
            case IDX_OPS:
                ops_f = std::unique_ptr<ROMappedFile>(new ROMappedFile(dir + "/o/p0"));
                buffers[idx] = ops_f->getBuffer();
                break;
            case IDX_OSP:
                osp_f = std::unique_ptr<ROMappedFile>(new ROMappedFile(dir + "/o/p1"));
                buffers[idx] = osp_f->getBuffer();
                break;
            }
        }
        long nelements = coordinates.getNElements(idx);
        size_t idxArray = (coordinates.getFileIdx(idx) << 16) + coordinates.getMark(idx);
        char strategy = coordinates.getStrategy(idx);
        PairItr *itr = strat->getBinaryTable(strategy);
        AbsNewTable *newitr = (AbsNewTable*) itr;
        const char *begin = buffers[idx] + idxArray;
        const char *end;
        if (newitr->getTypeItr() == NEWROW_ITR) {
            end =  begin + nelements *
                   (newitr->getReaderSize1() +
                    newitr->getReaderSize2() +
                    newitr->getReaderCountSize());
        } else { //NEW COLUMN
            int size = Utils::decode_int(begin);
            end = begin + size;
            begin = begin + 4;
        }

        if (second == -1) {
            //Here I should collect the information about the unique first terms
            int idx2 = (idx + 1) % 3;
            if (idx > 2)
                idx2 += 3;
            nfirstterms += coordinates.getNElements(idx2);
            newitr->setup(begin, end);
        } else {
            if (third == -1) {
                newitr->setup(second, begin, end);
            } else {
                newitr->setup(second, third, begin, end);
            }
            nfirstterms = 1;
        }
        newitr->setKey(first);
        return newitr;
    }
    return &_diEmpty;
}

struct _Sorter {
    const uint64_t *raw;
    _Sorter(std::vector<uint64_t> &elements) {
        raw = &(elements[0]);
    }
    bool operator()(const uint32_t i1, const uint32_t i2) const {
        return raw[i1] < raw[i2];
    }
};

struct _Sorter2 {
    const uint64_t *raw1;
    const uint64_t *raw2;
    _Sorter2(std::vector<uint64_t> &col1,
             std::vector<uint64_t> &col2) {
        raw1 = &(col1[0]);
        raw2 = &(col2[0]);
    }
    bool operator()(const uint32_t i, const uint32_t j) const {
        if (raw1[i] == raw1[j]) {
            return raw2[i] < raw2[j];
        } else {
            return raw1[i] < raw1[j];
        }
    }
};

struct _Sorter3 {
    const uint64_t *raw1;
    const uint64_t *raw2;
    const uint64_t *raw3;
    _Sorter3(std::vector<uint64_t> &col1,
             std::vector<uint64_t> &col2,
             std::vector<uint64_t> &col3) {
        raw1 = &(col1[0]);
        raw2 = &(col2[0]);
        raw3 = &(col3[0]);
    }
    bool operator()(const uint32_t i, const uint32_t j) const {
        if (raw1[i] == raw1[j]) {
            if (raw2[i] == raw2[j]) {
                return raw3[i] < raw3[j];
            } else {
                return raw2[i] < raw2[j];
            }
        } else {
            return raw1[i] < raw1[j];
        }
    }
};

char *_copy33(char *buffer, const long v1, const long v2);
void _sort(std::vector <uint32_t> &idx1,
           std::vector<uint64_t> &firstcolumn,
           std::vector<uint64_t> &secondcolumn,
           std::vector<uint64_t> &thirdcolumn,
           const bool getsecondlevel,
           string file) {
    std::chrono::system_clock::time_point start = std::chrono::system_clock::now();
    std::sort(idx1.begin(), idx1.end(), _Sorter(firstcolumn));
    std::chrono::duration<double> sec = std::chrono::system_clock::now() - start;
    LOG(DEBUG) << "Runtime sorting = " << sec.count() * 1000 << " " << idx1.size();

    //Create file
    size_t sizetable = idx1.size() * 8;
    ofstream f;
    f.open(file);
    f.seekp(sizetable);
    f.put(0);
    f.close();
    memmap::mapped_file_sink sink;
    sink.open(file, sizetable);
    char *currentbuffer = sink.data();

    start = std::chrono::system_clock::now();
    std::vector<std::pair<uint64_t, uint64_t>> secondlevel;
    secondlevel.reserve(idx1.size() / 2);
    uint64_t prevfirst = ~0lu;
    uint64_t prevsecond = ~0lu;
    uint64_t prevthird = ~0lu;
    uint64_t nvalid = 0;
    uint64_t blockstart = 0;
    for (int i = 0; i < idx1.size(); ++i) {
        const size_t j = idx1[i];
        if (firstcolumn[j] != prevfirst || secondcolumn[j] != prevsecond ||
                thirdcolumn[j] != prevthird) {
            if (prevfirst != firstcolumn[j]) {
                const size_t n = i - blockstart;
                if (n > 0) {
                    //Dump it on a file
                    if (n == 1) {
                        currentbuffer = _copy33(currentbuffer,
                                                secondcolumn[idx1[blockstart]],
                                                thirdcolumn[idx1[blockstart]]);
                    } else if (n == 2) {
                        if (secondcolumn[idx1[blockstart]] < secondcolumn[idx1[blockstart + 1]] ||
                                (secondcolumn[idx1[blockstart]] == secondcolumn[idx1[blockstart + 1]]
                                 && thirdcolumn[idx1[blockstart]] < thirdcolumn[idx1[blockstart + 1]])) {
                            currentbuffer = _copy33(currentbuffer,
                                                    secondcolumn[idx1[blockstart]],
                                                    thirdcolumn[idx1[blockstart]]);

                            currentbuffer = _copy33(currentbuffer,
                                                    secondcolumn[idx1[blockstart + 1]],
                                                    thirdcolumn[idx1[blockstart + 1]]);
                        } else {
                            currentbuffer = _copy33(currentbuffer,
                                                    secondcolumn[idx1[blockstart + 1]],
                                                    thirdcolumn[idx1[blockstart + 1]]);

                            currentbuffer = _copy33(currentbuffer,
                                                    secondcolumn[idx1[blockstart]],
                                                    thirdcolumn[idx1[blockstart]]);
                        }
                    } else {
                        //Sort by the second column
                        std::sort(idx1.begin() + blockstart,
                                  idx1.begin() + i, _Sorter2(secondcolumn, thirdcolumn));
                        for (size_t m = blockstart; m < i; ++m) {
                            size_t idxm = idx1[m];
                            currentbuffer = _copy33(currentbuffer,
                                                    secondcolumn[idxm],
                                                    thirdcolumn[idxm]);
                        }
                    }
                }
                blockstart = i;
            }

            if (getsecondlevel && (prevfirst != firstcolumn[j] || prevsecond != secondcolumn[j])) {
                secondlevel.push_back(make_pair(firstcolumn[j], secondcolumn[j]));
            }
            prevfirst = firstcolumn[j];
            prevsecond = secondcolumn[j];
            prevthird = thirdcolumn[j];
            nvalid++;
        }
    }
    sink.close();
    sec = std::chrono::system_clock::now() - start;
    LOG(DEBUG) << "Runtime sorting = " << sec.count() * 1000 << " nvalid=" << nvalid;
}

void DiffIndex3::createDiffIndex(DiffIndex::TypeUpdate update,
                                 string outputdir,
                                 string diffdir,
                                 std::vector<uint64_t> &all_s,
                                 std::vector<uint64_t> &all_p,
                                 std::vector<uint64_t> &all_o,
                                 bool dumpRawFormat,
                                 Querier * q,
                                 bool shouldSort) {
    /**** Create indices ****/
    std::vector <uint32_t> idx1;
    idx1.resize(all_s.size());
    uint32_t *raw_1 = &(idx1[0]);
    for (uint32_t i = 0; i < all_s.size(); ++i) {
        raw_1[i] = i;
    }

    /**** If flag is activated, dump all update in a file (useful for debugging) ****/
    if (dumpRawFormat) {
        Utils::create_directories(outputdir);
        LZ4Writer writer(outputdir + "/raw");
        for (size_t i = 0; i < all_s.size(); ++i) {
            writer.writeLong(all_s[i]);
            writer.writeLong(all_p[i]);
            writer.writeLong(all_o[i]);
        }
    }

    /**** Configuration options for the B+Tree with all keys.
     * I use the same settings as the Trident's tree ****/
    KBConfig c;
    MemoryOptimizer::optimizeForWriting(all_s.size(), c);
    KBConfig *config = &c;

    PropertyMap map;
    map.setBool(TEXT_KEYS, false);
    map.setBool(TEXT_VALUES, false);
    map.setBool(COMPRESSED_NODES, false);
    map.setInt(NODE_MIN_BYTES, config->getParamInt(TREE_NODEMINBYTES));
    map.setInt(FILE_MAX_SIZE, config->getParamInt(TREE_MAXFILESIZE));
    map.setInt(MAX_N_OPENED_FILES, config->getParamInt(TREE_MAXNFILES));
    map.setInt(MAX_EL_PER_NODE, config->getParamInt(TREE_MAXELEMENTSNODE));
    map.setInt(LEAF_SIZE_PREALL_FACTORY,
               config->getParamInt(TREE_MAXPREALLLEAVESCACHE));
    map.setInt(LEAF_SIZE_FACTORY, config->getParamInt(TREE_MAXLEAVESCACHE));
    map.setInt(MAX_NODES_IN_CACHE, config->getParamInt(TREE_MAXNODESINCACHE));
    map.setLong(CACHE_MAX_SIZE, config->getParamLong(TREE_MAXSIZECACHETREE));
    map.setInt(LEAF_MAX_PREALL_INTERNAL_LINES,
               config->getParamInt(TREE_MAXPREALLINTERNALLINES));
    map.setInt(LEAF_MAX_INTERNAL_LINES,
               config->getParamInt(TREE_MAXINTERNALLINES));
    map.setInt(LEAF_ARRAYS_FACTORY_SIZE, config->getParamInt(TREE_FACTORYSIZE));
    map.setInt(LEAF_ARRAYS_PREALL_FACTORY_SIZE,
               config->getParamInt(TREE_ALLOCATEDELEMENTS));
    map.setInt(NODE_KEYS_FACTORY_SIZE,
               config->getParamInt(TREE_NODE_KEYS_FACTORY_SIZE));
    map.setInt(NODE_KEYS_PREALL_FACTORY_SIZE,
               config->getParamInt(TREE_NODE_KEYS_PREALL_FACTORY_SIZE));

    /**** Sort by SPO,SOP ****/
    std::chrono::system_clock::time_point start = std::chrono::system_clock::now();
    Utils::create_directories(outputdir);
    string s_outputdir = outputdir + "/s";
    if (Utils::exists(s_outputdir)) {
        Utils::remove_all(s_outputdir);
    }
    Utils::create_directories(s_outputdir);
    size_t validtriples;
    std::unique_ptr<UpdateStats> ufs;


    if (update == TypeUpdate::ADDITION) {
        ufs = std::unique_ptr<UpdateStats>(new UpdateStats_add(q, IDX_SPO,
                                           IDX_SOP, false, false));
    } else {
        ufs = std::unique_ptr<UpdateStats>(new UpdateStats_rm(q, IDX_SPO,
                                           IDX_SOP, false, false));
    }

    string s_diffdir = diffdir + "/s";
    Utils::create_directories(s_diffdir);
    validtriples = DiffIndex3::sortIndex(s_outputdir, s_diffdir, IDX_SPO, IDX_SOP, idx1,
                                         all_s, all_p, all_o, map, q, ufs.get(), shouldSort);

    std::chrono::duration<double> sec = std::chrono::system_clock::now() - start;
    LOG(DEBUG) << "Runtime sort and create s* indices = " << sec.count() * 1000;

    /**** Sort by POS,PSO ****/
    start = std::chrono::system_clock::now();
    string p_outputdir = outputdir + "/p";
    if (Utils::exists(p_outputdir)) {
        Utils::remove_all(p_outputdir);
    }
    Utils::create_directories(p_outputdir);
    std::unique_ptr<UpdateStats> ufp;

    if (update == TypeUpdate::ADDITION) {
        ufp = std::unique_ptr<UpdateStats>(new UpdateStats_add(q, IDX_POS, IDX_PSO, true, true));
    } else {
        ufp = std::unique_ptr<UpdateStats>(new UpdateStats_rm(q, IDX_POS, IDX_PSO, true, true));
    }
    string p_diffdir = diffdir + "/p";
    Utils::create_directories(p_diffdir);
    DiffIndex3::sortIndex(p_outputdir, p_diffdir, IDX_POS, IDX_PSO,
                          idx1, all_p, all_o, all_s, map, q, ufp.get(), true);

    sec = std::chrono::system_clock::now() - start;
    LOG(DEBUG) << "Runtime sort and create p* indices = " << sec.count() * 1000;

    /**** Sort by OPS,OSP ****/
    start = std::chrono::system_clock::now();
    string o_outputdir = outputdir + "/o";
    if (Utils::exists(o_outputdir)) {
        Utils::remove_all(o_outputdir);
    }
    Utils::create_directories(o_outputdir);
    std::unique_ptr<UpdateStats> ufo;

    if (update == TypeUpdate::ADDITION) {
        ufo = std::unique_ptr<UpdateStats>(new UpdateStats_add(q, IDX_OPS, IDX_OSP, false, true));
    } else {
        ufo = std::unique_ptr<UpdateStats>(new UpdateStats_rm(q, IDX_OPS, IDX_OSP, false, true));
    }
    string o_diffdir = diffdir + "/o";
    Utils::create_directories(o_diffdir);
    DiffIndex3::sortIndex(o_outputdir, o_diffdir, IDX_OPS, IDX_OSP,
                          idx1, all_o, all_p, all_s, map, q, ufo.get(), true);


    sec = std::chrono::system_clock::now() - start;
    LOG(DEBUG) << "Runtime sort and create o* indices = " << sec.count() * 1000;

    /**** Sort the inverted pairs ****/
    start = std::chrono::system_clock::now();
    std::vector<uint64_t> &invertedpairsOP = ufp->getInvertedPairs1();
    std::vector<uint64_t> &invertedpairsSP = ufp->getInvertedPairs2();
    std::vector<uint64_t> &invertedpairsSO = ufo->getInvertedPairs2();
    std::sort(invertedpairsOP.begin(), invertedpairsOP.end());
    std::sort(invertedpairsSP.begin(), invertedpairsSP.end());
    std::sort(invertedpairsSO.begin(), invertedpairsSO.end());
    sec = std::chrono::system_clock::now() - start;
    LOG(DEBUG) << "Runtime sort unique pairs = " << sec.count() * 1000;

    start = std::chrono::system_clock::now();
    std::vector<UpdateStats::KeyInfo> &keysS = ufs->getAllKeys();
    std::vector<UpdateStats::KeyInfo> &keysP = ufp->getAllKeys();
    std::vector<UpdateStats::KeyInfo> &keysO = ufo->getAllKeys();
    //I must copy all unique terms for the S tree, and one for the O key
    //SP
    if (invertedpairsSP.size() > 0) {
        size_t idxSP = 0;
        long prevkey = invertedpairsSP[0];
        uint64_t count = 0;
        uint64_t idxKeysS = 0;
        while (idxSP < invertedpairsSP.size()) {
            if (invertedpairsSP[idxSP] != prevkey) {
                while (idxKeysS < keysS.size() && keysS[idxKeysS].key < prevkey) {
                    idxKeysS++;
                }
                assert(keysS[idxKeysS].key == prevkey);
                keysS[idxKeysS].nfirsts1 = count;
                count = 0;
                prevkey = invertedpairsSP[idxSP];
                assert(prevkey >= 0);
            }
            count++;
            idxSP++;
        }
        if (count > 0) {
            while (idxKeysS < keysS.size() && keysS[idxKeysS].key < prevkey) {
                idxKeysS++;
            }
            assert(keysS[idxKeysS].key == prevkey);
            keysS[idxKeysS].nfirsts1 = count;
        }
    }
    //SO
    if (invertedpairsSO.size() > 0) {
        size_t idxSO = 0;
        long prevkey = invertedpairsSO[0];
        uint64_t count = 0;
        uint64_t idxKeysS = 0;
        while (idxSO < invertedpairsSO.size()) {
            if (invertedpairsSO[idxSO] != prevkey) {
                while (idxKeysS < keysS.size() && keysS[idxKeysS].key < prevkey) {
                    idxKeysS++;
                }
                assert(keysS[idxKeysS].key == prevkey);
                keysS[idxKeysS].nfirsts2 = count;
                count = 0;
                prevkey = invertedpairsSO[idxSO];
            }
            count++;
            idxSO++;
        }
        if (count > 0) {
            while (idxKeysS < keysS.size() && keysS[idxKeysS].key < prevkey) {
                idxKeysS++;
            }
            assert(keysS[idxKeysS].key == prevkey);
            keysS[idxKeysS].nfirsts2 = count;
        }
    }
    sec = std::chrono::system_clock::now() - start;
    LOG(DEBUG) << "Copying the S* unique pair counts = " << sec.count() * 1000;
    //OP
    start = std::chrono::system_clock::now();
    if (invertedpairsOP.size() > 0) {
        size_t idxOP = 0;
        long prevkey = invertedpairsOP[0];
        uint64_t count = 0;
        size_t idxKeysO = 0;
        while (idxOP < invertedpairsOP.size()) {
            if (invertedpairsOP[idxOP] != prevkey) {
                while (idxKeysO < keysO.size() && keysO[idxKeysO].key < prevkey) {
                    idxKeysO++;
                }
                assert(keysO[idxKeysO].key == prevkey);
                keysO[idxKeysO].nfirsts1 = count;
                count = 0;
                prevkey = invertedpairsOP[idxOP];
            }
            count++;
            idxOP++;
        }
        if (count > 0) {
            while (idxKeysO < keysO.size() && keysO[idxKeysO].key < prevkey) {
                idxKeysO++;
            }
            assert(keysO[idxKeysO].key == prevkey);
            keysO[idxKeysO].nfirsts1 = count;
        }
    }
    sec = std::chrono::system_clock::now() - start;
    LOG(DEBUG) << "Copying the O* unique pair counts = " << sec.count() * 1000;

    //Writing the trees on disk
    start = std::chrono::system_clock::now();
    {
        TermCoordinates coord;
        coord.clear();
        Root root(outputdir + "/s", NULL, false, map);
        for (size_t i = 0; i < keysS.size(); ++i) {
            uint32_t pos1 = keysS[i].pos1;
            uint32_t pos2 = keysS[i].pos2;
            coord.set(IDX_SPO, pos1 >> 16, pos1 & 0xFFFF, keysS[i].nelements, keysS[i].strat1);
            coord.set(IDX_SOP, pos2 >> 16, pos2 & 0xFFFF, keysS[i].nelements, keysS[i].strat2);
            //Add the unique terms as two other permutations. This is a dirty hack to reuse
            //the space we already have available in the tree.
            coord.set((IDX_SPO + 1) % 3, 0, 0, keysS[i].nfirsts1, 0);
            coord.set((IDX_SOP + 1) % 3 + 3, 0, 0, keysS[i].nfirsts2, 0);
            root.append(keysS[i].key, &coord);
        }
    }
    {
        TermCoordinates coord;
        coord.clear();
        Root root(outputdir + "/p", NULL, false, map);
        for (size_t i = 0; i < keysP.size(); ++i) {
            uint32_t pos1 = keysP[i].pos1;
            uint32_t pos2 = keysP[i].pos2;
            coord.set(IDX_POS, pos1 >> 16, pos1 & 0xFFFF, keysP[i].nelements, keysP[i].strat1);
            coord.set(IDX_PSO, pos2 >> 16, pos2 & 0xFFFF, keysP[i].nelements, keysP[i].strat2);
            //Add the unique terms as two other permutations. This is a dirty hack to reuse
            //the space we already have available in the tree.
            coord.set((IDX_POS + 1) % 3, 0, 0, keysP[i].nfirsts1, 0);
            coord.set((IDX_PSO + 1) % 3 + 3, 0, 0, keysP[i].nfirsts2, 0);
            root.append(keysP[i].key, &coord);
        }
    }
    {
        TermCoordinates coord;
        coord.clear();
        Root root(outputdir + "/o", NULL, false, map);
        for (size_t i = 0; i < keysO.size(); ++i) {
            uint32_t pos1 = keysO[i].pos1;
            uint32_t pos2 = keysO[i].pos2;
            coord.set(IDX_OPS, pos1 >> 16, pos1 & 0xFFFF, keysO[i].nelements, keysO[i].strat1);
            coord.set(IDX_OSP, pos2 >> 16, pos2 & 0xFFFF, keysO[i].nelements, keysO[i].strat2);
            //Add the unique terms as two other permutations. This is a dirty hack to reuse
            //the space we already have available in the tree.
            coord.set((IDX_OPS + 1) % 3, 0, 0, keysO[i].nfirsts1, 0);
            coord.set((IDX_OSP + 1) % 3 + 3, 0, 0, keysO[i].nfirsts2, 0);
            root.append(keysO[i].key, &coord);
        }
    }
    sec = std::chrono::system_clock::now() - start;
    LOG(DEBUG) << "Runtime to write the B+Trees on disk = " << sec.count() * 1000;

    //Write some general statistics for the S* indices
    char buffer[8];
    ofstream f;
    f.open(outputdir + "/s/stats");
    Utils::encode_long(buffer, validtriples);
    f.write(buffer, 8);
    Utils::encode_long(buffer, ufs->getTotalKeys());
    f.write(buffer, 8);
    Utils::encode_long(buffer, ufp->getInvertedPairs2().size()); //Got it from PSO
    f.write(buffer, 8);
    Utils::encode_long(buffer, ufs->getTotalCount1());
    f.write(buffer, 8);
    Utils::encode_long(buffer, ufo->getInvertedPairs2().size()); //Got it from OSP
    f.write(buffer, 8);
    Utils::encode_long(buffer, ufs->getTotalCount2());
    f.write(buffer, 8);
    Utils::encode_long(buffer, ufs->getNewKeys());
    f.write(buffer, 8);
    f.close();
    //Write some general statistics for the P* indices
    f.open(outputdir + "/p/stats");
    Utils::encode_long(buffer, validtriples);
    f.write(buffer, 8);
    Utils::encode_long(buffer, ufp->getTotalKeys());
    f.write(buffer, 8);
    Utils::encode_long(buffer, ufp->getCount1());
    f.write(buffer, 8);
    Utils::encode_long(buffer, ufp->getTotalCount1());
    f.write(buffer, 8);
    Utils::encode_long(buffer, ufp->getCount2());
    f.write(buffer, 8);
    Utils::encode_long(buffer, ufp->getTotalCount2());
    f.write(buffer, 8);
    Utils::encode_long(buffer, ufp->getNewKeys());
    f.write(buffer, 8);
    f.close();
    //Write some general statistics for the O* indices
    f.open(outputdir + "/o/stats");
    Utils::encode_long(buffer, validtriples);
    f.write(buffer, 8);
    Utils::encode_long(buffer, ufo->getTotalKeys());
    f.write(buffer, 8);
    Utils::encode_long(buffer, invertedpairsOP.size());
    f.write(buffer, 8);
    Utils::encode_long(buffer, ufo->getTotalCount1());
    f.write(buffer, 8);
    Utils::encode_long(buffer, ufo->getCount2());
    f.write(buffer, 8);
    Utils::encode_long(buffer, ufo->getTotalCount2());
    f.write(buffer, 8);
    Utils::encode_long(buffer, ufo->getNewKeys());
    f.write(buffer, 8);
    f.close();

}

void DiffIndex3::getStrategyAndInserter(const std::vector<uint64_t> &tmp1,
                                        const std::vector<uint64_t> &tmp2,
                                        char &strat1,
                                        char &strat2,
                                        char &flagbytes1,
                                        char &flagbytes2) {
    uint8_t nbytes1 = Utils::numBytesFixedLength(tmp1.back() >> 32);
    flagbytes1 = 3;
    if (nbytes1 == 1) {
        flagbytes1 = 0;
    } else if (nbytes1 == 2) {
        flagbytes1 = 1;
    } else if (nbytes1 < 5) {
        flagbytes1 = 2;
    }
    uint8_t nbytes2 = Utils::numBytesFixedLength(tmp2.back() >> 32);
    flagbytes2 = 3;
    if (nbytes2 == 1) {
        flagbytes2 = 0;
    } else if (nbytes2 == 2) {
        flagbytes2 = 1;
    } else if (nbytes2 < 5) {
        flagbytes2 = 2;
    }
    strat1 = strat2 = 0;
    strat1 = StorageStrat::setStorageType(strat1, NEWROW_ITR);
    strat1 = StorageStrat::setBytesField1(strat1, flagbytes1);
    strat1 = StorageStrat::setBytesField2(strat1, flagbytes2);
    strat2 = StorageStrat::setStorageType(strat2, NEWROW_ITR);
    strat2 = StorageStrat::setBytesField1(strat2, flagbytes2);
    strat2 = StorageStrat::setBytesField2(strat2, flagbytes1);
}

char *_copy11(char *buffer, const long v1, const long v2) {
    buffer[0] = v1;
    buffer[1] = v2;
    return buffer + 2;
}
char *_copy12(char *buffer, const long v1, const long v2) {
    buffer[0] = v1;
    Utils::encode_short(buffer + 1, v2);
    return buffer + 3;
}
char *_copy13(char *buffer, const long v1, const long v2) {
    buffer[0] = v1;
    Utils::encode_int(buffer + 1, v2);
    return buffer + 5;
}
char *_copy14(char *buffer, const long v1, const long v2) {
    buffer[0] = v1;
    Utils::encode_long(buffer + 1, v2);
    return buffer + 9;
}
char *_copy21(char *buffer, const long v1, const long v2) {
    Utils::encode_short(buffer, v1);
    buffer[2] = v2;
    return buffer + 3;
}
char *_copy22(char *buffer, const long v1, const long v2) {
    Utils::encode_short(buffer, v1);
    Utils::encode_short(buffer + 2, v2);
    return buffer + 4;
}
char *_copy23(char *buffer, const long v1, const long v2) {
    Utils::encode_short(buffer, v1);
    Utils::encode_int(buffer + 2, v2);
    return buffer + 6;
}
char *_copy24(char *buffer, const long v1, const long v2) {
    Utils::encode_short(buffer, v1);
    Utils::encode_long(buffer + 2, v2);
    return buffer + 10;
}
char *_copy31(char *buffer, const long v1, const long v2) {
    Utils::encode_int(buffer, v1);
    buffer[4] = v2;
    return buffer + 5;
}
char *_copy32(char *buffer, const long v1, const long v2) {
    Utils::encode_int(buffer, v1);
    Utils::encode_short(buffer + 4, v2);
    return buffer + 6;
}
char *_copy33(char *buffer, const long v1, const long v2) {
    Utils::encode_int(buffer, v1);
    Utils::encode_int(buffer + 4, v2);
    return buffer + 8;
}
char *_copy34(char *buffer, const long v1, const long v2) {
    Utils::encode_int(buffer, v1);
    Utils::encode_long(buffer + 4, v2);
    return buffer + 12;
}
char *_copy41(char *buffer, const long v1, const long v2) {
    Utils::encode_long(buffer, v1);
    buffer[8] = v2;
    return buffer + 9;
}
char *_copy42(char *buffer, const long v1, const long v2) {
    Utils::encode_long(buffer, v1);
    Utils::encode_short(buffer + 8, v2);
    return buffer + 10;
}
char *_copy43(char *buffer, const long v1, const long v2) {
    Utils::encode_long(buffer, v1);
    Utils::encode_int(buffer + 8, v2);
    return buffer + 12;
}
char *_copy44(char *buffer, const long v1, const long v2) {
    Utils::encode_long(buffer, v1);
    Utils::encode_long(buffer + 8, v2);
    return buffer + 16;
}
char *(*inserters[16])(char*, const long, const long) = {_copy11, _copy12, _copy13, _copy14,
                                                         _copy21, _copy22, _copy23, _copy24,
                                                         _copy31, _copy32, _copy33, _copy34,
                                                         _copy41, _copy42, _copy43, _copy44
                                                        };

uint64_t DiffIndex3::storeRowFormat(const std::vector<uint64_t> &tmp1,
                                    const std::vector<uint64_t> &tmp2,
                                    char *&buffer1,
                                    char *&buffer2,
                                    long * counters1,
                                    long * counters2,
                                    UpdateStats *stats,
                                    char &strat1,
                                    char &strat2) {
    char flagbytes1, flagbytes2;
    DiffIndex3::getStrategyAndInserter(tmp1, tmp2,
                                       strat1, strat2,
                                       flagbytes1, flagbytes2);
    assert(flagbytes1 < 4 && flagbytes1 >= 0);
    assert(flagbytes2 < 4 && flagbytes2 >= 0);

    long prev1 = -1;
    long prev2 = -1;

    //I need these two variables to remove potential duplicates in the table
    uint64_t prevpair1 = ~0u;
    uint64_t prevpair2 = ~0u;
    uint64_t countprevpair1 = 0;
    uint64_t countprevpair2 = 0;
    uint64_t validrows = 0;
    uint64_t validrows2 = 0;

    //Copy the values
    assert(((flagbytes1 << 2) + flagbytes2) < 16);
    assert(((flagbytes2 << 2) + flagbytes1) < 16);
    char *(*ins1)(char*, const long, const long) = inserters[(flagbytes1 << 2) + flagbytes2];
    char *(*ins2)(char*, const long, const long) = inserters[(flagbytes2 << 2) + flagbytes1];
    for (size_t i = 0; i < tmp1.size(); ++i) {
        if (tmp1[i] != prevpair1) {
            long v1 = tmp1[i] >> 32;
            long v2 = tmp1[i] & ((uint32_t) - 1);
            buffer1 = ins1(buffer1, v1, v2);

            if (v1 != prev1) {
                if (prev1 != -1)
                    stats->addFirst1(prev1, validrows - countprevpair1);
                prev1 = v1;
                countprevpair1 = validrows;
            }
            prevpair1 = tmp1[i];
            validrows++;
        }

        if (tmp2[i] != prevpair2) {
            long v1 = tmp2[i] >> 32;
            long v2 = tmp2[i] & ((uint32_t) - 1);
            buffer2 = ins2(buffer2, v1, v2);

            if (v1 != prev2) {
                if (prev2 != -1)
                    stats->addFirst2(prev2, validrows2 - countprevpair2);
                prev2 = v1;
                countprevpair2 = validrows2;
            }
            prevpair2 = tmp2[i];
            validrows2++;
        }
    }

    if (validrows > 0) {
        stats->addFirst1(prev1, validrows - countprevpair1);
        stats->addFirst2(prev2, validrows2 - countprevpair2);
    }
    counters1[(flagbytes1 << 2) + flagbytes2] += tmp1.size();
    counters2[(flagbytes2 << 2) + flagbytes1] += tmp1.size();
    return validrows;
}

uint64_t DiffIndex3::storeRowColumnFormat(const std::vector<uint64_t> &tmp1,
        const std::vector<uint64_t> &tmp2,
        char *&buffer1,
        char *&buffer2,
        long * counters1,
        long * counters2,
        UpdateStats *stats,
        char &strat1,
        char &strat2,
        const bool invertCounters) {
    strat1 = strat2 = 0;

    //Set up first table with row layout
    char flagbytes1, flagbytes2;
    DiffIndex3::getStrategyAndInserter(tmp1, tmp2,
                                       strat1, strat2,
                                       flagbytes1, flagbytes2);
    assert(flagbytes1 < 4 && flagbytes1 >= 0);
    long prev1 = -1;
    long countprev1 = 0;
    uint64_t prevpair1 = ~0u;
    uint64_t validrows = 0;
    assert(((flagbytes1 << 2) + flagbytes2) < 16);
    char *(*ins1)(char*, const long, const long) = inserters[(flagbytes1 << 2) + flagbytes2];

    //Set up second table with column layout
    char *beginTable2 = buffer2;
    buffer2 += 4;
    strat2 = 0;
    strat2 = StorageStrat::setStorageType(strat2, NEWCOLUMN_ITR);
    const uint8_t bytesPerFirstEntry = Utils::numBytesFixedLength(tmp1.back() >> 32);
    const uint8_t bytesPerSecondEntry = Utils::numBytesFixedLength(tmp2.back() >> 32);
    const uint8_t bytesPerCount = Utils::numBytesFixedLength(tmp1.size());
    const uint8_t bytesPerOffset = Utils::numBytesFixedLength(tmp1.size());
    const uint8_t header2 = (bytesPerCount << 3) + (bytesPerOffset & 7);
    const uint8_t header1table2 = (bytesPerSecondEntry << 3) + (bytesPerFirstEntry & 7);
    buffer2[0] = header1table2;
    buffer2[1] = header2;
    buffer2 += 2;
    char *posNFirstTerms2 = buffer2;
    buffer2 += 4; //Reserve 4 bytes for later...
    buffer2 += Utils::encode_vlong2(buffer2, 0, tmp1.size());
    long prevfirsttable2 = -1;
    uint64_t prevpair2 = ~0u;
    char *pointerPrevCount2 = NULL;
    long previousNValid2 = 0;
    int nfirstterms2 = 0;
    long validrows2 = 0;

    //Copy the values
    for (size_t i = 0; i < tmp1.size(); ++i) {
        if (tmp1[i] != prevpair1) {
            long v1 = tmp1[i] >> 32;
            long v2 = tmp1[i] & ((uint32_t) - 1);
            buffer1 = ins1(buffer1, v1, v2);

            if (v1 != prev1) {
                if (prev1 != -1) {
                    if (!invertCounters)
                        stats->addFirst1(prev1, validrows - countprev1);
                    else
                        stats->addFirst2(prev1, validrows - countprev1);
                }
                prev1 = v1;
                countprev1 = validrows;
            }
            prevpair1 = tmp1[i];
            validrows++;
        }

        if (tmp2[i] != prevpair2) {
            const long v = tmp2[i] >> 32;
            if (v != prevfirsttable2) {
                if (prevfirsttable2 != -1) {
                    if (!invertCounters)
                        stats->addFirst2(prevfirsttable2, validrows2 - previousNValid2);
                    else
                        stats->addFirst1(prevfirsttable2, validrows2 - previousNValid2);
                }
                //Write the term
                Utils::encode_longNBytes(buffer2, bytesPerSecondEntry, v);
                buffer2 += bytesPerSecondEntry;
                //If pointer is not NULL, write previous count
                if (pointerPrevCount2) {
                    Utils::encode_longNBytes(pointerPrevCount2,
                                             bytesPerCount,
                                             validrows2 - previousNValid2);
                }
                pointerPrevCount2 = buffer2;
                //Skip the count, I will add later
                buffer2 += bytesPerCount;
                //Write offset
                Utils::encode_longNBytes(buffer2, bytesPerOffset, validrows2);
                buffer2 += bytesPerOffset;
                previousNValid2 = validrows2;
                nfirstterms2++;
                prevfirsttable2 = v;
            }
            validrows2++;
            prevpair2 = tmp2[i];
        }
    }

    if (!invertCounters)
        stats->addFirst1(prev1, validrows - countprev1);
    else
        stats->addFirst2(prev1, validrows - countprev1);

    counters1[(flagbytes1 << 2) + flagbytes2] += tmp1.size();
    if (pointerPrevCount2) {
        Utils::encode_longNBytes(pointerPrevCount2,
                                 bytesPerCount,
                                 validrows2 - previousNValid2);
        if (!invertCounters)
            stats->addFirst2(prevfirsttable2, validrows2 - previousNValid2);
        else
            stats->addFirst1(prevfirsttable2, validrows2 - previousNValid2);
    }
    Utils::encode_vlong2_fixedLen(posNFirstTerms2, nfirstterms2, 4);

    //Write second entries
    prevpair2 = ~0u;
    for (size_t i = 0; i < tmp1.size(); ++i) {
        if (tmp2[i] != prevpair2) {
            const long v = tmp2[i] & ((uint32_t) - 1);
            Utils::encode_longNBytes(buffer2, bytesPerFirstEntry, v);
            buffer2 += bytesPerFirstEntry;
            prevpair2 = tmp2[i];
        }
    }
    //Write the table size at the very beginning
    Utils::encode_int(beginTable2, buffer2 - beginTable2);

    return validrows;
}

uint64_t DiffIndex3::storeColumnFormat(const std::vector<uint64_t> &tmp1,
                                       const std::vector<uint64_t> &tmp2,
                                       char *&buffer1,
                                       char *&buffer2,
                                       UpdateStats *stats,
                                       char &strat1,
                                       char &strat2) {
    char *beginTable1 = buffer1;
    char *beginTable2 = buffer2;
    buffer1 += 4;
    buffer2 += 4;

    uint64_t validrows1 = 0;
    uint64_t validrows2 = 0;
    strat1 = strat2 = 0;
    strat1 = StorageStrat::setStorageType(strat1, NEWCOLUMN_ITR);
    strat2 = StorageStrat::setStorageType(strat2, NEWCOLUMN_ITR);

    //First determine the size of the maximum element in both columns
    const uint8_t bytesPerFirstEntry = Utils::numBytesFixedLength(tmp1.back() >> 32);
    const uint8_t bytesPerSecondEntry = Utils::numBytesFixedLength(tmp2.back() >> 32);
    const uint8_t bytesPerCount = Utils::numBytesFixedLength(tmp1.size());
    const uint8_t bytesPerOffset = Utils::numBytesFixedLength(tmp1.size());

    //Write the header on table 1 and table 2
    const uint8_t header1table1 = (bytesPerFirstEntry << 3) + (bytesPerSecondEntry & 7);
    const uint8_t header2 = (bytesPerCount << 3) + (bytesPerOffset & 7);
    buffer1[0] = header1table1;
    buffer1[1] = header2;
    buffer1 += 2;
    const uint8_t header1table2 = (bytesPerSecondEntry << 3) + (bytesPerFirstEntry & 7);
    buffer2[0] = header1table2;
    buffer2[1] = header2;
    buffer2 += 2;

    //Write size unique terms
    char *posNFirstTerms1 = buffer1;
    char *posNFirstTerms2 = buffer2;
    buffer1 += 4;
    buffer2 += 4; //Reserve 4 bytes for later...
    buffer1 += Utils::encode_vlong2(buffer1, 0, tmp1.size());
    buffer2 += Utils::encode_vlong2(buffer2, 0, tmp1.size());

    long prevfirsttable1 = -1;
    uint64_t prevpair1 = ~0u;
    char *pointerPrevCount1 = NULL;
    long previousNValid1 = 0;
    int nfirstterms1 = 0;

    long prevfirsttable2 = -1;
    uint64_t prevpair2 = ~0u;
    char *pointerPrevCount2 = NULL;
    long previousNValid2 = 0;
    int nfirstterms2 = 0;

    for (size_t i = 0; i < tmp1.size(); ++i) {
        if (tmp1[i] != prevpair1) {
            const long v = tmp1[i] >> 32;
            if (v != prevfirsttable1) {
                if (prevfirsttable1 != -1)
                    stats->addFirst1(prevfirsttable1, validrows1 - previousNValid1);

                //Write the term
                Utils::encode_longNBytes(buffer1, bytesPerFirstEntry, v);
                buffer1 += bytesPerFirstEntry;
                //If pointer is not NULL, write previous count
                if (pointerPrevCount1) {
                    Utils::encode_longNBytes(pointerPrevCount1,
                                             bytesPerCount,
                                             validrows1 - previousNValid1);
                }
                pointerPrevCount1 = buffer1;
                //Skip the count, I will add later
                buffer1 += bytesPerCount;
                //Write offset
                Utils::encode_longNBytes(buffer1, bytesPerOffset, validrows1);
                buffer1 += bytesPerOffset;
                previousNValid1 = validrows1;
                nfirstterms1++;
                prevfirsttable1 = v;
            }
            validrows1++;
            prevpair1 = tmp1[i];
        }

        if (tmp2[i] != prevpair2) {
            const long v = tmp2[i] >> 32;
            if (v != prevfirsttable2) {
                if (prevfirsttable2 != -1)
                    stats->addFirst2(prevfirsttable2, validrows2 - previousNValid2);

                //Write the term
                Utils::encode_longNBytes(buffer2, bytesPerSecondEntry, v);
                buffer2 += bytesPerSecondEntry;
                //If pointer is not NULL, write previous count
                if (pointerPrevCount2) {
                    Utils::encode_longNBytes(pointerPrevCount2,
                                             bytesPerCount,
                                             validrows2 - previousNValid2);
                }
                pointerPrevCount2 = buffer2;
                //Skip the count, I will add later
                buffer2 += bytesPerCount;
                //Write offset
                Utils::encode_longNBytes(buffer2, bytesPerOffset, validrows2);
                buffer2 += bytesPerOffset;
                previousNValid2 = validrows2;
                nfirstterms2++;
                prevfirsttable2 = v;
            }
            validrows2++;
            prevpair2 = tmp2[i];
        }
    }
    //Write the last count
    if (pointerPrevCount1) {
        Utils::encode_longNBytes(pointerPrevCount1,
                                 bytesPerCount,
                                 validrows1 - previousNValid1);
        stats->addFirst1(prevfirsttable1, validrows1 - previousNValid1);
    }
    if (pointerPrevCount2) {
        Utils::encode_longNBytes(pointerPrevCount2,
                                 bytesPerCount,
                                 validrows2 - previousNValid2);
        stats->addFirst2(prevfirsttable2, validrows2 - previousNValid2);
    }
    //Write the number of first terms at the beginning
    Utils::encode_vlong2_fixedLen(posNFirstTerms1, nfirstterms1, 4);
    Utils::encode_vlong2_fixedLen(posNFirstTerms2, nfirstterms2, 4);

    //Write second entries
    prevpair1 = ~0u;
    prevpair2 = ~0u;
    for (size_t i = 0; i < tmp1.size(); ++i) {
        if (tmp1[i] != prevpair1) {
            const long v = tmp1[i] & ((uint32_t) - 1);
            Utils::encode_longNBytes(buffer1, bytesPerSecondEntry, v);
            buffer1 += bytesPerSecondEntry;
            prevpair1 = tmp1[i];
        }
        if (tmp2[i] != prevpair2) {
            const long v = tmp2[i] & ((uint32_t) - 1);
            Utils::encode_longNBytes(buffer2, bytesPerFirstEntry, v);
            buffer2 += bytesPerFirstEntry;
            prevpair2 = tmp2[i];
        }
    }
    //Write the table size at the very beginning
    Utils::encode_int(beginTable1, buffer1 - beginTable1);
    Utils::encode_int(beginTable2, buffer2 - beginTable2);

    return validrows1;
}

bool DiffIndex3::shouldUseColumns(const std::vector<uint64_t> &table) {
    int countrepet = 0;
    for (int i = 0; i < 5; ++i) {
        //jump to a random idx
        const int idx = rand() % table.size();
        const long key = table[idx] >> 32;
        countrepet++;
        for (int j = idx; j < idx + 5 && idx < table.size(); ++j) {
            const long ckey = table[j] >> 32;
            if (key == ckey) {
                countrepet++;
            } else {
                break;
            }
        }

    }
    if (countrepet / 5 > 3) {
        return true;
    } else {
        return false;
    }
}

uint64_t DiffIndex3::storeTablesOnBuffer(const long key,
        const std::vector<uint64_t> &tmp1,
        const std::vector<uint64_t> &tmp2,
        const int perm1,
        const int perm2,
        std::unique_ptr<RWMappedFile> &mappedFile1,
        std::unique_ptr<RWMappedFile> &mappedFile2,
        StatStrategy & stats,
        long * counters1,
        long * counters2,
        UpdateStats *statsFirstTerms) {

    statsFirstTerms->setKey(key, tmp1.size());
    long unique1 = statsFirstTerms->getCount1();
    long unique2 = statsFirstTerms->getCount2();

    char strat1, strat2;
    RWMappedFile::Block block1;
    RWMappedFile::Block block2;
    char *buffer1 = NULL;
    char *buffer2 = NULL;
    uint64_t validrows = 0;
    //if (true) {
    if (tmp1.size() < 64) {
        const size_t sizetab1 = tmp1.size() * 8;
        block1 = mappedFile1->getNewBlock(sizetab1);
        buffer1 = block1.buffer;
        block2 = mappedFile2->getNewBlock(sizetab1);
        buffer2 = block2.buffer;

        //Store the table in row format
        validrows = storeRowFormat(tmp1, tmp2, buffer1, buffer2,
                                   counters1, counters2,
                                   statsFirstTerms,
                                   strat1, strat2);
        stats.bothRowsSmallCounter++;
    } else {
        //Should I store both in column format or only one of them?
        const size_t sizetabrow = tmp1.size() * 8;
        const size_t sizetabcol = tmp1.size() * 16;
        bool column1 = shouldUseColumns(tmp1);
        bool column2 = shouldUseColumns(tmp2);
        if (!column1 && !column2) {
            block1 = mappedFile1->getNewBlock(sizetabrow);
            buffer1 = block1.buffer;
            block2 = mappedFile2->getNewBlock(sizetabrow);
            buffer2 = block2.buffer;

            validrows = storeRowFormat(tmp1, tmp2, buffer1, buffer2,
                                       counters1, counters2,
                                       statsFirstTerms,
                                       strat1, strat2);
            stats.bothRowsCounter++;
        } else if (!column1 && column2) {
            block1 = mappedFile1->getNewBlock(sizetabrow);
            buffer1 = block1.buffer;
            block2 = mappedFile2->getNewBlock(sizetabcol);
            buffer2 = block2.buffer;

            validrows = storeRowColumnFormat(tmp1, tmp2, buffer1, buffer2,
                                             counters1, counters2,
                                             statsFirstTerms,
                                             strat1, strat2, false);
            stats.rowColumnCounter++;
        } else if (column1 && !column2) {
            block1 = mappedFile1->getNewBlock(sizetabcol);
            buffer1 = block1.buffer;
            block2 = mappedFile2->getNewBlock(sizetabrow);
            buffer2 = block2.buffer;

            validrows = storeRowColumnFormat(tmp2, tmp1, buffer2, buffer1,
                                             counters2, counters1,
                                             statsFirstTerms,
                                             strat2, strat1, true);
            stats.columnRowCounter++;
        } else {
            block1 = mappedFile1->getNewBlock(sizetabcol);
            buffer1 = block1.buffer;
            block2 = mappedFile2->getNewBlock(sizetabcol);
            buffer2 = block2.buffer;

            validrows = storeColumnFormat(tmp1, tmp2, buffer1, buffer2,
                                          statsFirstTerms,
                                          strat1, strat2);
            stats.bothColumnsCounter++;
        }
    }

    uint32_t pos1 = block1.begin;
    uint32_t pos2 = block2.begin;
    mappedFile1->setLastBlockSize(buffer1 - block1.buffer);
    mappedFile2->setLastBlockSize(buffer2 - block2.buffer);

    unique1 = statsFirstTerms->getCount1() - unique1;
    unique2 = statsFirstTerms->getCount2() - unique2;

    statsFirstTerms->addCoordForKey(key, pos1, pos2, validrows, strat1, strat2,
                                    unique1, unique2);

    return validrows;
}

size_t DiffIndex3::sortIndex(string outputdir,
                             string globaloutputdir,
                             int perm1,
                             int perm2,
                             std::vector<uint32_t> &idx1,
                             std::vector<uint64_t> &firstcolumn,
                             std::vector<uint64_t> &secondcolumn,
                             std::vector<uint64_t> &thirdcolumn,
                             PropertyMap & map,
                             Querier * q,
                             UpdateStats *statsFirstTerms,
                             const bool sort) {
    std::chrono::system_clock::time_point start = std::chrono::system_clock::now();
    if (sort) {
        std::sort(idx1.begin(), idx1.end(), _Sorter(firstcolumn));
    }
    std::chrono::duration<double> sec = std::chrono::system_clock::now() - start;
    LOG(DEBUG) << "Sort by first column " << sec.count() * 1000;

    //Init the tree and tmp arrays
    std::vector<uint64_t> tmp1;
    std::vector<uint64_t> tmp2;


    string file1;
    const size_t initialsize = min(idx1.size() * sizeof(uint64_t),
                                   (unsigned long) 128 * 1024 * 1024);
    LOG(DEBUG) << "Initial size: " << initialsize;
    if (idx1.size() < THRESHOLD_USEGLOBALFILES) { //If the update is small, then I store it in a single global file
        file1 = globaloutputdir + "/p0";
    } else {
        file1 = outputdir + "/p0";
    }
    std::unique_ptr<RWMappedFile> mappedFile1 = std::unique_ptr<RWMappedFile>(
                new RWMappedFile(file1, initialsize));

    string file2;
    if (idx1.size() < THRESHOLD_USEGLOBALFILES) { //If the update is small, then I store it in a single global file
        file2 = globaloutputdir + "/p1";
    } else {
        file2 = outputdir + "/p1";
    }
    std::unique_ptr<RWMappedFile> mappedFile2 = std::unique_ptr<RWMappedFile>(
                new RWMappedFile(file2, initialsize));

    //Start the inserting
    long prevkey = -1;
    size_t i = 0;

    //Stats
    long ntables = 0;
    long nsorts = 0;
    long nvalid = 0;
    long countersLayouts1[16];
    long countersLayouts2[16];
    memset(countersLayouts1, 0, sizeof(long) * 16);
    memset(countersLayouts2, 0, sizeof(long) * 16);
    StatStrategy stats;

    while (i < idx1.size()) {
        const uint64_t first = firstcolumn[idx1[i]];
        const uint64_t second = secondcolumn[idx1[i]];
        const uint64_t third = thirdcolumn[idx1[i]];

        if (first != prevkey) {
            //Sort the previous group
            if (sort && tmp1.size() > 1) {
                if (tmp1.size() == 2) {
                    if (tmp1[0] > tmp1[1]) {
                        uint64_t box = tmp1[0];
                        tmp1[0] = tmp1[1];
                        tmp1[1] = box;
                    }
                    if (tmp2[0] > tmp2[1]) {
                        uint64_t box = tmp2[0];
                        tmp2[0] = tmp2[1];
                        tmp2[1] = box;
                    }
                } else {
                    nsorts++;
                    std::sort(tmp1.begin(), tmp1.end());
                    std::sort(tmp2.begin(), tmp2.end());
                }
            }

            if (!tmp1.empty()) {
                nvalid += storeTablesOnBuffer(prevkey, tmp1, tmp2, perm1, perm2,
                                              mappedFile1,
                                              mappedFile2, stats,
                                              countersLayouts1,
                                              countersLayouts2, statsFirstTerms);
            }

            prevkey = first;
            tmp1.clear();
            tmp2.clear();
            ntables++;
        }

        uint64_t v1 = (second << 32) + third;
        tmp1.push_back(v1);
        uint64_t v2 = (third << 32) + second;
        tmp2.push_back(v2);
        i++;
    }
    if (!tmp1.empty()) {
        if (sort) {
            std::sort(tmp1.begin(), tmp1.end());
            std::sort(tmp2.begin(), tmp2.end());
        }
        nvalid += storeTablesOnBuffer(prevkey, tmp1, tmp2, perm1, perm2,
                                      mappedFile1, mappedFile2,
                                      stats, countersLayouts1,
                                      countersLayouts2, /*uq,*/ statsFirstTerms);
    }

    //Resize the file
    mappedFile1 = std::unique_ptr<RWMappedFile>();
    mappedFile2 = std::unique_ptr<RWMappedFile>();

    LOG(DEBUG) << "N. tuples " << idx1.size() << " unique " << nvalid;
    LOG(DEBUG) << "Ntables=" << ntables << " nsorts=" << nsorts;
    /*for (int i = 0; i < 16; ++i) {
        LOG(DEBUG) << "counters layout1 " << i << " is " << countersLayouts1[i];
        LOG(DEBUG) << "counters layout2 " << i << " is " << countersLayouts2[i];
    }*/
    LOG(DEBUG) << "BothRowsSmall: " << stats.bothRowsSmallCounter
                             << " BothRows: " << stats.bothRowsCounter
                             << " BothColumn: " << stats.bothColumnsCounter
                             << " ColumnRow: " << stats.columnRowCounter
                             << " RowColumn: " << stats.rowColumnCounter;

    return nvalid;
}

long DiffIndex3::getSize() const {
    return size;
}

long DiffIndex3::getCard(int idx, long first) const {
    TermCoordinates coord;
    if (idx == IDX_SPO || idx == IDX_SOP) {
        if (s->get(first, &coord)) {
            return coord.getNElements(idx);
        } else {
            return 0;
        }
    }
    if (idx == IDX_POS || idx == IDX_PSO) {
        if (p->get(first, &coord)) {
            return coord.getNElements(idx);
        } else {
            return 0;
        }
    }
    if (idx == IDX_OPS || idx == IDX_OSP) {
        if (o->get(first, &coord)) {
            return coord.getNElements(idx);
        } else {
            return 0;
        }
    }
    return 0;
}

long DiffIndex3::getNUniqueKeys(int idx) {
    return nuniquekeys[idx];
}

void DiffIndex3::getTermListItr(int idx, DiffTermItr * itr) {
    if (idx == IDX_SPO || idx == IDX_SOP) {
        itr->init(idx, nkeys_s, getNUniqueKeys(idx), s->itr());
    } else if (idx == IDX_POS || idx == IDX_PSO) {
        itr->init(idx, nkeys_p, getNUniqueKeys(idx), p->itr());
    } else {
        itr->init(idx, nkeys_o, getNUniqueKeys(idx), o->itr());
    }
}

long DiffIndex3::getUniqueNFirstTerms(int idx) {
    return nuniquefirstterms[idx];
}

long DiffIndex3::getNFirstTables(int idx) {
    return nfirstterms[idx];
}

DiffIndex3::~DiffIndex3() {
}

RWMappedFile::RWMappedFile(std::string file, size_t initialsize) : file(file) {
    std::chrono::system_clock::time_point start = std::chrono::system_clock::now();
    spaceleft = initialsize;
    if (!Utils::exists(file)) {
        currentposition = 0;
        ofstream f;
        f.open(file);
        f.seekp(currentposition + initialsize);
        f.put(0);
        f.close();
    } else {
        currentposition = Utils::fileSize(file);
        Utils::resizeFile(file, currentposition + spaceleft);
    }

    const size_t alignment = memmap::mapped_file_sink::alignment();
    const size_t offset = currentposition % alignment;
    sink.open(file, spaceleft + offset, currentposition - offset);
    currentbuffer = sink.data() + offset;
    std::chrono::duration<double> sec = std::chrono::system_clock::now() - start;
    LOG(DEBUG) << "Runtime init file = " << sec.count() * 1000;
}

RWMappedFile::Block RWMappedFile::getNewBlock(const size_t maxTableSize) {
    if (maxTableSize > spaceleft) {
        std::chrono::system_clock::time_point start = std::chrono::system_clock::now();
        sink.close();
        size_t incr = max((size_t)64 * 1014 * 1024, maxTableSize);
        Utils::resizeFile(file, currentposition + incr);
        spaceleft = incr;
        const size_t alignment = memmap::mapped_file_sink::alignment();
        const size_t offset = currentposition % alignment;
        sink.open(file, spaceleft + offset, currentposition - offset);
        currentbuffer = sink.data() + offset;
        std::chrono::duration<double> sec = std::chrono::system_clock::now() - start;
        LOG(DEBUG) << "Runtime resize file = " << sec.count() * 1000;
    }
    Block b;
    b.begin = currentposition;
    b.buffer = currentbuffer;
    return b;
}

RWMappedFile::~RWMappedFile() {
    std::chrono::system_clock::time_point start = std::chrono::system_clock::now();
    currentbuffer = NULL;
    sink.close();
    Utils::resizeFile(file, currentposition);
    std::chrono::duration<double> sec = std::chrono::system_clock::now() - start;
    LOG(DEBUG) << "Runtime close file = " << sec.count() * 1000;
}

void RWMappedFile::setLastBlockSize(const size_t spaceused) {
    assert(spaceused <= spaceleft);
    currentbuffer += spaceused;
    spaceleft -= spaceused;
    currentposition += spaceused;
}
