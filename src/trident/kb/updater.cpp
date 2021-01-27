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


#include <trident/kb/updater.h>
#include <trident/utils/propertymap.h>
#include <trident/kb/kb.h>
#include <trident/kb/querier.h>
#include <trident/kb/dictmgmt.h>
#include <trident/tree/stringbuffer.h>
#include <trident/tree/root.h>

#include <kognac/filereader.h>

#include <string>

void Updater::parseUpdate(std::string update,
                          StringCollection &support,
                          std::vector<TextualTriple> &output) {
    std::vector<std::string> filesToParse;
    if (Utils::isDirectory(update)) {
        //Read files. Ignore hidden ones.
        filesToParse = Utils::getFiles(update);
        //J: The old code had a bug, I think 
    } else {
        filesToParse.push_back(update);
    }

    //Parse all files. Populate the output of arrays
    int64_t invalidtriples = 0;
    int64_t validtriples = 0;
    for (auto file : filesToParse) {
        FileInfo filei;
        filei.path = file;
        filei.start = 0;
        filei.size = Utils::fileSize(file);
        //Check whether the file terminates in *.gz
        if (Utils::ends_with(file, ".gz")) {
            filei.splittable = false;
        } else {
            filei.splittable = true;
        }
        FileReader reader(filei);
        while (reader.parseTriple()) {
            if (reader.isTripleValid()) {
                TextualTriple t;
                int length;
                const char *s = reader.getCurrentS(length);
                t.s = support.addNew(s, length);
                t.lens = length;
                const char *p = reader.getCurrentP(length);
                t.p = support.addNew(p, length);
                t.lenp = length;
                const char *o = reader.getCurrentO(length);
                t.o = support.addNew(o, length);
                t.leno = length;
                output.push_back(t);
                validtriples++;
            } else {
                invalidtriples++;
            }
        }
    }
    LOG(DEBUGL) << "Parsed " << validtriples << " invalid " << invalidtriples;
}

void Updater::writeDict(DictMgmt *dictmgmt,
                        string newupdatedir,
                        ByteArrayToNumberMap &indict) {
    if (indict.empty()) {
        return;
    }

    //If the update is small enough, then we add it to a global hashmap
    if (indict.size() < 10000) {
        for (auto itr = indict.begin(); itr != indict.end(); ++itr) {
            const char *term = itr->first;
            nTerm id = itr->second;
            dictmgmt->putInUpdateDict(id, term + 2, Utils::decode_short(term));
        }
        return;
    }

    //The update is large enough to justify the creation of a dedicated B+Tree
    string dictdir = newupdatedir + "/dict";
    Utils::create_directories(dictdir);

    //Init data structures
    Stats stats;
    std::unique_ptr<StringBuffer> sb = std::unique_ptr<StringBuffer>(
                                           new StringBuffer(dictdir, false, 10, 4096*SB_BLOCK_SIZE,
                                                   &stats));
    PropertyMap conf;
    conf.setBool(TEXT_KEYS, true);
    conf.setBool(TEXT_VALUES, false);
    string t2idir = dictdir + "/t2id";
    Utils::create_directories(t2idir);
    std::unique_ptr<Root> dict = std::unique_ptr<Root>(new Root(t2idir, sb.get(),
                                 false, conf));
    string i2tdir = dictdir + "/id2t";
    Utils::create_directories(i2tdir);
    conf.setBool(TEXT_KEYS, false);
    conf.setBool(TEXT_VALUES, true);
    std::unique_ptr<Root> invdict = std::unique_ptr<Root>(new Root(i2tdir,
                                    sb.get(), false, conf));

    //Write the elements
    uint64_t maxid = 0;
    for (auto itr = indict.begin(); itr != indict.end(); ++itr) {
        int64_t coordinates = sb->getSize();
        const char *term = itr->first;
        nTerm id = itr->second;
        if (dict->insertIfNotExists((tTerm*) (term + 2), Utils::decode_short(term), id)) {
            invdict->put(id, coordinates);
        }
        if (id > maxid)
            maxid = id;
    }

    ofstream ofs;
    ofs.open(dictdir + "/stats");
    char data[8];
    Utils::encode_long(data, indict.size());
    ofs.write(data, 8);
    Utils::encode_long(data, maxid + 1);
    ofs.write(data, 8);
    ofs.close();
}

void Updater::compressUpdate(DiffIndex::TypeUpdate type,
                             string updatedir,
                             std::vector<uint64_t> &all_s,
                             std::vector<uint64_t> &all_p,
                             std::vector<uint64_t> &all_o,
                             KB *kb,
                             Querier *q,
                             ByteArrayToNumberMap &tmpdict,
                             StringCollection &tmpdictsupport) {
    std::unique_ptr<char[]> supportbuffer(new char[MAX_TERM_SIZE + 2]);
    Utils::encode_short(supportbuffer.get(), 0);
    int64_t supportlen = 0;

    std::chrono::system_clock::time_point start = std::chrono::system_clock::now();
    std::vector<Triple> parsedtriples;
    {
        //Read the update and parse the strings
        std::vector<TextualTriple> triples;
        StringCollection col(64 * 1024 * 1024);
        parseUpdate(updatedir, col, triples);

        //Load the existing dictionary
        DictMgmt *dict = kb->getDictMgmt();
        int64_t nextID = kb->getNextID();
        int64_t previd = -1;

        //Sort the update by s
        std::sort(triples.begin(), triples.end(), sortByS);

        //Convert all s into numbers
        for (auto itr = triples.begin(); itr != triples.end(); ++itr) {
            if (supportlen == itr->lens &&
                    memcmp(itr->s, supportbuffer.get() + 2, supportlen) == 0) {
                itr->nums = previd;
            } else {
                memcpy(supportbuffer.get() + 2, itr->s, itr->lens);
                Utils::encode_short(supportbuffer.get(), itr->lens);
                supportlen = itr->lens;

                nTerm id;
                bool resp = dict->getNumber(itr->s, itr->lens, &id);
                if (resp) {
                    itr->nums = id;
                } else {
                    if (type == DiffIndex::TypeUpdate::ADDITION_df) {
                        //Add a new entry in the temporary dictionary
                        itr->nums = nextID;
                        const char *newentry = tmpdictsupport.addNew(supportbuffer.get(),
                                               itr->lens + 2);
                        tmpdict.insert(std::make_pair(newentry, nextID));
                        nextID++;
                    } else {
                        itr->nums = UINT64_MAX;
                        continue;
                    }
                }
                previd = itr->nums;
            }
        }

        //Sort the update by p
        std::sort(triples.begin(), triples.end(), sortByP);
        supportlen = 0;
        Utils::encode_short(supportbuffer.get(), 0);

        //Convert all p into numbers
        for (auto itr = triples.begin(); itr != triples.end(); ++itr) {
            if (supportlen == itr->lenp &&
                    memcmp(itr->p, supportbuffer.get() + 2, supportlen) == 0) {
                itr->nump = previd;
            } else {
                memcpy(supportbuffer.get() + 2, itr->p, itr->lenp);
                Utils::encode_short(supportbuffer.get(), itr->lenp);
                supportlen = itr->lenp;

                nTerm id;
                bool resp = dict->getNumber(itr->p, itr->lenp, &id);
                if (resp) {
                    itr->nump = id;
                } else {
                    if (type == DiffIndex::TypeUpdate::ADDITION_df) {
                        //Is it existing in the tmp dict already?
                        if (tmpdict.count(supportbuffer.get())) {
                            auto itr2 = tmpdict.find(supportbuffer.get());
                            itr->nump = itr2->second;
                        } else {
                            //Add a new entry in the temporary dictionary
                            itr->nump = nextID;
                            const char *newentry = tmpdictsupport.addNew(supportbuffer.get(),
                                                   itr->lenp + 2);
                            tmpdict.insert(std::make_pair(newentry, nextID));
                            nextID++;
                        }
                    } else {
                        itr->nump = UINT64_MAX;
                        continue;
                    }
                }
                previd = itr->nump;
            }
        }

        //Sort the update by o
        std::sort(triples.begin(), triples.end(), sortByO);
        supportlen = 0;
        Utils::encode_short(supportbuffer.get(), 0);

        //Convert all o into numbers
        for (auto itr = triples.begin(); itr != triples.end(); ++itr) {
            if (supportlen == itr->leno &&
                    memcmp(itr->o, supportbuffer.get() + 2, supportlen) == 0) {
                itr->numo = previd;
            } else {
                memcpy(supportbuffer.get() + 2, itr->o, itr->leno);
                Utils::encode_short(supportbuffer.get(), itr->leno);
                supportlen = itr->leno;

                nTerm id;
                bool resp = dict->getNumber(itr->o, itr->leno, &id);
                if (resp) {
                    itr->numo = id;
                } else {
                    if (type == DiffIndex::TypeUpdate::ADDITION_df) {
                        //Is it existing in the tmp dict already?
                        if (tmpdict.count(supportbuffer.get())) {
                            auto itr2 = tmpdict.find(supportbuffer.get());
                            itr->numo = itr2->second;
                        } else {
                            //Add a new entry in the temporary dictionary
                            itr->numo = nextID;
                            const char *newentry = tmpdictsupport.addNew(supportbuffer.get(),
                                                   itr->leno + 2);
                            tmpdict.insert(std::make_pair(newentry, nextID));
                            nextID++;
                        }
                    } else {
                        itr->numo = UINT64_MAX;
                        continue;
                    }
                }
                previd = itr->numo;
            }
        }

        for (auto itr = triples.begin(); itr != triples.end(); ++itr) {
            if (~itr->nums && ~itr->nump && ~itr->numo) {
                Triple t;
                t.s = itr->nums;
                t.p = itr->nump;
                t.o = itr->numo;
                parsedtriples.push_back(t);
            }
        }
    }

    //Re-sort the numeric triples.
    std::sort(parsedtriples.begin(), parsedtriples.end(), Triple::sorter);

    //Remove duplicates...
    auto newend = std::unique(parsedtriples.begin(), parsedtriples.end(),
                              Triple::equal);
    parsedtriples.resize(std::distance(parsedtriples.begin(), newend));

    //Add triples that are either not existing (ADD) or existing (REMOVE) ...
    match(type, all_s, all_p, all_o, q, parsedtriples);
    std::chrono::duration<double> sec = std::chrono::system_clock::now() - start;
    LOG(DEBUGL) << "Runtime compressing and filtering the update = " << sec.count() * 1000;

}

void Updater::creatediffupdate(DiffIndex::TypeUpdate type, std::string kbdir,
                               std::string updatedir) {
    std::chrono::system_clock::time_point startdiff = std::chrono::system_clock::now();
    std::vector<uint64_t> all_s;
    std::vector<uint64_t> all_p;
    std::vector<uint64_t> all_o;

    //Set up a tmp dictionary
    StringCollection tmpdictsupport(8 * 1024 * 1024);
    ByteArrayToNumberMap tmpdict;
    tmpdict.set_empty_key(EMPTY_KEY);
    tmpdict.set_deleted_key(DELETED_KEY);

    KBConfig config;
    KB kb(kbdir.c_str(), true, false, true, config);
    Querier *q = kb.query();

    compressUpdate(type, updatedir, all_s, all_p, all_o, &kb, q,
                   tmpdict, tmpdictsupport);

    if (!all_s.empty()) {
        std::chrono::system_clock::time_point start = std::chrono::system_clock::now();
        //Get the location to store the update
        string locationupdate = getPathForUpdate(kbdir);

        //Launch the procedure that is creating the index
        DiffIndex3::createDiffIndex(type, locationupdate, kbdir + "/_diff", all_s, all_p, all_o,
                                    true, q, true);

        //Write the type of file
        string flagup;
        if (type == DiffIndex::TypeUpdate::ADDITION_df) {
            //write also an additional dictionary (if any)
            writeDict(kb.getDictMgmt(), locationupdate, tmpdict);
            flagup = locationupdate + "/ADD";
        } else {
            flagup = locationupdate + "/DEL";
        }
        ofstream ofs(flagup);
        ofs.close();


        std::chrono::duration<double> sec = std::chrono::system_clock::now() - start;
        LOG(DEBUGL) << "Runtime creating the diff index from the update = " << sec.count() * 1000;
    } else {
        LOG(DEBUGL) << "The update is empty";
    }
    std::chrono::duration<double> secdiff = std::chrono::system_clock::now() - startdiff;
    LOG(INFOL) << "Runtime update " << secdiff.count() * 1000 << " ms.";

    delete q;
}

std::string Updater::getPathForUpdate(std::string kbdir) {
    std::string diffdir = kbdir + "/_diff";
    if (!Utils::exists(diffdir)) {
        return diffdir + "/0";
    } else {
        //Read all files in the directory that starts with a number
        int idx = 0;
        std::string updir = diffdir + "/" + to_string(idx);
        while (Utils::exists(updir)) {
            idx++;
            updir = diffdir + "/" + to_string(idx);
        }
        return updir;
    }
}

void Updater::match(DiffIndex::TypeUpdate type,
                    std::vector<uint64_t> &outputs,
                    std::vector<uint64_t> &outputp,
                    std::vector<uint64_t> &outputo,
                    Querier *q,
                    std::vector<Triple> &input) {

    PairItr *kbitr = q->getIterator(IDX_SPO, -1, -1, -1);
    if (kbitr->hasNext()) {
        kbitr->next();
    } else {
        q->releaseItr(kbitr);
        kbitr = NULL;
    }

    auto itr = input.begin();
    while (itr != input.end()) {
        //move kbitr to the good position
        while (kbitr && cmp(kbitr, *itr) < 0) {
            if (kbitr->hasNext()) {
                kbitr->next();
            } else {
                q->releaseItr(kbitr);
                kbitr = NULL;
            }
        }
        if (kbitr) {
            //do the check
            int res = cmp(kbitr, *itr);
            if (type == DiffIndex::TypeUpdate::ADDITION_df) {
                if (res != 0) {
                    outputs.push_back(itr->s);
                    outputp.push_back(itr->p);
                    outputo.push_back(itr->o);
                }
            } else {
                if (res == 0) {
                    outputs.push_back(itr->s);
                    outputp.push_back(itr->p);
                    outputo.push_back(itr->o);
                    //Must move also the original iterator
                    if (kbitr->hasNext()) {
                        kbitr->next();
                    } else {
                        break;
                    }
                }
            }
            itr++;
        } else {
            break;
        }
    }
    if (kbitr)
        q->releaseItr(kbitr);

    //Copy all remaining tuples
    if (type == DiffIndex::TypeUpdate::ADDITION_df) {
        while (itr != input.end()) {
            outputs.push_back(itr->s);
            outputp.push_back(itr->p);
            outputo.push_back(itr->o);
            itr++;
        }
    }
}

int Updater::cmp(PairItr *itr, const Triple &t) {
    if (itr->getKey() < t.s) {
        return -1;
    } else if (itr->getKey() == t.s) {
        if (itr->getValue1() < t.p) {
            return -1;
        } else if (itr->getValue1() == t.p) {
            if (itr->getValue2() < t.o) {
                return -1;
            } else if (itr->getValue2() == t.o) {
                return 0;
            } else {
                return 1;
            }
        } else {
            return 1;
        }
    } else {
        return 1;
    }
}
