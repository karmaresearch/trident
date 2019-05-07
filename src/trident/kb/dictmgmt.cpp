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


#include <trident/kb/dictmgmt.h>
#include <trident/tree/root.h>
#include <trident/tree/stringbuffer.h>
#include <trident/tree/treeitr.h>

#include <kognac/hashfunctions.h>
#include <kognac/lz4io.h>

#include <lz4.h>

#include <iostream>
#include <fstream>

using namespace std;

DictMgmt::DictMgmt(Dict mainDict, string dirToStoreGUD, bool hash, string e2r,
        string e2s) :
    hash(hash) {
        nTuples = 0;
        printTuples = false;
        sTuples = 0;
        insertedNewTerms = new int64_t[1];
        insertedNewTerms[0] = 0;
        largestID = 0;

        dictionaries.push_back(mainDict);
        beginrange.push_back(0);

        gud_modified = false;
        gud_largestID = 0;
        gudLocation = dirToStoreGUD;
        gud_idtext.set_empty_key(UINT64_MAX);
        gud_idtext.set_deleted_key(UINT64_MAX - 1);
        //load gud
        if (Utils::exists(dirToStoreGUD + DIR_SEP + "gud")) {
            std::chrono::system_clock::time_point start = std::chrono::system_clock::now();
            ifstream ifs;
            ifs.open(dirToStoreGUD + DIR_SEP + "gud");
            ifs >> gud_largestID;
            while (!ifs.eof()) {
                uint64_t id;
                string term;
                ifs >> id;
                ifs >> term;
                if (term.size() > 0) {
                    gud_idtext.insert(make_pair(id, term));
                    gud_textid.insert(make_pair(term, id));
                }
            }
            ifs.close();
            std::chrono::duration<double> sec = std::chrono::system_clock::now()
                - start;
            LOG(DEBUGL) << "Time loading GUD " << sec.count() * 1000;
        }

        if (Utils::exists(e2r)) {
            LOG(DEBUGL) << "Load the mappings rel->ent from " << e2r;
            //Load the mappings from the relation IDs to the entity IDs
            r2e.set_deleted_key(UINT64_MAX);
            LZ4Reader reader(e2r);
            char buffer[MAX_TERM_SIZE];

            while (!reader.isEof()) {
                int64_t e = reader.parseLong();
                int64_t r = reader.parseLong();
                r2e.insert(std::make_pair(r,e));
                //Also store the corresponding strings
                int size = 0;
                bool resp = getText(e, buffer, size);
                if (resp) {
                    std::string srel = std::string(buffer, size);
                    s2r.insert(std::make_pair(srel, r));
                    //r2s.insert(std::make_pair(r, srel));
                } else {
                    LOG(WARNL) << "String for entity " << e << " is not found";
                }
            }
        } else if (Utils::exists(e2s)) {
            LOG(DEBUGL) << "Load the mappings rel->ent (string) from " << e2s;
            r2e.set_deleted_key(UINT64_MAX);
            LZ4Reader reader(e2s);
            while (!reader.isEof()) {
                int64_t r = reader.parseLong();
                int size;
                const char *t = reader.parseString(size);
                s2r.insert(std::make_pair(string(t + 2, size - 2), r));
                r2s.insert(std::make_pair(r,string(t + 2, size - 2)));
            }

        }
    }

TreeItr *DictMgmt::getInvDictIterator() {
    return dictionaries[0].invdict->itr();
}

TreeItr *DictMgmt::getDictIterator() {
    return dictionaries[0].dict->itr();
}

StringBuffer *DictMgmt::getStringBuffer() {
    return dictionaries[0].sb.get();
}

void DictMgmt::putInUpdateDict(const uint64_t id,
        const char *term,
        const size_t len) {
    gud_idtext.insert(make_pair(id, string(term, len)));
    gud_textid.insert(make_pair(string(term, len), id));
    gud_modified = true;
    if (id > gud_largestID)
        gud_largestID = id;
}

bool DictMgmt::getTextRel(nTerm key, char *value, int &size) {
    if (r2s.count(key)) {
        auto f = r2s.find(key);
        size = f->second.size();
        memcpy(value, f->second.c_str(), f->second.size());
        return true;
    } else {
        if (r2e.count(key)) {
            return getText(r2e[key], value, size);
        } else {
            return false;
        }
    }
}

bool DictMgmt::getText(nTerm key, char *value) {
    int64_t coordinates;
    int idx = 0;
    while (idx < beginrange.size() - 1 && key >= beginrange[idx + 1]) {
        idx++;
    }
    if (dictionaries[idx].invdict->get(key, coordinates)) {
        int size = 0;
        dictionaries[idx].sb->get(coordinates, value, size);
        value[size] = '\0';
        return true;
    }
    if (!gud_idtext.empty()) {
        auto it = gud_idtext.find(key);
        if (it != gud_idtext.end()) {
            const size_t size = it->second.size();
            memcpy(value, it->second.c_str(), size);
            value[size] = '\0';
            return true;
        }
    }
    return false;
}

bool DictMgmt::getText(nTerm key, std::string &value) {
    int64_t coordinates;
    int idx = 0;
    while (idx < beginrange.size() - 1 && key >= beginrange[idx + 1]) {
        idx++;
    }
    if (dictionaries[idx].invdict->get(key, coordinates)) {
        int size = 0;
        char *rawvalue = dictionaries[idx].sb->get(coordinates, size);
        value = std::string(rawvalue, size);
        return true;
    }
    if (!gud_idtext.empty()) {
        auto it = gud_idtext.find(key);
        if (it != gud_idtext.end()) {
            value = it->second;
            return true;
        }
    }
    return false;

}

bool DictMgmt::getText(nTerm key, char *value, int &size) {
    int64_t coordinates;
    int idx = 0;
    while (idx < beginrange.size() - 1 && key >= beginrange[idx + 1]) {
        idx++;
    }
    if (dictionaries[idx].invdict->get(key, coordinates)) {
        dictionaries[idx].sb->get(coordinates, value, size);
        return true;
    }
    if (!gud_idtext.empty()) {
        auto it = gud_idtext.find(key);
        if (it != gud_idtext.end()) {
            size = it->second.size();
            memcpy(value, it->second.c_str(), size);
            return true;
        }
    }
    return false;
}

void DictMgmt::getTextFromCoordinates(int64_t coordinates, char *output,
        int &sizeOutput) {
    dictionaries[0].sb->get(coordinates, output, sizeOutput);
    output[sizeOutput] = '\0';
}

bool DictMgmt::getNumber(const char *key, const int sizeKey, nTerm *value) {
    int i = 0;
    while (i < dictionaries.size()) {
        if (!dictionaries[i].dict->get((tTerm*) key, sizeKey, value)) {
            i++;
        } else {
            return true;
        }
    }

    if (!gud_textid.empty()) {
        auto it = gud_textid.find(string(key, sizeKey));
        if (it != gud_textid.end()) {
            *value = it->second;
            return true;
        }
    }

    return false;
}

bool DictMgmt::getNumberRel(const char *key, const int sizeKey, nTerm *value) {
    std::string skey = std::string(key, sizeKey);
    if (s2r.count(skey)) {
        *value = s2r[skey];
        return true;
    }
    return false;
}

void DictMgmt::appendPair(const char *key, int sizeKey, nTerm &value) {
    int64_t coordinates = dictionaries[0].sb->getSize();
    dictionaries[0].dict->append((tTerm*) key, sizeKey, value);
    dictionaries[0].invdict->put(value, coordinates);
    insertedNewTerms[0]++;
    if (value > largestID)
        largestID = value;
}

bool DictMgmt::putPair(const char *key, int sizeKey, nTerm &value) {
    int64_t coordinates = dictionaries[0].sb->getSize();
    if (dictionaries[0].dict->insertIfNotExists((tTerm*) key, sizeKey, value)) {
        dictionaries[0].invdict->put(value, coordinates);
        insertedNewTerms[0]++;
        if (value > largestID)
            largestID = value;
        return true;
    }
    return false;
}

bool DictMgmt::putDict(const char *key, int sizeKey, nTerm &value) {
    if (dictionaries[0].dict->insertIfNotExists((tTerm*) key, sizeKey, value)) {
        insertedNewTerms[0]++;
        if (value > largestID)
            largestID = value;
        return true;
    }
    return false;
}

bool DictMgmt::putDict(const char *key, int sizeKey, nTerm &value,
        int64_t &coordinates) {
    coordinates = dictionaries[0].sb->getSize();
    if (dictionaries[0].dict->insertIfNotExists((tTerm*) key, sizeKey, value)) {
        insertedNewTerms[0]++;
        if (value > largestID)
            largestID = value;
        return true;
    }
    return false;
}

bool DictMgmt::putInvDict(const char *key, int sizeKey, nTerm &value) {
    int64_t coordinates = dictionaries[0].sb->getSize();
    dictionaries[0].sb->append((char*) key, sizeKey);
    dictionaries[0].invdict->put(value, coordinates);
    return true;
}

bool DictMgmt::putInvDict(const nTerm key, const int64_t coordinates) {
    dictionaries[0].invdict->put(key, coordinates);
    return true;
}

void DictMgmt::addUpdates(std::vector<Dict> &updates) {
    //Add the updates
    for (int i = 0; i < updates.size(); ++i) {
        dictionaries.push_back(updates[i]);
        TreeItr *itr = updates[i].invdict->itr();
        if (!itr->hasNext())
            throw 10; //should not happen
        int64_t coord;
        int64_t key = itr->next(coord);
        beginrange.push_back(key);
        delete itr;
    }
}

DictMgmt::~DictMgmt() {
    delete[] insertedNewTerms;
    if (gud_modified && !gud_idtext.empty()) {
        //Write down the new version
        std::chrono::system_clock::time_point start = std::chrono::system_clock::now();
        ofstream os;
        os.open(gudLocation + DIR_SEP + "gud", ios_base::trunc);
        os << gud_largestID << endl;
        for (auto it = gud_idtext.begin(); it != gud_idtext.end(); ++it) {
            os << it->first << '\t' << it->second << endl;
        }
        os.close();
        std::chrono::duration<double> sec = std::chrono::system_clock::now()
            - start;
        LOG(DEBUGL) << "Time writing GUD " << sec.count() * 1000;
    }
}
