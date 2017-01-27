#ifndef _QUERYDICT_H
#define _QUERYDICT_H

#include <unordered_map>

class QueryDict {
private:
    uint64_t nextId;
    std::unordered_map<uint64_t, std::string> map;
    std::unordered_map<std::string, uint64_t> invertedMap;

public:
    QueryDict(uint64_t nextId) : nextId(nextId) {}

    bool hasID(uint64_t id) const {
        return map.find(id) != map.end();
    }

    bool isEmpty() const {
        return map.size() == 0;
    }

    std::string getLiteral(uint64_t id) {
        return map.find(id)->second;
    }

    uint64_t add(std::string &literal) {
        std::unordered_map<std::string, uint64_t>::iterator itr = invertedMap.find(literal);
        if (itr == invertedMap.end()) {
            invertedMap.insert(std::make_pair(literal, nextId));
            map.insert(std::make_pair(nextId, literal));
            return nextId++;
        } else {
            return itr->second;
        }
    }

    std::pair<char*, char*> getStringBoundaries(uint64_t id) {
        std::pair<char*, char*> pair;
        std::unordered_map<uint64_t, std::string>::iterator itr = map.find(id);
        const char* str = itr->second.c_str();
        pair.first = (char*)str;
        pair.second = (char*)(str + itr->second.length());
        return pair;
    }
};

#endif
