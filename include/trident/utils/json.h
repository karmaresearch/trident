#ifndef _JSON_H
#define _JSON_H

#include <kognac/logs.h>

#include <map>
#include <string>
#include <vector>

class JSON {
    private:
        std::map<std::string, std::string> values;
        std::map<std::string, JSON> children;

        std::vector<std::string> listvalues;
        std::vector<JSON> listchildren;

    public:
        void put(std::string name, std::string value) {
            values.insert(std::make_pair(name, value));
        }

        void put(std::string name, long value) {
            put(name, std::to_string(value));
        }

        void add_child(std::string name, JSON &child) {
            children.insert(std::make_pair(name, child));
        }

        void push_back(std::string value) {
            listvalues.push_back(value);
        }

        void push_back(JSON value) {
            listchildren.push_back(value);
        }

        static void write(zstr::ofstream &out, JSON &value) {
            LOG(ERRORL) << "Not implemented yet";
        }
};
#endif
