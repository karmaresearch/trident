#ifndef _JSON_H
#define _JSON_H

#include <kognac/logs.h>

#include <map>
#include <string>
#include <vector>
#include <iostream>


template<class T>
bool operator!=(const std::reverse_iterator<T>  &a, const std::reverse_iterator<T> &b) { return !(a == b);}

class JSON {
    private:
        std::map<std::string, std::string> values;
        std::map<std::string, JSON> children;

        std::vector<std::string> listvalues;
        std::vector<JSON> listchildren;

    public:
        void put(std::string name, std::string value) {
            if (name=="")
                throw 10;
            values.insert(std::make_pair(name, value));
        }

        void put(std::string name, long value) {
            if (name=="")
                throw 10;
            put(name, std::to_string(value));
        }

        void put(std::string name, unsigned char value) {
            if (name=="")
                throw 10;
            put(name, std::to_string(value));
        }

        void put(std::string name, unsigned short value) {
            if (name=="")
                throw 10;
            put(name, std::to_string(value));
        }

        void put(std::string name, int value) {
            if (name=="")
                throw 10;
            put(name, std::to_string(value));
        }

        void put(std::string name, unsigned int value) {
            if (name=="")
                throw 10;
            put(name, std::to_string(value));
        }

        void put(std::string name, unsigned long value) {
            if (name=="")
                throw 10;
            put(name, std::to_string(value));
        }

        void put(std::string name, unsigned long long value) {
            if (name=="")
                throw 10;
            put(name, std::to_string(value));
        }

        void put(std::string name, double value) {
            if (name=="")
                throw 10;
            put(name, std::to_string(value));
        }

        void put(std::string name, float value) {
            if (name=="")
                throw 10;
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

        bool containsChild(std::string key) const {
            return children.count(key);
        }

        bool contains(std::string key) const {
            return values.count(key);
        }

        const std::string get(std::string key) const {
            return values.find(key)->second;
        }

        const JSON &getChild(std::string key) {
            return children[key];
        }

        const std::vector<JSON> &getListChildren() const {
            return listchildren;
        }

        static void write(std::ostream &out, JSON &value);

        static void read(std::string &in, JSON &value);
};
#endif
