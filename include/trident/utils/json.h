#ifndef _JSON_H
#define _JSON_H

#include <kognac/logs.h>

#include <map>
#include <string>
#include <vector>
#include <iostream>

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

        static void write(std::ostream &out, JSON &value);
};
#endif
