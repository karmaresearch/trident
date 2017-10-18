#include <trident/utils/json.h>

void JSON::write(std::ostream &out, JSON &value) {
    out << "{";
    bool first = true;
    for (auto v : value.values) {
        if (!first)
            out << ",";
        out << "\"" << v.first << "\" : \"" << v.second << "\"";
        first = false;
    }
    for (auto v : value.children) {
        if (!first)
            out << ",";
        out << "\"" << v.first << "\" : ";
        write(out, v.second);
        first = false;
    }
    if (!value.listvalues.empty()) {
        if (!first) {
            out << ",";
        }
        out << "[";
        bool firstel = true;
        for(auto v : value.listvalues) {
            if (!firstel)
                out << ",";
            out << "\"" << v << "\"";
            firstel = false;
        }
        out << "]";
    }
    if (!value.listchildren.empty()) {
        if (!first) {
            out << ",";
        }
        out << "[";
        bool firstel = true;
        for(auto v : value.listchildren) {
            if (!firstel)
                out << ",";
            write(out, v);
            firstel = false;
        }
        out << "]";
    }
    out << "}";
}
