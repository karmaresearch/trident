#include <trident/utils/json.h>

std::string escape_json(const std::string &s) {
    std::ostringstream o;
    for (auto c = s.cbegin(); c != s.cend(); c++) {
        if (*c == '"' || *c == '\\' || ('\x00' <= *c && *c <= '\x1f')) {
            o << "\\u"
                << std::hex << std::setw(4) << std::setfill('0') << (int)*c;
        } else {
            o << *c;
        }
    }
    return o.str();
}

void JSON::write(std::ostream &out, JSON &value) {
    if (value.listchildren.size() > 0) {
        if (!value.listvalues.empty() ||
                !value.values.empty() ||
                !value.children.empty()) {
            throw 10;
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
    } else if (value.listvalues.size() > 0) {
        if (!value.values.empty() ||
                !value.children.empty()) {
            throw 10;
        }
        out << "[";
        bool firstel = true;
        for(auto v : value.listvalues) {
            if (!firstel)
                out << ",";
            out << "\"" << escape_json(v) << "\"";
            firstel = false;
        }
        out << "]";
    } else {
        out << "{";
        bool first = true;
        for (auto v : value.values) {
            if (!first)
                out << ",";
            out << "\"" << escape_json(v.first) << "\" : \"" << escape_json(v.second) << "\"";
            first = false;
        }
        for (auto v : value.children) {
            if (!first)
                out << ",";
            out << "\"" << escape_json(v.first) << "\" : ";
            write(out, v.second);
            first = false;
        }
        out << "}";
    }
}
