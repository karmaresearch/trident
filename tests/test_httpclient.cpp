#include <trident/utils/httpclient.h>
#include <trident/utils/json.h>

#include <iostream>
#include <map>
#include <sstream>

int main(int argc, const char** argv) {
    HttpClient client("localhost", 9200);
    if (client.connect()) {
    } else {
        std::cerr << "Not connected" << std::endl;
    }

    std::string req = "/wiki/_search?pretty";
    std::string headers;
    std::string resp;
    std::map<std::string, std::string> params;

    
    JSON json;
    JSON child;
    JSON child2;
    child.add_child("match_all", child2);
    json.add_child("query", child);
    std::stringstream ss;
    JSON::write(ss, json);
    std::string payload = ss.str();
    params.insert(std::make_pair("", payload));

    if (client.post(req, params, headers, resp, "application/json")) {
        //std::cout << resp << std::endl;
        std::cout << headers << std::endl;
        std::cout << "OK" << std::endl;
    } else {
        std::cerr << "Error!" << std::endl;
    }
}
