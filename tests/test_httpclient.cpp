#include <trident/utils/httpclient.h>

#include <iostream>

int main(int argc, const char** argv) {
    HttpClient client("www.google.nl", 80);
    if (client.connect()) {
    } else {
        std::cerr << "Not connected" << std::endl;
    }

    std::string req = "/";
    std::string resp;
    if (client.get(req, resp)) {
        std::cout << resp << std::endl;
    } else {
        std::cerr << "Error!" << std::endl;
    }
}
