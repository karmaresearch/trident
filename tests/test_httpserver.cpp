#include <trident/utils/httpserver.h>

static void processRequest() {
}

int main(int argc, const char** argv) {
    auto f = std::bind(&processRequest);
    HttpServer server(8080, f);
    server.start();
}
