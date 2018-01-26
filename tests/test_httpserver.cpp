#include <trident/utils/httpserver.h>

int main(int argc, const char** argv) {
    HttpServer server(8080);
    server.start();
}
