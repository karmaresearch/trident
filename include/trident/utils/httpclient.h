#ifndef _HTTP_CLIENT_H
#define _HTTP_CLIENT_H

#include <string>
#include <inttypes.h>
#include <string.h>

#if defined(__unix__) || defined(__unix) || defined(unix) || (defined(__APPLE__) && defined(__MACH__))
#include <sys/socket.h>
#include <sys/select.h>
#include <poll.h>
#include <netinet/in.h>
#include <unistd.h>
#include <fcntl.h>
#include <arpa/inet.h>
#include <netdb.h>
#endif

class HttpClient {
    private:
        int sockfd;
        uint32_t port;
        std::string address;
        struct sockaddr_in server;

    public:
        HttpClient(std::string address, uint32_t port) :
            sockfd(-1), port(port), address(address) {
            };

        bool connect();

        void disconnect();

        bool get(std::string &path, std::string &response);

        ~HttpClient();
};

#endif
