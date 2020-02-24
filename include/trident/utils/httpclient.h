#ifndef _HTTP_CLIENT_H
#define _HTTP_CLIENT_H

#include <string>
#include <inttypes.h>
#include <string.h>
#include <map>

#if defined(__unix__) || defined(__unix) || defined(unix) || (defined(__APPLE__) && defined(__MACH__))
#include <sys/socket.h>
#include <sys/select.h>
#include <poll.h>
#include <netinet/in.h>
#include <unistd.h>
#include <fcntl.h>
#include <arpa/inet.h>
#include <netdb.h>

class HttpClient {
    private:
        int sockfd;
        uint32_t port;
        std::string address;

        bool getResponse(const std::string &request,
                std::string &headers,
                std::string &response);

    public:
        class URL {
            public:
                std::string protocol;
                std::string host;
                int port;
                std::string path;
                std::string query;
                URL() : protocol(""), host(""), port(80), path(""), query("") {}
        };

        HttpClient(std::string address, uint32_t port) :
            sockfd(-1), port(port), address(address) {
            };

        bool connect();

        void disconnect();

        bool get(const std::string &path,
                std::string &headers,
                std::string &response,
                std::string &formatOutput);

        bool get(const std::string &path,
                std::string &headers,
                std::string &response) {
            std::string format = "";
            return get(path, headers, response, format);
        }

        bool post(const std::string &path,
                std::map<std::string, std::string> &params,
                std::string &headers,
                std::string &response,
                std::string contenttype = "application/x-www-form-urlencoded");

        bool post(const std::string &path,
                std::string &params,
                std::string &headers,
                std::string &response,
                std::string contenttype = "application/x-www-form-urlencoded");

        ~HttpClient();

        static URL parse(std::string url);

        static std::string unescape(const std::string &s);

        static std::string escape(const std::string &s);
};

#endif

#endif
