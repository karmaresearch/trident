#ifndef _HTTP_SERVER_H
#define _HTTP_SERVER_H

#include <trident/utils/parallel.h>

#include <string>
#include <thread>
#include <inttypes.h>

#if defined(__unix__) || defined(__unix) || defined(unix) || (defined(__APPLE__) && defined(__MACH__))
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>
#include <fcntl.h>
#endif

class HttpServer {
    private:
        class HttpRequestHander {
            private:
                int connFd;
                bool eof;

            public:
                HttpRequestHander(int connFd) : connFd(connFd), eof(false) {
                }

                HttpRequestHander() : connFd(-1), eof(true) {}

                bool isEOF() const {
                    return eof;
                }

                void processRequest(
                        std::function<void(
                            const std::string&, std::string&)> &handler);
        };

        uint32_t port;
        bool launched;

        int listenFd;
        struct sockaddr_in svrAdd, clntAdd;
        ConcurrentQueue<HttpRequestHander> queue;
        std::vector<std::thread> threads;
        std::function<void(const std::string&, std::string&)> handlerFunction;

        bool run();

        void pullFromQueue();

    public:
        HttpServer(uint32_t port,
                std::function<void(const std::string&, std::string&)> handler,
                uint32_t nthreads = 1);

        void start();

        void stop();

        bool isLaunched() {
            return launched;
        }

        static std::string unescape(std::string s);
};

#endif
