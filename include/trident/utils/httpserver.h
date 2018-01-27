#ifndef _HTTP_SERVER_H
#define _HTTP_SERVER_H

#include <trident/utils/parallel.h>

#include <string>
#include <thread>
#include <inttypes.h>

#if defined(__unix__) || defined(__unix) || defined(unix) || (defined(__APPLE__) && defined(__MACH__))
#include <sys/socket.h>
#include <sys/select.h>
#include <poll.h>
#include <netinet/in.h>
#include <unistd.h>
#include <fcntl.h>
#endif

class HttpServer {
    private:
        class Socket {
            private:
                int connFd;
                bool eof;
                bool toBeClosed;

            public:
                Socket(int connFd) : connFd(connFd), eof(false), toBeClosed(false) {
                }

                Socket() : connFd(-1), eof(true) {}

                int getFD() const {
                    return connFd;
                }

                bool shouldBeClosed() const {
                    return toBeClosed;
                }

                void close() {
                    toBeClosed = true;
                }

                bool isEOF() const {
                    return eof;
                }
        };

        uint32_t port;
        bool launched;
        uint64_t maxLifeConn;

        int listenFd;
        struct sockaddr_in svrAdd, clntAdd;
        ConcurrentQueue<Socket> queueReady;
        std::vector<std::thread> threads;
        std::function<void(const std::string&, std::string&)> handlerFunction;

        ConcurrentQueue<Socket> queueWait;
        std::thread threadWait;

        bool listn();

        void processSocket();

        void waitForData();

    public:
        HttpServer(uint32_t port,
                std::function<void(const std::string&, std::string&)> handler,
                uint32_t nthreads = 1,
                long maxLifeConn = 7000); //after one second, connections are closed

        void start();

        void stop();

        bool isLaunched() {
            return launched;
        }

        static std::string unescape(std::string s);
};

#endif
