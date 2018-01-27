#include <trident/utils/httpserver.h>

#include <kognac/logs.h>


HttpServer::HttpServer(uint32_t port,
        std::function<void(const std::string&, std::string&)> handler,
        uint32_t nthreads) : port(port), handlerFunction(handler) {
    threads.resize(nthreads);
    for(uint32_t i = 0; i < nthreads; ++i) {
        threads[i] = std::thread(&HttpServer::pullFromQueue, this);
    }
}

void HttpServer::pullFromQueue() {
    while (true) {
        HttpRequestHander handler;
        queue.pop_wait(handler);
        if (handler.isEOF()) {
            break;
        }
        handler.processRequest(handlerFunction);
    }
}

bool HttpServer::run() {
    //Create the socket
    listenFd = socket(PF_INET, SOCK_STREAM, IPPROTO_TCP);
    if(listenFd < 0) {
        return false;
    }

    bzero((char*) &svrAdd, sizeof(svrAdd));
    svrAdd.sin_family = AF_INET;
    svrAdd.sin_addr.s_addr = INADDR_ANY;
    svrAdd.sin_port = htons(port);
    if(bind(listenFd, (struct sockaddr *)&svrAdd, sizeof(svrAdd)) < 0) {
        return false;
    }
    listen(listenFd, 10);
    launched = true;
    while (true) {
        //this is where client connects.
        socklen_t len = sizeof(clntAdd);
        int connFd = accept(listenFd, (struct sockaddr *)&clntAdd, &len);
        if (connFd < 0) {
            return false;
        }
        //Put a HttpRequestHandler in a synchronized queue so that a thread can
        //take care of it
        queue.push(HttpRequestHander(connFd));
    }
    launched = false;
}

void HttpServer::start() {
    launched = false;
    run();
}

void HttpServer::stop() {
    //Stop all processing threads
    for(int i = 0; i < threads.size(); ++i)
        queue.push(HttpRequestHander());
    for(auto &t : threads) {
        t.join();
    }
    close(listenFd);
    while (launched) {
        //TODO Wait until the thread has stopped
    }
}

void HttpServer::HttpRequestHander::processRequest(std::function<void(
            const std::string&, std::string&)> &handler) {

    //Make the socket non-blocking
    //fcntl(connFd, F_SETFL, fcntl(connFd, F_GETFL, 0) | O_NONBLOCK);

    //Read the data and print it on screen
    try {
        char buffer[1024];
        while (true) {
            std::string request = "";
            auto len = read(connFd, buffer, 1024);
            if (len == 0) { //Client has closed the conversation
                shutdown(connFd, 2);
                close(connFd);
                connFd = -1;
                return;
            } else if (len == -1) {
                //There are some errors here, handle?
                //break;
                throw 10;
            }
            request += std::string(buffer, len);
            std::string response = "";
            handler(request, response);
            long size = 0;
            do {
                size += send(connFd, response.c_str() + size,
                        response.size() - size, 0);
            } while (size < response.size());
            //shutdown(connFd, 2);
            //close(connFd);
        }
    } catch (...) {
        if (connFd != -1) {
            shutdown(connFd, 2);
            close(connFd);
        }
    }
}

//Inspired from https://stackoverflow.com/questions/2673207/c-c-url-decode-library#14530993
std::string HttpServer::unescape(std::string s) {
    const char *src = s.c_str();
    std::vector<char> dst;
    char a, b;
    while (*src) {
        if ((*src == '%') &&
                ((a = src[1]) && (b = src[2])) &&
                (isxdigit(a) && isxdigit(b))) {
            if (a >= 'a')
                a -= 'a'-'A';
            if (a >= 'A')
                a -= ('A' - 10);
            else
                a -= '0';
            if (b >= 'a')
                b -= 'a'-'A';
            if (b >= 'A')
                b -= ('A' - 10);
            else
                b -= '0';
            dst.push_back(16*a+b);
            src+=3;
        } else if (*src == '+') {
            dst.push_back(' ');
            src++;
        } else {
            dst.push_back(*src++);
        }
    }
    return std::string(dst.data(), dst.size());
}
