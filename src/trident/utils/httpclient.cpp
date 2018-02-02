#include <trident/utils/httpclient.h>

#include <kognac/utils.h>

#include <iostream>
#include <sstream>
#include <algorithm>

bool HttpClient::connect() {
    //Get IP address of the host
    struct addrinfo hints, *res, *res0;
    memset(&hints, 0, sizeof(hints));
    hints.ai_family = PF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    std::string sport = std::to_string(port);
    int error = getaddrinfo(address.c_str(), sport.c_str(), &hints, &res0);
    if (error) {
        freeaddrinfo(res0);
        return false;
    }

    for (res = res0; res; res = res->ai_next) {
        sockfd = socket(res->ai_family, res->ai_socktype,
                res->ai_protocol);
        if (sockfd < 0) {
            continue;
        }
        if (::connect(sockfd, res->ai_addr, res->ai_addrlen) < 0) {
            close(sockfd);
            sockfd = -1;
            continue;
        }
        break;  /* okay we got one */
    }
    if (sockfd < 0) {
        freeaddrinfo(res0);
        return false;
    }
    freeaddrinfo(res0);
    return true;
}

bool HttpClient::get(std::string &path, std::string &response) {
    std::string request = "GET "  + path + " HTTP/1.1\r\nHost: " + address + ":" + std::to_string(port) + "\r\n\r\n";
    int ret = send(sockfd, request.c_str(), request.size(), 0);
    if (ret < 0) {
        return false;
    } else {
        char buffer[1024];
        response = "";
        std::string rawresponse = "";

        bool initialParsing = false;

        bool determinedSize = false;
        long totalSize = 0; //used if not chunked
        bool isChunked = false;
        unsigned long sizeChunk = -1;
        unsigned long currentChunkSize = 0;

        while (true) {
            int resp = recv(sockfd , buffer, 1024, 0);
            if (resp == 0 || resp == -1) {
                break;
            } else {
                if (determinedSize) {
                    //I am reading content ...
                    int bufferIdx = 0;
                    while (bufferIdx < resp) {
                        if (currentChunkSize < sizeChunk) {
                            //Read until sizeChunk
                            int remainingSize = std::min(
                                    (int)(sizeChunk - currentChunkSize),
                                    resp - bufferIdx);
                            response += std::string(buffer + bufferIdx,
                                    remainingSize);
                            currentChunkSize += remainingSize;
                            bufferIdx += remainingSize;
                        } else if (currentChunkSize == sizeChunk) {
                            //Must read the next chunk
                            std::string rest = std::string(buffer +
                                    bufferIdx + 2,
                                    resp - bufferIdx - 2);
                            auto end = rest.find("\r\n");
                            std::string sSizeChunk = rest.substr(0, end);
                            std::stringstream ss; //convert the string in a number
                            ss << std::hex << sSizeChunk;
                            ss >> sizeChunk;
                            currentChunkSize = 0;
                            bufferIdx +=  2 + end + 2;
                            if (sizeChunk == 0) {
                                return true;
                            }
                        }
                    }
                } else {
                    rawresponse += std::string(buffer, resp);
                    if (!initialParsing) {
                        if (rawresponse.size() + resp > 13) {
                            //Get the response type
                            if (!Utils::starts_with(rawresponse, "HTTP/1")) {
                                return false;
                            } else {
                                //Read the code. Should be 200
                                std::string code = rawresponse.substr(9, 3);
                                if (code != "200") {
                                    //not supported
                                    return false;
                                }
                            }
                            initialParsing = true;
                        }
                    }
                    if (!determinedSize) {
                        if (rawresponse.find("Content-Length:") !=
                                std::string::npos) {
                            //Not implemented
                            return false;
                        } else if (rawresponse.
                                find("Transfer-Encoding: chunked") !=
                                std::string::npos) {
                            isChunked = true;
                            //Read the size of the chunk
                            auto startidx = rawresponse.find("Transfer-Encoding: chunked");
                            std::string rest = rawresponse.substr(startidx + 30);
                            auto endidx = rest.find("\r\n");
                            std::string sSizeChunk = rest.substr(0, endidx);
                            std::stringstream ss; //convert the string in a number
                            ss << std::hex << sSizeChunk;
                            ss >> sizeChunk;
                            //Copy the content in body
                            response = rest.substr(endidx + 2);
                            currentChunkSize = response.size();
                            determinedSize = true;
                        }
                    }
                }
            }
        }
        return true;
    }
}

void HttpClient::disconnect() {
    if (sockfd != -1) {
        close(sockfd);
    }
}

HttpClient::~HttpClient() {
    disconnect();
}
