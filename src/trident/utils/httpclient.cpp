#include <trident/utils/httpclient.h>

#include <kognac/utils.h>
#include <kognac/logs.h>

#include <iostream>
#include <sstream>
#include <algorithm>
#include <functional>
#include <vector>
#include <iomanip>

#if defined(_WIN32)
//The Http Client and Server are only supported under Linux/Mac
#else

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

HttpClient::URL HttpClient::parse(std::string url) {
    HttpClient::URL output;

    //Parse protocol
    const std::string protEnd("://");
    std::string::const_iterator protI = search(url.begin(), url.end(),
            protEnd.begin(), protEnd.end());
    output.protocol.reserve(distance(url.cbegin(), protI));
    transform(url.cbegin(), protI,
            back_inserter(output.protocol),
            std::ptr_fun<int,int>(tolower));
    if( protI == url.end() )
        return output;

    //Hostname
    advance(protI, protEnd.length());
    std::string::const_iterator portI = find(protI, url.cend(), ':');
    std::string::const_iterator pathI;
    if (portI != url.end()) {
        output.host.reserve(distance(protI, portI));
        transform(protI, portI,
                back_inserter(output.host),
                std::ptr_fun<int,int>(tolower)); // host is icase
        //Parse the port number
        pathI  = find(protI, url.cend(), '/');
        std::string sport;
        sport.reserve(distance(portI, pathI));
        transform(portI, pathI,
                back_inserter(sport),
                std::ptr_fun<int,int>(tolower));
        output.port = std::stoi(sport);

    } else {
        pathI  = find(protI, url.cend(), '/');
        output.host.reserve(distance(protI, pathI));
        transform(protI, pathI,
                back_inserter(output.host),
                std::ptr_fun<int,int>(tolower)); // host is icase
    }

    //path
    auto queryI = find(pathI, url.cend(), '?');
    output.path.assign(pathI, queryI);

    //query
    if( queryI != url.end())
        ++queryI;
    output.query.assign(queryI, url.cend());
    return output;
}

//Inspired from https://stackoverflow.com/questions/2673207/c-c-url-decode-library#14530993
std::string HttpClient::unescape(const std::string &s) {
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

//Inspired by https://stackoverflow.com/questions/154536/encode-decode-urls-in-c#17708801
std::string HttpClient::escape(const std::string &s) {
    std::ostringstream escaped;
    escaped.fill('0');
    escaped << std::hex;

    std::string::const_iterator n;
    for (std::string::const_iterator i = s.cbegin(),
            n = s.cend(); i != n; ++i) {
        std::string::value_type c = (*i);
        if (std::isalnum(c) || c == '-' || c == '_' || c == '.' || c == '~') {
            escaped << c;
            continue;
        }
        // Any other characters are percent-encoded
        escaped << std::uppercase;
        escaped << '%' << std::setw(2) << int((unsigned char) c);
        escaped << std::nouppercase;
    }

    return escaped.str();
}

bool HttpClient::getResponse(const std::string &request,
        std::string &headers,
        std::string &response) {
    int ret = send(sockfd, request.c_str(), request.size(), 0);
    if (ret < 0) {
        return false;
    } else {
        char buffer[1024];
        response = "";
        headers = "";

        bool processedHeaders = false;
        bool processBody = false;

        //uint64_t totalSize = 0; //used if not chunked
        bool isChunked = false;
        size_t sizeChunk = 0;
        size_t currentChunkSize = 0;

        //Pointers to the buffer
        size_t sizebuffer = 0;
        const char *remaining = buffer;

        while (true) {
            if (sizebuffer <= 0) {
                sizebuffer = recv(sockfd , buffer, 1024, 0);
                remaining = buffer;
                if (sizebuffer == 0 || sizebuffer == -1) {
                    break;
                }
            }

            //Process the buffer
            if (processBody) {
                //I am reading content ...
                while (sizebuffer > 0) {
                    if (currentChunkSize < sizeChunk) {
                        //Read until sizeChunk
                        int remainingSize = std::min(
                                (size_t)(sizeChunk - currentChunkSize),
                                sizebuffer);
                        response += std::string(remaining,
                                remainingSize);
                        currentChunkSize += remainingSize;
                        remaining += remainingSize;
                        sizebuffer -= remainingSize;
                    } else if (currentChunkSize == sizeChunk) {
                        //Must read the next chunk
                        if (sizebuffer >= 1 && remaining[0] == '\r') {
                            remaining++;
                            sizebuffer--;
                        }
                        if (sizebuffer >= 1 && remaining[0] == '\n') {
                            remaining++;
                            sizebuffer--;
                        }
                        std::string rest = std::string(remaining, sizebuffer);
                        auto end = rest.find("\r\n");
                        if (end == std::string::npos) {
                            LOG(ERRORL) << "Special case not handled. Must implement it ...";
                            return false;
                        }
                        std::string sSizeChunk = rest.substr(0, end);
                        std::stringstream ss; //convert the string to number
                        ss << std::hex << sSizeChunk;
                        ss >> sizeChunk;
                        currentChunkSize = 0;
                        remaining += end + 2;
                        sizebuffer -= end + 2;
                        if (sizeChunk == 0) {
                            return true;
                        }
                    }
                }

                //If not chunked and read everthing then exit
                if (!isChunked && currentChunkSize == sizeChunk) {
                    return true;
                }
            } else if (!processedHeaders) {
                //Collect headers until '\r\n\r\n'
                std::string sbuffer = std::string(remaining, sizebuffer);
                headers += sbuffer;
                if (headers.find("\r\n\r\n") != std::string::npos) {
                    auto idx = headers.find("\r\n\r\n");
                    headers = headers.substr(0, idx);
                    remaining += idx + 4;
                    sizebuffer -= idx + 4;
                    processedHeaders = true;
                }
                if (headers.size() > 13) {
                    //Get the response type
                    if (!Utils::starts_with(headers, "HTTP/1")) {
                        LOG(ERRORL) << "No HTTP header";
                        return false;
                    } else {
                        //Read the code. Should be 200
                        std::string code = headers.substr(9, 3);
                        if (code != "200") {
                            //not supported
                            LOG(ERRORL) << "No 200 code " << code;
                            return false;
                        }
                    }
                }
            }
            if (processedHeaders && !processBody) {
                //Determine the way to finish reading the body
                std::string lheaders = headers;
                std::transform(lheaders.begin(), lheaders.end(),
                        lheaders.begin(), ::tolower);
                if (lheaders.find("content-length:") !=
                        std::string::npos) {
                    isChunked = false;
                    //Get the size of the response
                    auto idx = lheaders.find("content-length:");
                    std::string line = lheaders.substr(idx + 16);
                    auto end = line.find("\r\n");
                    std::stringstream ss;
                    ss << line.substr(0, end);
                    ss >> sizeChunk;
                    processBody = true;
                } else if (lheaders.
                        find("transfer-encoding: chunked") !=
                        std::string::npos) {
                    isChunked = true;
                    processBody = true;
                }
            }
        }
        return true;
    }
}

bool HttpClient::get(const std::string &path,
        std::string &headers,
        std::string &response,
        std::string &formatOutput) {
    std::string request = "GET "  + path + " HTTP/1.1\r\nHost: " +
        address + ":" + std::to_string(port);
    if (formatOutput != "") {
        request += "\r\nAccept: " + formatOutput;
    }
    request += "\r\n\r\n";
    return getResponse(request, headers, response);
}

bool HttpClient::post(const std::string &path,
        std::map<std::string, std::string> &params,
        std::string &headers,
        std::string &response,
        std::string contenttype) {
    //Encode the parameters
    std::string sparams = "";
    bool first = true;
    for (auto &pair : params) {
        if (!first)
            sparams += "&";
        if (pair.first == "") {
            //only value
            sparams += pair.second;
        } else {
            //Key/value
            sparams += pair.first + "=" + pair.second;
        }
        first = false;
    }
    //Construct the request
    std::string request = "POST " + path + " HTTP/1.1\r\nHost: " +
        address + ":" + std::to_string(port) + "\r\n" +
        "Content-Type: " + contenttype + "\r\n" +
        "Content-Length: " + std::to_string(sparams.size()) + "\r\n\r\n" +
        sparams;
    return getResponse(request, headers, response);
}

void HttpClient::disconnect() {
    if (sockfd != -1) {
        close(sockfd);
    }
}

HttpClient::~HttpClient() {
    disconnect();
}

#endif
