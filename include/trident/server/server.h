#ifndef _SERVER_H
#define _SERVER_H

/* Code inspired by the tutorial available at http://pastebin.com/1KLsjJLZ */

#include <trident/utils/json.h>
#include <trident/utils/httpserver.h>

#include <layers/TridentLayer.hpp>

#include <cts/infra/QueryGraph.hpp>
#include <cts/parser/SPARQLParser.hpp>
#include <rts/runtime/QueryDict.hpp>

#include <map>

using namespace std;

class TridentServer {
    protected:
        TridentLayer kb;

    private:
        string dirhtmlfiles;
        map<string, string> cachehtml;
        std::thread t;
        string cmdArgs;

        bool isActive;
        int webport;
        std::shared_ptr<HttpServer> server;
        int nthreads;

        void startThread(int port);

        void processRequest(std::string req, std::string &resp);

    public:
        //OK
        TridentServer(KB &kb, string htmlfiles, int nthreads = 1);

        //OK
        void start(int port);

        //OK
        void connect();

        //OK
        void stop();

        //OK
        string getDefaultPage();

        //OK
        string getPage(string page);

        //OK
        void setActive() {
            isActive = true;
        }

        //OK
        void setInactive() {
            isActive = false;
        }

        //OK
        void join() {
            t.join();
        }

        //OK
        string getCommandLineArgs() {
            return cmdArgs;
        }

        //OK
        static string lookup(string sId, TridentLayer &db);

};
#endif
