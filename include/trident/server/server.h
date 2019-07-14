#ifndef _SERVER_H
#define _SERVER_H

/* Code inspired by the tutorial available at http://pastebin.com/1KLsjJLZ */

#include <trident/utils/json.h>
#include <trident/utils/httpserver.h>
#include <trident/ml/subgraphhandler.h>

#include <layers/TridentLayer.hpp>
#include <kognac/progargs.h>

#include <cts/infra/QueryGraph.hpp>
#include <cts/parser/SPARQLParser.hpp>
#include <rts/runtime/QueryDict.hpp>

#include <faiss/IndexPQ.h>
#include <faiss/IndexIVFPQ.h>
#include <faiss/IndexFlat.h>
#include <faiss/IndexIVFFlat.h>
#include <faiss/index_io.h>

#include <map>

using namespace std;

class TridentServer {
    protected:
        TridentLayer kb;
        std::unique_ptr<SubgraphHandler> sh;
        ProgramArgs &vm;
        faiss::Index *annIndex;

        std::unique_ptr<char[]> buffer;

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

        static void parseQuery(bool &success,
                SPARQLParser &parser,
                std::unique_ptr<QueryGraph> &queryGraph,
                QueryDict &queryDict,
                TridentLayer &db);

        void processRequest(std::string req, std::string &resp);

    public:
        //OK
        TridentServer(KB &kb, ProgramArgs &vm,
                string htmlfiles, int nthreads = 1);

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

        //OK
        void execLinkPrediction(string query,
                int64_t subgraphThreshold, string algo, JSON &response);

        //OK
        static void execSPARQLQuery(string sparqlquery,
                bool explain,
                int64_t nterms,
                TridentLayer &db,
                bool printstdout,
                bool jsonoutput,
                JSON *jsonvars,
                JSON *jsonresults,
                JSON *jsonstats);
};
#endif
