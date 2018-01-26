#ifndef _SERVER_H
#define _SERVER_H

/* Code inspired by the tutorial available at http://pastebin.com/1KLsjJLZ */

#include <trident/utils/json.h>
#include <trident/utils/httpserver.h>

#include <layers/TridentLayer.hpp>

#include <cts/infra/QueryGraph.hpp>
#include <cts/parser/SPARQLParser.hpp>
#include <rts/runtime/QueryDict.hpp>

/*#include <boost/asio.hpp>
#include <boost/enable_shared_from_this.hpp>
#include <boost/shared_ptr.hpp>*/

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

        /*boost::asio::io_service io;
          boost::asio::ip::tcp::acceptor acceptor;
          boost::asio::ip::tcp::resolver resolver;*/

        bool isActive;
        int webport;
        std::shared_ptr<HttpServer> server;
        //string webport;

        /*class Server: public boost::enable_shared_from_this<Server> {
          private:
          std::string res, req;
          TridentServer *inter;

          std::ostringstream ss;
          std::unique_ptr<char[]> data_;

          public:
          boost::asio::ip::tcp::socket socket;
          Server(boost::asio::io_service &io, TridentServer *inter):
          inter(inter), socket(io) {
          data_ = std::unique_ptr<char[]>(new char[4096]);
          }

        //OK
        void writeHandler(const boost::system::error_code &err, std::size_t bytes);
        //OK
        void readHeader(boost::system::error_code const &err, size_t bytes);
        //OK
        void acceptHandler(const boost::system::error_code &err);
        };*/

        //OK
        void startThread(int port);

        //OK
        static void parseQuery(bool &success,
                SPARQLParser &parser,
                std::unique_ptr<QueryGraph> &queryGraph,
                QueryDict &queryDict,
                TridentLayer &db);

        void processRequest(std::string req, std::string &resp);

    public:
        //OK
        TridentServer(KB &kb, string htmlfiles);

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
        static void execSPARQLQuery(string sparqlquery,
                bool explain,
                long nterms,
                TridentLayer &db,
                bool printstdout,
                bool jsonoutput,
                JSON *jsonvars,
                JSON *jsonresults,
                JSON *jsonstats);
};
#endif
