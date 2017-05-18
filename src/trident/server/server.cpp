#include <trident/server/server.h>

#include <cts/parser/SPARQLLexer.hpp>
#include <cts/semana/SemanticAnalysis.hpp>
#include <cts/plangen/PlanGen.hpp>
#include <cts/codegen/CodeGen.hpp>
#include <rts/runtime/Runtime.hpp>
#include <rts/operator/Operator.hpp>
#include <rts/operator/PlanPrinter.hpp>
#include <rts/operator/ResultsPrinter.hpp>

#include <kognac/utils.h>

#include <boost/algorithm/string/predicate.hpp>
#include <boost/algorithm/string/replace.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/filesystem.hpp>

#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/json_parser.hpp>

#include <curl/curl.h>

#include <string>
#include <fstream>
#include <chrono>
#include <thread>

using boost::property_tree::ptree;
using boost::property_tree::read_json;
using boost::property_tree::write_json;

TridentServer::TridentServer(KB &kb, string htmlfiles) : kb(kb),
    dirhtmlfiles(htmlfiles),
    acceptor(io), resolver(io),
    isActive(false) {
    }

void TridentServer::startThread(string address, string port) {
    this->webport = port;
    boost::asio::ip::tcp::resolver::query query(address, port);
    boost::asio::ip::tcp::endpoint endpoint = *resolver.resolve(query);
    acceptor.open(endpoint.protocol());
    acceptor.set_option(boost::asio::ip::tcp::acceptor::reuse_address(true));
    acceptor.bind(endpoint);
    acceptor.listen();
    connect();
    io.run();
}

void TridentServer::start(string address, string port) {
    t = boost::thread(&TridentServer::startThread, this, address, port);
}

void TridentServer::stop() {
    BOOST_LOG_TRIVIAL(info) << "Stopping server ...";
    while (isActive) {
        std::this_thread::sleep_for(chrono::milliseconds(100));
    }
    acceptor.cancel();
    acceptor.close();
    io.stop();
    BOOST_LOG_TRIVIAL(info) << "Done";
}

void TridentServer::connect() {
    boost::shared_ptr<Server> conn(new Server(io, this));
    acceptor.async_accept(conn->socket,
            boost::bind(&Server::acceptHandler,
                conn, boost::asio::placeholders::error));
};

void TridentServer::Server::acceptHandler(const boost::system::error_code &err) {
    if (err == boost::system::errc::success) {
        socket.async_read_some(boost::asio::buffer(data_.get(), 4096),
                boost::bind(&Server::readHeader, shared_from_this(),
                    boost::asio::placeholders::error,
                    boost::asio::placeholders::bytes_transferred));
    }
}

string _getValueParam(string req, string param) {
    int pos = req.find(param);
    if (pos == string::npos) {
        return "";
    } else {
        int postart = req.find("=", pos);
        int posend = req.find("&", pos);
        if (posend == string::npos) {
            return req.substr(postart + 1);
        } else {
            return req.substr(postart + 1, posend - postart - 1);
        }
    }
}

void TridentServer::parseQuery(bool &success,
        SPARQLParser &parser,
        QueryGraph &queryGraph,
        QueryDict &queryDict,
        TridentLayer &db) {

    //Sometimes the query introduces new constants which need an ID
    try {
        parser.parse();
    } catch (const SPARQLParser::ParserException& e) {
        cerr << "parse error: " << e.message << endl;
        success = false;
        return;
    }

    // And perform the semantic anaylsis
    try {
        SemanticAnalysis semana(db, queryDict);
        semana.transform(parser, queryGraph);
    } catch (const SemanticAnalysis::SemanticException& e) {
        cerr << "semantic error: " << e.message << endl;
        success = false;
        return;
    }
    if (queryGraph.knownEmpty()) {
        cout << "<empty result -- known empty>" << endl;
        success = false;
        return;
    }

    success = true;
    return;
}

string TridentServer::lookup(string sId, TridentLayer &db) {
    const char *start;
    const char *end;
    unsigned id = stoi(sId);
    ::Type::ID type;
    unsigned st;
    db.lookupById(id, start, end, type, st);
    return string(start, end - start);
}

void TridentServer::execSPARQLQuery(string sparqlquery,
        bool explain,
        long nterms,
        TridentLayer &db,
        bool printstdout,
        bool jsonoutput,
        boost::property_tree::ptree *jsonvars,
        boost::property_tree::ptree *jsonresults,
        boost::property_tree::ptree *jsonstats) {
    std::unique_ptr<QueryDict> queryDict = std::unique_ptr<QueryDict>(new QueryDict(nterms));
    std::unique_ptr<QueryGraph> queryGraph = std::unique_ptr<QueryGraph>(new QueryGraph());
    bool parsingOk;

    std::unique_ptr<SPARQLLexer> lexer =
        std::unique_ptr<SPARQLLexer>(new SPARQLLexer(sparqlquery));
    std::unique_ptr<SPARQLParser> parser = std::unique_ptr<SPARQLParser>(
            new SPARQLParser(*lexer.get()));
    boost::chrono::system_clock::time_point start = boost::chrono::system_clock::now();
    parseQuery(parsingOk, *parser.get(), *queryGraph.get(), *queryDict.get(), db);
    if (!parsingOk) {
        boost::chrono::duration<double> duration = boost::chrono::system_clock::now() - start;
        BOOST_LOG_TRIVIAL(info) << "Runtime query: 0ms.";
        BOOST_LOG_TRIVIAL(info) << "Runtime total: " << duration.count() * 1000 << "ms.";
        BOOST_LOG_TRIVIAL(info) << "# rows = 0";
        return;
    }

    std::vector<string> jsonnamevars;
    if (jsonvars) {
        //Copy the output of the query in the json vars
        for (QueryGraph::projection_iterator itr = queryGraph->projectionBegin();
                itr != queryGraph->projectionEnd(); ++itr) {
            string namevar = parser->getVariableName(*itr);
            boost::property_tree::ptree var;
            var.put("", namevar);
            jsonvars->push_back(std::make_pair("", var));
            jsonnamevars.push_back(namevar);
        }
    }

    // Run the optimizer
    PlanGen *plangen = new PlanGen();
    Plan* plan = plangen->translate(db, *queryGraph.get(), false);
    // delete plangen;  Commented out, because this also deletes all plans!
    // In particular, it corrupts the current plan.
    // --Ceriel
    if (!plan) {
        cerr << "internal error plan generation failed" << endl;
        delete plangen;
        return;
    }
    if (explain)
        plan->print(0);

    // Build a physical plan
    Runtime runtime(db, NULL, queryDict.get());
    Operator* operatorTree = CodeGen().translate(runtime, *queryGraph.get(), plan, false);

    // Execute it
    if (explain) {
        DebugPlanPrinter out(runtime, false);
        operatorTree->print(out);
        delete operatorTree;
    } else {
#if DEBUG
        DebugPlanPrinter out(runtime, false);
        operatorTree->print(out);
#endif
        //set up output options for the last operators
        ResultsPrinter *p = (ResultsPrinter*) operatorTree;
        p->setSilent(!printstdout);
        if (jsonoutput) {
            p->setJSONOutput(jsonresults, jsonnamevars);
        }

        boost::chrono::system_clock::time_point startQ = boost::chrono::system_clock::now();
        if (operatorTree->first()) {
            while (operatorTree->next());
        }
        boost::chrono::duration<double> durationQ = boost::chrono::system_clock::now() - startQ;
        boost::chrono::duration<double> duration = boost::chrono::system_clock::now() - start;
        BOOST_LOG_TRIVIAL(info) << "Runtime query: " << durationQ.count() * 1000 << "ms.";
        BOOST_LOG_TRIVIAL(info) << "Runtime total: " << duration.count() * 1000 << "ms.";
        if (jsonstats) {
            jsonstats->put("runtime", to_string(durationQ.count()));
            jsonstats->put("nresults", to_string(p->getPrintedRows()));

        }
        if (printstdout) {
            long nElements = p->getPrintedRows();
            BOOST_LOG_TRIVIAL(info) << "# rows = " << nElements;
        }
        delete operatorTree;
    }
    delete plangen;
}

void TridentServer::Server::readHeader(boost::system::error_code const &err,
        size_t bytes) {
    inter->setActive();

    ss << string(data_.get(), bytes);
    string tmpstring = ss.str();
    int pos = tmpstring.find("Content-Length:");
    if (pos != string::npos) {
        int endpos = tmpstring.find("\r\n\r\n", pos);
        string slenparams = tmpstring.substr(pos + 16, endpos - pos - 16);
        int lenparams = std::stoi(slenparams);
        int remsize = tmpstring.size() - endpos - 4;
        if (remsize < lenparams) {
            //I must keep reading ...
            acceptHandler(err);
            return;
        }
    }

    req = ss.str();
    //Get the page
    string page;
    string message = "";
    bool isjson = false;

    if (boost::starts_with(req, "POST")) {
        int pos = req.find("HTTP");
        string path = req.substr(5, pos - 6);
        if (path == "/sparql") {
            //Get the SPARQL query
            string form = req.substr(req.find("application/x-www-form-urlencoded"));
            string printresults = _getValueParam(form, "print");
            string sparqlquery = _getValueParam(form, "query");
            //Decode the query
            CURL *curl;
            curl = curl_easy_init();
            if (curl) {
                int newlen = 0;
                char *un2 = curl_easy_unescape(curl, sparqlquery.c_str(),
                        sparqlquery.size(), &newlen);
                sparqlquery = string(un2, newlen);
                curl_free(un2);
                boost::algorithm::replace_all(sparqlquery, "+", " ");
                boost::algorithm::replace_all(sparqlquery, "\r\n", "\n");
            } else {
                throw 10;
            }
            curl_easy_cleanup(curl);

            //Execute the SPARQL query
            ptree pt;
            ptree vars;
            ptree bindings;
            ptree stats;
            bool jsonoutput = printresults != string("false");
            TridentServer::execSPARQLQuery(sparqlquery,
                    false,
                    inter->kb.getNTerms(),
                    inter->kb,
                    false,
                    jsonoutput,
                    &vars,
                    &bindings,
                    &stats);
            pt.add_child("head.vars", vars);
            pt.add_child("results.bindings", bindings);
            pt.add_child("stats", stats);

            std::ostringstream buf;
            write_json(buf, pt, false);
            page = buf.str();
            isjson = true;
        } else if (path == "/lookup") {
            string form = req.substr(req.find("application/x-www-form-urlencoded"));
            string id = _getValueParam(form, "id");
            //Lookup the value
            string value = lookup(id, inter->kb);
            ptree pt;
            pt.put("value", value);
            std::ostringstream buf;
            write_json(buf, pt, false);
            page = buf.str();
            isjson = true;
        } else {
            page = "Error!";
        }
    } else if (boost::starts_with(req, "GET")) {
        //Get the page
        int pos = req.find("HTTP");
        string path = req.substr(4, pos - 5);
        if (path.size() > 1) {
            page = inter->getPage(path);
        }
    }

    if (page == "") {
        //return the main page
        page = inter->getDefaultPage();
    }
    if (isjson) {
        res = "HTTP/1.1 200 OK\r\nContent-Type: application/json\nContent-Length: " + to_string(page.size()) + "\r\n\r\n" + page;
    } else {
        res = "HTTP/1.1 200 OK\r\nContent-Length: " + to_string(page.size()) + "\r\n\r\n" + page;
    }

    boost::asio::async_write(socket, boost::asio::buffer(res),
            boost::asio::transfer_all(),
            boost::bind(&Server::writeHandler,
                shared_from_this(),
                boost::asio::placeholders::error,
                boost::asio::placeholders::bytes_transferred));

    inter->setInactive();
}

void TridentServer::Server::writeHandler(const boost::system::error_code &err,
        std::size_t bytes) {
    socket.close();
    inter->connect();
};

string TridentServer::getDefaultPage() {
    return getPage("/index.html");
}

string TridentServer::getPage(string f) {
    if (cachehtml.count(f)) {
        return cachehtml.find(f)->second;
    }

    //Read the file (if any) and return it to the user
    string pathfile = dirhtmlfiles + "/" + f;
    if (boost::filesystem::exists(boost::filesystem::path(pathfile))) {
        //Read the content of the file
        BOOST_LOG_TRIVIAL(debug) << "Reading the content of " << pathfile;
        ifstream ifs(pathfile);
        stringstream sstr;
        sstr << ifs.rdbuf();
        string contentFile = sstr.str();
        //Replace WEB_PORT with the right port
        size_t index = 0;
        index = contentFile.find("WEB_PORT", index);
        if (index != std::string::npos)
            contentFile.replace(index, 8, webport);

        cachehtml.insert(make_pair(f, contentFile));
        return contentFile;
    }

    return "Error! I cannot find the page to show.";
}
