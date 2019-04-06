#include <trident/server/server.h>
#include <trident/utils/httpclient.h>

#include <cts/parser/SPARQLLexer.hpp>
#include <cts/semana/SemanticAnalysis.hpp>
#include <cts/plangen/PlanGen.hpp>
#include <cts/codegen/CodeGen.hpp>
#include <rts/runtime/Runtime.hpp>
#include <rts/operator/Operator.hpp>
#include <rts/operator/PlanPrinter.hpp>
#include <rts/operator/ResultsPrinter.hpp>

#include <kognac/utils.h>

//#include <curl/curl.h>

#include <string>
#include <fstream>
#include <chrono>
#include <thread>
#include <regex>

TridentServer::TridentServer(KB &kb, string htmlfiles, int nthreads) :
    kb(kb),
    dirhtmlfiles(htmlfiles),
    isActive(false), nthreads(nthreads) {

    }

void TridentServer::startThread(int port) {
    this->webport = port;
    server->start();
}

void TridentServer::start(int port) {
    auto f = std::bind(&TridentServer::processRequest, this,
            std::placeholders::_1,
            std::placeholders::_2);
    server = std::shared_ptr<HttpServer>(new HttpServer(port,
                f, nthreads));
    t = std::thread(&TridentServer::startThread, this, port);
}

void TridentServer::stop() {
    LOG(INFOL) << "Stopping server ...";
    while (isActive) {
        std::this_thread::sleep_for(chrono::milliseconds(100));
    }
    server->stop();
    LOG(INFOL) << "Done";
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
        std::unique_ptr<QueryGraph> &queryGraph,
        QueryDict &queryDict,
        TridentLayer &db) {

    //Sometimes the query introduces new constants which need an ID
    try {
        parser.parse(false, true);
    } catch (const SPARQLParser::ParserException& e) {
        cerr << "parse error: " << e.message << endl;
        success = false;
        return;
    }
    queryGraph = std::unique_ptr<QueryGraph>(
            new QueryGraph(parser.getVarCount()));
    // And perform the semantic anaylsis
    try {
        SemanticAnalysis semana(db, queryDict);
        semana.transform(parser, *queryGraph.get());
    } catch (const SemanticAnalysis::SemanticException& e) {
        cerr << "semantic error: " << e.message << endl;
        success = false;
        return;
    }
    if (queryGraph->knownEmpty()) {
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
        int64_t nterms,
        TridentLayer &db,
        bool printstdout,
        bool jsonoutput,
        JSON *jsonvars,
        JSON *jsonresults,
        JSON *jsonstats) {
    std::unique_ptr<QueryDict> queryDict = std::unique_ptr<QueryDict>(
            new QueryDict(nterms));
    std::unique_ptr<QueryGraph> queryGraph;
    bool parsingOk;

    std::unique_ptr<SPARQLLexer> lexer =
        std::unique_ptr<SPARQLLexer>(new SPARQLLexer(sparqlquery));
    std::unique_ptr<SPARQLParser> parser = std::unique_ptr<SPARQLParser>(
            new SPARQLParser(*lexer.get()));
    std::chrono::system_clock::time_point start = std::chrono::system_clock::now();
    parseQuery(parsingOk, *parser.get(), queryGraph, *queryDict.get(), db);
    if (!parsingOk) {
        std::chrono::duration<double> duration = std::chrono::system_clock::now() - start;
        LOG(INFOL) << "Runtime query: 0ms.";
        LOG(INFOL) << "Runtime total: " << duration.count() * 1000 << "ms.";
        LOG(INFOL) << "# rows = 0";
        return;
    }

    std::vector<string> jsonnamevars;
    if (jsonvars) {
        //Copy the output of the query in the json vars
        for (QueryGraph::projection_iterator itr = queryGraph->projectionBegin();
                itr != queryGraph->projectionEnd(); ++itr) {
            string namevar = parser->getVariableName(*itr);
            jsonvars->push_back(namevar);
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

        std::chrono::system_clock::time_point startQ = std::chrono::system_clock::now();
        if (operatorTree->first()) {
            while (operatorTree->next());
        }
        std::chrono::duration<double> durationQ = std::chrono::system_clock::now() - startQ;
        std::chrono::duration<double> duration = std::chrono::system_clock::now() - start;
        LOG(INFOL) << "Runtime query: " << durationQ.count() * 1000 << "ms.";
        LOG(INFOL) << "Runtime total: " << duration.count() * 1000 << "ms.";
        if (jsonstats) {
            jsonstats->put("runtime", to_string(durationQ.count()));
            jsonstats->put("nresults", to_string(p->getPrintedRows()));

        }
        if (printstdout) {
            uint64_t nElements = p->getPrintedRows();
            LOG(INFOL) << "# rows = " << nElements;
        }
        delete operatorTree;
    }
    delete plangen;
}

void TridentServer::processRequest(std::string req, std::string &res) {
    setActive();
    //Get the page
    string page;
    string message = "";
    bool isjson = false;

    if (Utils::starts_with(req, "POST")) {
        int pos = req.find("HTTP");
        string path = req.substr(5, pos - 6);
        if (path == "/sparql") {
            //Get the SPARQL query
            string form = req.substr(req.find("application/x-www-form-urlencoded"));
            string printresults = _getValueParam(form, "print");
            string sparqlquery = _getValueParam(form, "query");
            sparqlquery = HttpClient::unescape(sparqlquery);
            std::regex e1("\\+");
            std::string replacedString;
            std::regex_replace(std::back_inserter(replacedString),
                    sparqlquery.begin(), sparqlquery.end(),
                    e1, "$1 ");
            sparqlquery = replacedString;
            std::regex e2("\\r\\n");
            replacedString = "";
            std::regex_replace(std::back_inserter(replacedString),
                    sparqlquery.begin(), sparqlquery.end(), e2, "$1\n");
            sparqlquery = replacedString;

            //Execute the SPARQL query
            JSON pt;
            JSON vars;
            JSON bindings;
            JSON stats;
            bool jsonoutput = printresults != string("false");
            TridentServer::execSPARQLQuery(sparqlquery,
                    false,
                    kb.getNTerms(),
                    kb,
                    false,
                    jsonoutput,
                    &vars,
                    &bindings,
                    &stats);
            JSON head;
            head.add_child("vars", vars);
            pt.add_child("head", head);
            JSON results;
            results.add_child("bindings", bindings);
            pt.add_child("results", results);
            pt.add_child("stats", stats);

            std::ostringstream buf;
            JSON::write(buf, pt);
            page = buf.str();
            isjson = true;
        } else if (path == "/lookup") {
            string form = req.substr(req.find("application/x-www-form-urlencoded"));
            string id = _getValueParam(form, "id");
            //Lookup the value
            string value = lookup(id, kb);
            JSON pt;
            pt.put("value", value);
            std::ostringstream buf;
            JSON::write(buf, pt);
            page = buf.str();
            isjson = true;
        } else {
            page = "Error!";
        }
    } else if (Utils::starts_with(req, "GET")) {
        //Get the page
        int pos = req.find("HTTP");
        string path = req.substr(4, pos - 5);
        if (path.size() > 1) {
            page = getPage(path);
        }
    }

    if (page == "") {
        //return the main page
        page = getDefaultPage();
    }
    if (isjson) {
        res = "HTTP/1.1 200 OK\r\nContent-Type: application/json\nContent-Length: ";
        res += to_string(page.size()) + "\r\n\r\n" + page;
    } else {
        res = "HTTP/1.1 200 OK\r\nContent-Length: ";
        res+= to_string(page.size()) + "\r\n\r\n" + page;
    }

    /*boost::asio::async_write(socket, boost::asio::buffer(res),
      boost::asio::transfer_all(),
      boost::bind(&Server::writeHandler,
      shared_from_this(),
      boost::asio::placeholders::error,
      boost::asio::placeholders::bytes_transferred));*/

    setInactive();
}

/*void TridentServer::Server::writeHandler(const boost::system::error_code &err,
  std::size_t bytes) {
  socket.close();
  inter->connect();
  };*/

string TridentServer::getDefaultPage() {
    return getPage("/index.html");
}

string TridentServer::getPage(string f) {
    if (cachehtml.count(f)) {
        return cachehtml.find(f)->second;
    }

    //Read the file (if any) and return it to the user
    string pathfile = dirhtmlfiles + "/" + f;
    if (Utils::exists(pathfile)) {
        //Read the content of the file
        LOG(DEBUGL) << "Reading the content of " << pathfile;
        ifstream ifs(pathfile);
        stringstream sstr;
        sstr << ifs.rdbuf();
        string contentFile = sstr.str();
        //Replace WEB_PORT with the right port
        size_t index = 0;
        index = contentFile.find("WEB_PORT", index);
        if (index != std::string::npos)
            contentFile.replace(index, 8, to_string(webport));

        cachehtml.insert(make_pair(f, contentFile));
        return contentFile;
    }
    return "Error! I cannot find the page to show.";
}
