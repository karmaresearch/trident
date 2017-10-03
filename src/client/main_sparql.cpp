#include <trident/sparql/query.h>
#include <trident/sparql/plan.h>

//RDF3X dependencies
#include <layers/TridentLayer.hpp>
#include <cts/parser/SPARQLLexer.hpp>
#include <cts/parser/SPARQLParser.hpp>
#include <cts/infra/QueryGraph.hpp>
#include <cts/semana/SemanticAnalysis.hpp>
#include <cts/plangen/PlanGen.hpp>
#include <cts/codegen/CodeGen.hpp>
#include <rts/runtime/Runtime.hpp>
#include <rts/runtime/QueryDict.hpp>
#include <rts/operator/Operator.hpp>
#include <rts/operator/PlanPrinter.hpp>
#include <rts/operator/ResultsPrinter.hpp>
//END RDF3x dependencies

#include <boost/program_options.hpp>

#include <string>

using namespace std;

namespace po = boost::program_options;

std::unique_ptr<Query> createQueryFromRF3XQueryGraph(SPARQLParser &parser,
        QueryGraph &graph) {
    std::vector<Pattern*> patterns;
    std::vector<Filter*> filters;
    std::vector<std::string> projections;

    for (QueryGraph::projection_iterator itr = graph.projectionBegin();
            itr != graph.projectionEnd(); ++itr) {
        //Get the text
        projections.push_back(parser.getVariableName(*itr));
    }

    //Create a "Pattern" object for each element in the graph
    QueryGraph::SubQuery &sb = graph.getQuery();
    for (auto el : sb.nodes) {
        Pattern *pattern = new Pattern();
        if (!el.constSubject) {
            pattern->addVar(0, parser.getVariableName(el.subject));
        } else {
            pattern->subject(el.subject);
        }
        if (!el.constPredicate) {
            pattern->addVar(1, parser.getVariableName(el.predicate));
        } else {
            pattern->predicate(el.predicate);
        }
        if (!el.constObject) {
            pattern->addVar(2, parser.getVariableName(el.object));
        } else {
            pattern->object(el.object);
        }

        patterns.push_back(pattern);
        filters.push_back(NULL);
        el.additionalData = (void*) pattern;
    }

    return std::unique_ptr<Query>(new Query(patterns, filters, projections));
}

void parseQuery(bool &success,
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

void callRDF3X(TridentLayer &db, const string &queryFileName, bool explain,
        bool disableBifocalSampling, bool resultslookup) {
    QueryDict queryDict(db.getNextId());
    QueryGraph queryGraph;
    bool parsingOk;

    // Parse the query
    string queryContent = "";
    if (queryFileName == "") {
        //Read it from STDIN
        cout << "SPARQL Query:" << endl;
        getline(cin, queryContent);
        cout << "QUERY " << queryContent << endl;
    } else {
        std::fstream inFile;
        inFile.open(queryFileName);//open the input file
        std::stringstream strStream;
        strStream << inFile.rdbuf();//read the file
        queryContent = strStream.str();
    }

    SPARQLLexer lexer(queryContent);
    SPARQLParser parser(lexer);

    std::chrono::system_clock::time_point start = std::chrono::system_clock::now();
    parseQuery(parsingOk, parser, queryGraph,
            queryDict, db);
    if (!parsingOk) {
        std::chrono::duration<double> duration = std::chrono::system_clock::now() - start;
        LOG(INFO) << "Runtime queryopti: 0ms.";
        LOG(INFO) << "Runtime queryexec: 0ms.";
        LOG(INFO) << "Runtime totalexec: " << duration.count() * 1000 << "ms.";
        LOG(INFO) << "# rows = 0";
        return;
    }

    // Run the optimizer
    PlanGen plangen;
    Plan* plan = NULL;
    if (!disableBifocalSampling) {
        plan = plangen.translate(db, queryGraph);
    } else {
        db.disableBifocalSampling();
        plan = plangen.translate(db, queryGraph, false);
    }

    if (!plan) {
        cerr << "internal error plan generation failed" << endl;
        return;
    }
    std::chrono::duration<double> durationO = std::chrono::system_clock::now() - start;

    // Build a physical plan
    Runtime runtime(db, NULL, &queryDict);
    Operator* operatorTree = CodeGen().translate(runtime, queryGraph, plan, !resultslookup);

    // Execute it
    if (explain) {
        DebugPlanPrinter out(runtime, false);
        operatorTree->print(out);
        delete operatorTree;
    } else {
        std::chrono::system_clock::time_point startQ = std::chrono::system_clock::now();
        if (operatorTree->first()) {
            while (operatorTree->next());
        }
        std::chrono::duration<double> durationQ = std::chrono::system_clock::now() - startQ;
        std::chrono::duration<double> duration = std::chrono::system_clock::now() - start;
        LOG(INFO) << "Runtime queryopti: " << durationO.count() * 1000 << "ms.";
        LOG(INFO) << "Runtime queryexec: " << durationQ.count() * 1000 << "ms.";
        LOG(INFO) << "Runtime totalexec: " << duration.count() * 1000 << "ms.";
        ResultsPrinter *p = (ResultsPrinter*) operatorTree;
        long nElements = p->getPrintedRows();
        LOG(INFO) << "# rows = " << nElements;
        delete operatorTree;
    }
}

void execNativeQuery(po::variables_map &vm, Querier *q, KB &kb, bool silent) {
    long nElements = 0;
    char bufferTerm[MAX_TERM_SIZE];
    DictMgmt *dict = kb.getDictMgmt();
    string queryFileName = vm["query"].as<string>();
    TridentLayer db(kb);

    //Parse the query
    QueryDict queryDict(db.getNextId());
    QueryGraph queryGraph;
    bool parsingOk;

    std::fstream inFile;
    inFile.open(queryFileName);//open the input file
    std::stringstream strStream;
    strStream << inFile.rdbuf();//read the file
    SPARQLLexer lexer(strStream.str());
    SPARQLParser parser(lexer);

    std::chrono::system_clock::time_point start = std::chrono::system_clock::now();

    parseQuery(parsingOk, parser, queryGraph, queryDict, db);
    if (!parsingOk) {
        std::chrono::duration<double> duration = std::chrono::system_clock::now() - start;
        LOG(INFO) << "Runtime queryopti: 0ms.";
        LOG(INFO) << "Runtime queryexec: 0ms.";
        LOG(INFO) << "Runtime totalexec: " << duration.count() * 1000 << "ms.";
        return;
    }

    std::unique_ptr<Query> query  = createQueryFromRF3XQueryGraph(parser,
            queryGraph);
    TridentQueryPlan plan(q);
    plan.create(*query.get(), SIMPLE);
    std::chrono::duration<double> durationO = std::chrono::system_clock::now() - start;

    //Output plan
#ifdef DEBUG
    LOG(DEBUG) << "Translated plan:";
    plan.print();
#endif

    {
        q->resetCounters();
        std::chrono::system_clock::time_point startQ = std::chrono::system_clock::now();
        TupleIterator *root = plan.getIterator();
        //Execute the query
        const uint8_t nvars = (uint8_t) root->getTupleSize();
        while (root->hasNext()) {
            root->next();
            if (! silent) {
                for (uint8_t i = 0; i < nvars; ++i) {
                    dict->getText(root->getElementAt(i), bufferTerm);
                    std::cout << bufferTerm << ' ';
                }
                std::cout << '\n';
            }
            nElements++;
        }
        std::chrono::duration<double> sec = std::chrono::system_clock::now()
            - startQ;
        std::chrono::duration<double> secT = std::chrono::system_clock::now()
            - start;
        //Print stats
        LOG(INFO) << "Runtime queryopti: " << durationO.count() * 1000 << "ms.";
        LOG(INFO) << "Runtime queryexec: " << sec.count() * 1000 << "ms.";
        LOG(INFO) << "Runtime totalexec: " << secT.count() * 1000 << "ms.";
        LOG(INFO) << "# rows = " << nElements;
        plan.releaseIterator(root);
    }
    //Print stats dictionary
}
