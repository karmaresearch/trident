#include <trident/sparql/query.h>
#include <trident/sparql/plan.h>

#include <kognac/progargs.h>

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

#include <string>

using namespace std;

DDLEXPORT void execNativeQuery(ProgramArgs &vm, Querier *q, KB &kb, bool silent);
DDLEXPORT void callRDF3X(TridentLayer &db, const string &queryFileName, bool explain,
	bool disableBifocalSampling, bool resultslookup);

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
        std::shared_ptr<QueryGraph> &queryGraph,
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
    queryGraph = std::shared_ptr<QueryGraph>(new QueryGraph(parser.getVarCount()));

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

void callRDF3X(TridentLayer &db, const string &queryFileName, bool explain,
        bool disableBifocalSampling, bool resultslookup) {
    QueryDict queryDict(db.getNextId());
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
    std::shared_ptr<QueryGraph> queryGraph;

    std::chrono::system_clock::time_point start = std::chrono::system_clock::now();
    parseQuery(parsingOk, parser, queryGraph,
            queryDict, db);
    if (!parsingOk) {
        std::chrono::duration<double> duration = std::chrono::system_clock::now() - start;
        LOG(INFOL) << "Runtime queryopti: 0ms.";
        LOG(INFOL) << "Runtime queryexec: 0ms.";
        LOG(INFOL) << "Runtime totalexec: " << duration.count() * 1000 << "ms.";
        LOG(INFOL) << "# rows = 0";
        return;
    }

    // Run the optimizer
    PlanGen plangen;
    Plan* plan = NULL;
    if (!disableBifocalSampling) {
        plan = plangen.translate(db, *queryGraph.get());
    } else {
        db.disableBifocalSampling();
        plan = plangen.translate(db, *queryGraph.get(), false);
    }

    if (!plan) {
        cerr << "internal error plan generation failed" << endl;
        return;
    }
    std::chrono::duration<double> durationO = std::chrono::system_clock::now() - start;

    // Build a physical plan
    Runtime runtime(db, NULL, &queryDict);
    Operator* operatorTree = CodeGen().translate(runtime, *queryGraph.get(), plan, !resultslookup);

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
        LOG(INFOL) << "Runtime queryopti: " << durationO.count() * 1000 << "ms.";
        LOG(INFOL) << "Runtime queryexec: " << durationQ.count() * 1000 << "ms.";
        LOG(INFOL) << "Runtime totalexec: " << duration.count() * 1000 << "ms.";
        ResultsPrinter *p = (ResultsPrinter*) operatorTree;
        uint64_t nElements = p->getPrintedRows();
        LOG(INFOL) << "# rows = " << nElements;
        delete operatorTree;
    }
}

void execNativeQuery(ProgramArgs &vm, Querier *q, KB &kb, bool silent) {
    uint64_t nElements = 0;
    char bufferTerm[MAX_TERM_SIZE];
    DictMgmt *dict = kb.getDictMgmt();
    string queryFileName = vm["query"].as<string>();
    TridentLayer db(kb);

    //Parse the query
    QueryDict queryDict(db.getNextId());
    bool parsingOk;

    std::fstream inFile;
    inFile.open(queryFileName);//open the input file
    std::stringstream strStream;
    strStream << inFile.rdbuf();//read the file
    SPARQLLexer lexer(strStream.str());
    SPARQLParser parser(lexer);
    std::shared_ptr<QueryGraph> queryGraph;

    std::chrono::system_clock::time_point start = std::chrono::system_clock::now();

    parseQuery(parsingOk, parser, queryGraph, queryDict, db);
    if (!parsingOk) {
        std::chrono::duration<double> duration = std::chrono::system_clock::now() - start;
        LOG(INFOL) << "Runtime queryopti: 0ms.";
        LOG(INFOL) << "Runtime queryexec: 0ms.";
        LOG(INFOL) << "Runtime totalexec: " << duration.count() * 1000 << "ms.";
        return;
    }

    std::unique_ptr<Query> query  = createQueryFromRF3XQueryGraph(parser,
            *queryGraph.get());
    TridentQueryPlan plan(q);
    plan.create(*query.get(), SIMPLE);
    std::chrono::duration<double> durationO = std::chrono::system_clock::now() - start;

    //Output plan
#ifdef DEBUG
    LOG(DEBUGL) << "Translated plan:";
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
        LOG(INFOL) << "Runtime queryopti: " << durationO.count() * 1000 << "ms.";
        LOG(INFOL) << "Runtime queryexec: " << sec.count() * 1000 << "ms.";
        LOG(INFOL) << "Runtime totalexec: " << secT.count() * 1000 << "ms.";
        LOG(INFOL) << "# rows = " << nElements;
        plan.releaseIterator(root);
    }
    //Print stats dictionary
}
