#ifndef _SPARQL_H
#define _SPARQL_H

#include <trident/utils/json.h>

#include <layers/TridentLayer.hpp>
#include <cts/infra/QueryGraph.hpp>
#include <cts/parser/SPARQLParser.hpp>
#include <rts/runtime/QueryDict.hpp>

class SPARQLUtils {
    public:
        static void parseQuery(bool &success,
                SPARQLParser &parser,
                std::unique_ptr<QueryGraph> &queryGraph,
                QueryDict &queryDict,
                TridentLayer &db);

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
