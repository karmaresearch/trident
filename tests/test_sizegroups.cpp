#include "../src/storage/pairstorage.h"
#include "../src/tree/root.h"
#include "../src/kb/consts.h"
#include "../src/kb/kbconfig.h"
#include "../src/kb/memoryopt.h"
#include "../src/kb/querier.h"
#include "../src/storage/storagestrat.h"

#include <boost/log/trivial.hpp>
#include <boost/log/utility/setup/console.hpp>
#include <boost/log/core.hpp>
#include <boost/log/expressions.hpp>
#include <boost/log/support/date_time.hpp>
#include <boost/log/utility/setup/common_attributes.hpp>

namespace logging = boost::log;

int main(int argc, char **argv) {

    logging::add_common_attributes();
    logging::add_console_log(std::cerr,
                             logging::keywords::format =
                                 (logging::expressions::stream << "["
                                  << logging::expressions::attr <
                                  boost::log::attributes::current_thread_id::value_type > (
                                      "ThreadID") << " "
                                  << logging::expressions::format_date_time <
                                  boost::posix_time::ptime > ("TimeStamp",
                                          "%H:%M:%S") << " - "
                                  << logging::trivial::severity << "] "
                                  << logging::expressions::smessage));
    boost::shared_ptr<logging::core> core = logging::core::get();



    //Init
    KBConfig config;
    KB kb(argv[1], true, config);



    /*PropertyMap map;
    map.setBool(TEXT_KEYS, false);
    map.setBool(TEXT_VALUES, false);
    map.setBool(COMPRESSED_NODES, false);
    map.setInt(LEAF_SIZE_PREALL_FACTORY,
            config.getParamInt(TREE_MAXPREALLLEAVESCACHE));
    map.setInt(LEAF_SIZE_FACTORY, config.getParamInt(TREE_MAXLEAVESCACHE));
    map.setInt(MAX_NODES_IN_CACHE, config.getParamInt(TREE_MAXNODESINCACHE));
    map.setInt(NODE_MIN_BYTES, config.getParamInt(TREE_NODEMINBYTES));
    map.setLong(CACHE_MAX_SIZE, config.getParamLong(TREE_MAXSIZECACHETREE));
    map.setInt(FILE_MAX_SIZE, config.getParamInt(TREE_MAXFILESIZE));
    map.setInt(MAX_N_OPENED_FILES, config.getParamInt(TREE_MAXNFILES));
    map.setInt(MAX_EL_PER_NODE, config.getParamInt(TREE_MAXELEMENTSNODE));

    map.setInt(LEAF_MAX_PREALL_INTERNAL_LINES,
            config.getParamInt(TREE_MAXPREALLINTERNALLINES));
    map.setInt(LEAF_MAX_INTERNAL_LINES,
            config.getParamInt(TREE_MAXINTERNALLINES));
    map.setInt(LEAF_ARRAYS_FACTORY_SIZE, config.getParamInt(TREE_FACTORYSIZE));
    map.setInt(LEAF_ARRAYS_PREALL_FACTORY_SIZE,
            config.getParamInt(TREE_ALLOCATEDELEMENTS));

    map.setInt(NODE_KEYS_FACTORY_SIZE,
            config.getParamInt(TREE_NODE_KEYS_FACTORY_SIZE));
    map.setInt(NODE_KEYS_PREALL_FACTORY_SIZE,
            config.getParamInt(TREE_NODE_KEYS_PREALL_FACTORY_SIZE));
    Root tree(argv[2], NULL, true, map);*/

    int permutation = atoi(argv[2]);

    //Calculate the statistics
    TreeItr *itr = kb.getItrTerms();
    Querier *q = kb.query();
    DictMgmt *dm = kb.getDictMgmt();
    char supportTerm[512];

    //Stats
    int64_t ngroups = 0;
    int64_t totalElGroups = 0;
    int64_t maxSizeGroup = 0;

    int64_t elconschain = 0;
    int64_t nconschain = 0;

    while (itr->hasNext()) {
        TermCoordinates value;
        int64_t key = itr->next(&value);

        if (!value.exists(1)) {
            continue;
        }

        ngroups++;
//        if (ngroups % 1000000 == 0)
//            cout << "Processing key " << key << endl;

        PairItr *pi = q->getPairIterator(&value, permutation, -1, -1);

        int64_t elgroup = 0;
        int64_t prevEl = -1;
        int64_t sizeChain = 1;
        int64_t prevFirstTerm = -1;
        int64_t chainsPerGroup = 0;
        while (pi->has_next()) {
            pi->next_pair();
            elgroup++;
            if (key == 4118110 || key == 4401) {
                dm->getText(pi->getValue1(), supportTerm);
                string one(supportTerm);
                dm->getText(pi->getValue2(), supportTerm);
                string two(supportTerm);
                cout << "\t\t\t" << one << " " << pi->getValue1() << " " << two << " " << pi->getValue2() << endl;

            }
            if (prevFirstTerm != pi->getValue1()) {
                prevEl = -1;
                sizeChain = 1;
                prevFirstTerm = pi->getValue1();
            }

            if (prevEl != -1) {
                if (pi->getValue2() == prevEl + 1) {
                    sizeChain++;
                } else {
                    if (sizeChain > 2) {
                        chainsPerGroup++;
                        elconschain += sizeChain;
                    }
                    sizeChain = 1;
                    prevEl = -1;
                }
            }
            prevEl = pi->getValue2();
        }
        nconschain += chainsPerGroup;
        totalElGroups += elgroup;
        if (elgroup > maxSizeGroup) {
            maxSizeGroup = elgroup;
        }
        q->releaseItr(pi);

        dm->getText(key, supportTerm);
        unsigned char strat = value.getStrategy(1);
        cout << "Key " << supportTerm << "-" << key << " " << chainsPerGroup << " " << chainsPerGroup << " number elements " << elgroup << " strategy " << (((strat >> 7) != 0) ? "COMPR" : "LIST") << endl;
    }

    cout << "N. groups: " << ngroups << endl;
    cout << "Avg. sizeGroup: " << (double) totalElGroups / ngroups << endl;
    cout << "Max size group: " << maxSizeGroup << endl;
    cout << "N. consecutive chains: " << nconschain << endl;
    cout << "Avg. length consecutive chains: " << (double)elconschain / nconschain << endl;

    delete q;
    delete itr;
}



