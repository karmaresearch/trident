#include<snap/analytics.h>
#include <snap/directed.h>
#include <snap/undirected.h>

void Analytics::run(KB &kb,
                string nameTask,
                string outputfile,
                string params) {

            //Check what type of graph is stored in the KB
            if (kb.getGraphType() == GraphType::DIRECTED) {
                Analytics::runTask<PTrident_TNGraph, Trident_TNGraph>(kb, nameTask, outputfile, params);
            } else {
                //Graph should be undirected
                if ((kb.getGraphType() != GraphType::UNDIRECTED)) {
                    LOG(ERRORL) << "Graph analytical operations work only on simple directed or simple undirected graphs";
                    throw 10;
                }
                Analytics::runTask<PTrident_UTNGraph, Trident_UTNGraph>(kb, nameTask, outputfile, params);
            }

        }

