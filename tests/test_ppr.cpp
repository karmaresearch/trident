#include <trident/kb/kb.h>

#include <snap-core/Snap.h>
#include <snap/nativetasks.h>
#include <snap/tasks.h>
#include <snap/directed.h>
#include <snap/undirected.h>

#include <iostream>
#include <map>
#include <sstream>

int main(int argc, const char** argv) {
    KBConfig config;
    KB kb(argv[1], true, false, true, config);
    PTrident_TNGraph Graph = new Trident_TNGraph(&kb);

    std::vector<double> weights(Graph->GetNodes());
    TSnap::GetPageRank_stl<PTrident_TNGraph>(Graph, weights, false, 0.85, 0, 10);

}
