#include <boost/chrono.hpp>

#include <sstream>
#include <fstream>
#include <iostream>
#include <vector>
#include <cstdlib>


#include <Snap.h>
#include <boost/lexical_cast.hpp>

namespace timens = boost::chrono;
using namespace std;

int main(int argc, const char** argv) {


    //Input Graph
    const TStr InFNm = TStr(argv[1]);
    //Output file
    const TStr OutFNm = TStr(argv[2]);
    //File with list of noes
    const TStr argsMod = TStr(argv[3]);

    printf("Loading %s...", InFNm.CStr());
    TIntFltH BtwH, EigH, PRankH, CcfH, CloseH, HubH, AuthH;

    std::chrono::milliseconds durationPR;
    PNGraph Graph = TSnap::LoadEdgeList<PNGraph>(InFNm);
    PUNGraph UGraph = TSnap::ConvertGraph<PUNGraph>(Graph); 

    std::chrono::system_clock::time_point start = std::chrono::system_clock::now();
    TSnap::GetPageRank(Graph, PRankH, 0.85, 0, 100);
    durationPR = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now() - start);
    cout << endl << "Runtime PR: " << durationPR.count() << " ms." << endl;

    start = std::chrono::system_clock::now();
    TSnap::GetHits(Graph, HubH, AuthH);
    std::chrono::milliseconds durationHits = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now() - start);
    cout << endl << "Runtime Hits: " << durationHits.count() << " ms." << endl;

    //start = std::chrono::system_clock::now();
    //TSnap::GetEigenVectorCentr(UGraph, EigH);
    //std::chrono::milliseconds durationEV = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now() - start);
    //cout << endl << "Runtime EigenVector: " << durationEV.count() << " ms." << endl;
 
    start = std::chrono::system_clock::now();
    TSnap::GetNodeClustCf(UGraph, CcfH);
    std::chrono::milliseconds durationCF = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now() - start);
    cout << endl << "Runtime ClusterCoeff: " << durationCF.count() << " ms." << endl;

    start = std::chrono::system_clock::now();
    long c = TSnap::CountTriangles(Graph);
    std::chrono::milliseconds durationTriangles = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now() - start);
    cout << endl << "Runtime Triangles: " << durationTriangles.count() << " ms." << c << endl;

    start = std::chrono::system_clock::now();
    long output = TSnap::GetBfsFullDiam(Graph, 1000, true);
    std::chrono::milliseconds durationDiam = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now() - start);
    cout << endl << "Runtime Diameter: " << durationDiam.count() << " ms." << output << endl;

    start = std::chrono::system_clock::now();
    double outputWcc = TSnap::GetMxWccSz(Graph);
    std::chrono::milliseconds durationMaxWcc = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now() - start);
    cout << endl << "Runtime MaxWcc: " << durationMaxWcc.count() << " ms." << outputWcc << endl;

    start = std::chrono::system_clock::now();
    double outputScc = TSnap::GetMxSccSz(Graph);
    std::chrono::milliseconds durationMaxScc = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now() - start);
    cout << endl << "Runtime MaxScc: " << durationMaxScc.count() << " ms." << outputScc << endl;

    TIntV Values;
    std::ifstream ifs(string(argsMod.CStr()));
    std::string line;
    while (std::getline(ifs, line)) {
        int v = boost::lexical_cast<int>(line);
        Values.Add(v);
    }
    ifs.close();
    const int edges = Graph->GetEdges();
    start = std::chrono::system_clock::now();
    double outputMod = TSnap::GetModularity(Graph, Values, edges);
    std::chrono::milliseconds durationMod = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now() - start);
    cout << endl << "Runtime Mod: " << durationMod.count() << " ms. " << outputMod << endl;

    start = std::chrono::system_clock::now();
    TSnap::GetBetweennessCentr(Graph, BtwH);
    std::chrono::milliseconds durationBetCentr = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now() - start);
    cout << endl << "Runtime BetCentr: " << durationBetCentr.count() << " ms." << endl;

    return 0;
}
