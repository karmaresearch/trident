#include <Snap.h>

#include <chrono>

using namespace std;

int main(int argc, char* argv[]) {
    Env = TEnv(argc, argv, TNotify::StdNotify);
    Env.PrepArgs(TStr::Fmt("Test. build: %s, %s. Time: %s", __TIME__, __DATE__, TExeTm::GetCurTm()));
    const TStr InFNm = Env.GetIfArgPrefixStr("-i:", "../as20graph.txt", "Input un/directed graph");
    std::chrono::milliseconds durationPR;

    PNGraph Graph = TSnap::LoadEdgeList<PNGraph>(InFNm);
    std::chrono::system_clock::time_point start = std::chrono::system_clock::now();

    int64 out = TSnap::CountTriangles(Graph);
    //double out = TSnap::GetClustCf<PNGraph>(Graph, -1);
    durationPR = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now() - start);
    cout << "Runtime algos: " << durationPR.count() << " ms. out=" << out << endl;
}
