#include <iostream>
#include <list>
#include <sparsehash/dense_hash_map>
#include <string>
#include <chrono>

#include <kognac/lz4io.h>
#include <kognac/stringscol.h>
#include <kognac/hashmap.h>

using namespace std;
namespace timens = std::chrono;

int main(int argc, const char** args) {

    StringCollection poolForMap(64*1024*1024);
    int ndicts = stoi(args[1]);
    CompressedByteArrayToNumberMap uncommonMap;
	uncommonMap.resize(500000000);
    const char *prefixDict = args[2];
    timens::system_clock::time_point start = timens::system_clock::now();
    timens::system_clock::time_point prevstart = start;
    cout << "Load factor map " << uncommonMap.load_factor() << " max load factor " << uncommonMap.max_load_factor() << " max n. buckets " << uncommonMap.max_bucket_count() << endl;

    
    for (int i = 0; i < ndicts; ++i) {
        LZ4Reader *dictFile = new LZ4Reader(string(prefixDict) + to_string(i)+string("-np1"));
        cout << "Loading file " << string(prefixDict) + to_string(i) << endl;
        if (dictFile != NULL) {
            int64_t countEntries = 0;
            while (!dictFile->isEof()) {
                int64_t compressedTerm = dictFile->parseLong();
                int sizeTerm;
                const char *term = dictFile->parseString(sizeTerm);
                /*if (uncommonMap.find(term) == uncommonMap.end()) {*/
                    const char *newTerm = poolForMap.addNew((char*) term,
                                          sizeTerm);
                    /*uncommonMap.insert(
                        std::make_pair(newTerm, compressedTerm));
                } else {
                    LOG(ERRORL) << "This should not happen! Term " << term
                                             << " was already being inserted";
                }*/
                countEntries++;
                if (countEntries % 1000000 == 0) {
		cout << "Pool size " << poolForMap.allocatedBytes() << endl;
std::chrono::duration<double> sec = std::chrono::system_clock::now() - prevstart;
std::chrono::duration<double> totalsec = std::chrono::system_clock::now() - start;
prevstart = std::chrono::system_clock::now();
                    cout << "Loaded " << countEntries << " total time " << totalsec.count()  << " time from prev. " << sec.count() << " Load factor map " << uncommonMap.load_factor() << " n. buckets " << uncommonMap.bucket_count() << endl;
                }
            }
        }
    }
    cout << "Finished!" << endl;
}
