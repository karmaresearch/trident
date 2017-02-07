#include <kognac/lz4io.h>
#include <kognac/filemerger.h>
#include <kognac/utils.h>
#include <kognac/multidisklz4reader.h>
#include <kognac/multidisklz4writer.h>
#include <kognac/triple.h>

#include <trident/kb/dictmgmt.h>
#include <trident/kb/kbconfig.h>
#include <trident/kb/inserter.h>
#include <trident/kb/memoryopt.h>
#include <trident/loader.h>

#include <boost/chrono.hpp>
#include <boost/log/trivial.hpp>

#include <iostream>
#include <cstdlib>
#include <sstream>
#include <fstream>
#include <vector>
#include <string>

using namespace std;
namespace timens = boost::chrono;

int main(int argc, const char** argv) {
    string inputP2 = argv[1];
    string inputP5 = argv[2];

    Loader::sortPermutation<L_Triple>(inputP2, 8, 72, true, 110000000000l, 30000000000l, 16, false);
    Loader::sortPermutation<L_Triple>(inputP5, 8, 72, true, 110000000000l, 30000000000l, 16, false);
    //Loader::sortPermutation(inputP4, 8, 72, true, 110000000000l, 30000000000l, 16);
}
