#include "../src/kb/kbconfig.h"
#include "../src/kb/kb.h"
#include "../src/kb/memoryopt.h"
#include "../src/kb/inserter.h"

#include "../src/main/loader.h"

#include "../src/sorting/sorter.h"
#include "../src/sorting/filemerger.h"

#include "../src/compression/compressor.h"

#include "../src/utils/lz4io.h"
#include "../src/utils/utils.h"

#include "../src/utils/stringscol.h"

#include <boost/chrono.hpp>
#include <boost/filesystem.hpp>

#include <iostream>
#include <cstdlib>
#include <sstream>
#include <fstream>
#include <vector>
#include <string>

using namespace std;
namespace timens = boost::chrono;
namespace fs = boost::filesystem;

int main(int argc, const char** argv) {

	LZ4Reader *r = new LZ4Reader(argv[1]);

	while (!r->isEof()) {
		int64_t number = r->parseLong();
		int size;
		const char *term = r->parseString(size);
		cout << number << " " << string(term + 2, size -2) << endl;
	}


	delete r;
	BOOST_LOG_TRIVIAL(debug)<< "finished";
}
