#include "../src/utils/lz4io.h"

#include <iostream>
#include <cstdlib>
#include <sstream>
#include <fstream>
#include <vector>

#include "../src/sorting/filemerger.h"
#include "../src/compression/compressor.h"

using namespace std;

int main(int argc, const char** argv) {

	vector<string> files;
	for(int i = 1; i < argc; ++i) {
		files.push_back(string(argv[i]));
		cerr << "Adding file " << argv[i] << endl;
	}

	FileMerger<Term> merger(files);

	int64_t count = 0;
	int64_t duplicates = 0;
	char *previousTerm = new char[1024];
check_again:
	while(!merger.isEmpty()) {
		Term t = merger.get();
		
		if (t.equals(previousTerm)) {
			duplicates++;
			goto check_again;
		}		

		memcpy(previousTerm, t.term,t.size);
		cout << "Process term " << string(t.term + 2, t.size -2) << endl;
		count++;
	}

	cerr << "Elements processed " << count << " " << duplicates << endl;

}
