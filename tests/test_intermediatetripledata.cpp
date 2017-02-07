#include "../src/kb/kbconfig.h"
#include "../src/kb/kb.h"
#include "../src/kb/memoryopt.h"
#include "../src/kb/inserter.h"

#include "../src/main/loader.h"

#include "../src/sorting/sorter.h"
#include "../src/sorting/filemerger.h"

#include "../src/utils/lz4io.h"
#include "../src/utils/utils.h"

#include <boost/chrono.hpp>

#include <iostream>
#include <cstdlib>
#include <sstream>
#include <fstream>
#include <vector>
#include <string>

using namespace std;
namespace timens = boost::chrono;

int main(int argc, const char** argv) {
	LZ4Reader *reader = new LZ4Reader(argv[1]);
	long count = 0;

	int idx = atoi(argv[2]);

	while (!reader->isEof()) {
		long comb = reader->parseLong();
		long tripleId = comb >> 2;
		long pos = comb & 3;
		long term = reader->parseLong();

		if (tripleId % 16 != idx || tripleId < 0 || tripleId > 20000000000l
				|| pos < 0 || pos > 2 || term < 0 || term > 5000000000l) {
			cout << "Found problem with " << tripleId << " " << term << endl;
			exit(1);
		}

		if (tripleId == 0 && pos == 0) {
			cout << "Stop. I found it" << endl;
			exit(0);
		}

		if (count % 1000000 == 0) {
			cout << "Processed " << count << " records. Last term is "
					<< tripleId << " " << pos << " " << term << endl;
		}
		count++;
	}

	cout << "Terminated. Processed " << count << " records" << endl;

//	while (!reader->isEof()) {
//		int flag = reader->parseByte();
//		if (flag == 1) {
//			long term = reader->parseLong();
//		} else if (flag == 0) {
//			int sizeTerm;
//			const char *term = reader->parseString(sizeTerm);
//			cout << "Term not recognized " << string(term + 2, sizeTerm - 2)
//					<< " triple " << (count / 3) << " pos " << (count % 3)
//					<< endl;
//			exit(1);
//		} else {
//			cout << "Flag " << flag << endl;
//			exit(1);
//		}
//		count++;
//	}
//
//	cout << "Processed " << count << " records" << endl;

	delete reader;

}
