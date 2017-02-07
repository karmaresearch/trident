#include "../src/utils/lz4io.h"
#include "../src/compression/compressor.h"

#include <boost/chrono.hpp>

#include <iostream>
#include <cstdlib>
#include <sstream>
#include <fstream>

using namespace std;
namespace timens = boost::chrono;

int main(int argc, const char** argv) {

//	//Insert into a compressed file
//	long startingValue = 0;
//	cout << "Starting from " << startingValue << endl;
//	{
//		LZ4Writer writer("/Users/jacopo/Desktop/file");
//		for (int i = 0; i < 1000000; ++i) {
//			writer.writeVLong(i);
//		}
//	}

	{
		LZ4Reader reader(argv[1]);
		while (!reader.isEof()) {
			Term t;
			t.readFrom(&reader);
		}
	}
	cout << "Finished";
	return 0;
}
