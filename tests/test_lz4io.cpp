#include "../src/utils/lz4io.h"
#include "../src/compression/compressor.h"

#include <iostream>
#include <cstdlib>
#include <sstream>
#include <fstream>

using namespace std;

int main(int argc, const char** argv) {

//	//Insert into a compressed file
//	int64_t startingValue = 0;
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
