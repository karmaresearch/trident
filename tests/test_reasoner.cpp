#include "../src/reasoner/concepts.h"
#include "../src/kb/kb.h"
#include "../src/kb/memoryopt.h"
#include <iostream>
#include <string>

using namespace std;
int main(int argc, char **argv) {

    string kbDir(argv[1]);
	KBConfig kbc;
	MemoryOptimizer::optimizeForReading(1,kbc);
	KB kb(kbDir.c_str(), true, kbc);

    reasoner::Program program(&kb);
    program.readFromFile(std::string(argv[2]));
    return EXIT_SUCCESS;
}
