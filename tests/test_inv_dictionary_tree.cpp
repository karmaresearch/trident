#include <iostream>
#include <boost/chrono.hpp>
#include <cstdlib>
#include "../src/tree/root.h"
#include <sstream>
#include <fstream>

using namespace std;
namespace timens = boost::chrono;

int main(int argc, const char** argv) {

	Root *root = new Root(argv[1], 16 * 1024 * 1024, 10, false, true);

	TreeItr *itr = root->itr();
	char text_term[1024];
	Value value;

	while (itr->hasNext()) {
		long key = itr->next(&value);
		value.text(text_term);
		cout << text_term << " " << key << endl;
	}

	return 0;
}
