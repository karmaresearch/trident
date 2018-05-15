#include <iostream>
#include <chrono>
#include <cstdlib>
#include "../src/kb/kb.h"
#include "../src/kb/inserter.h"
#include "../src/kb/querier.h"
#include "../src/utils/propertymap.h"
#include <sstream>
#include <fstream>

using namespace std;
namespace timens = std::chrono;

class MyTreeInserter: public TreeInserter {
private:
	Inserter *ins;
	TermCoordinates c;
public:
	MyTreeInserter(Inserter *ins) {
		this->ins = ins;
	}

	void addEntry(nTerm key, int64_t nElements, short file, int pos,
			char strategy) {
		c.set(0, file, pos, nElements, strategy);
		ins->insert(key, &c);
	}
};

int main(int argc, const char** argv) {

	int n = 9000000 * 3;
	int firstGroupTerms = 900000;
	int secondGroupTerms = 900000;

	int64_t firstKey;
	int64_t secondKey;
	int64_t *array1 = new int64_t[n];
	for (int64_t i = 0; i < n; ++i) {
		if (i % firstGroupTerms == 0)
			firstKey = i;
		if (i % secondGroupTerms == 0)
			secondKey = i;

		if (i % 3 == 0) {
			array1[i] = firstKey;
		} else if (i % 3 == 1) {
			array1[i] = secondKey;
		} else {
			array1[i] = i;
		}
	}

	cout << "Finished filling the array" << endl;

	KBConfig c;
	c.setParamInt(STORAGE_MAX_FILE_SIZE, 4 * 1024 * 1024);
	std::string inPath("/Users/jacopo/Desktop/kb");
	KB *kb = new KB(inPath, false, c);
	Inserter *ins = kb->insert();

	//Populate the tree
	std::chrono::system_clock::time_point start =
			std::chrono::system_clock::now();
	cout << "Start inserting" << endl;
	MyTreeInserter tins(ins);
	for (int i = 0; i < n; i += 3) {
		if (i % 1000000 == 0) {
			cout << "Inserting n1 " << i << endl;
		}
		ins->insert(0, array1[i], array1[i + 1], array1[i + 2], NULL, tins);
	}

	ins->flush(0, NULL, tins);
	std::chrono::duration<double> sec = std::chrono::system_clock::now()
			- start;
	cout << "Duration insertion " << sec.count() / 1000 << endl;

//	delete[] array1;
	delete ins;
	delete kb;

	kb = new KB(inPath, true, c);
	Querier *q = kb->query();

	//* TEST QUERIES *//
	for (int i = 0; i < n; i += firstGroupTerms) {
		PairItr *itr = q->get(0, i, -1, -1);
		if (itr != NULL) {
			int64_t count = 0;
			int64_t secondTerm = i;
			while (itr->has_next()) {
				itr->next_pair();
				if (count % secondGroupTerms == 0) {
					secondTerm = i + count;
				}
				if (itr->getValue1() != secondTerm
						|| itr->getValue2() != i + count + 2) {
					cerr << "Problem1" << endl;
					break;
				}
				count += 3;
			}

			if (count != firstGroupTerms) {
				cerr << "Problem2" << endl;
				break;
			}
			q->releaseItr(itr);
		} else {
			cerr << "Not found " << i << endl;
			break;
		}
	}

	cout << "Finished N1 with x -1 -1" << endl;

	int offset = 0;
	for (int i = 0; i < n; i += firstGroupTerms) {
		int64_t secondTerm = i + offset;
		PairItr *itr = q->get(0, i, secondTerm, -1);
		if (itr != NULL) {
			int64_t count = 0;
			while (itr->has_next()) {
				itr->next_pair();
				if (itr->getValue1() != secondTerm) {
					cerr << "Problem1" << endl;
					break;
				}
				if (itr->getValue2() != secondTerm + count + 2) {
					cerr << "Problem3" << endl;
					break;
				}
				count += 3;
			}

			if (count != secondGroupTerms) {
				cerr << "Problem2" << endl;
				break;
			}
			q->releaseItr(itr);

			offset = (offset + secondGroupTerms) % firstGroupTerms;
		} else {
			cerr << "Not found " << i << endl;
			break;
		}
	}

	cout << "Finished N1 with x y -1" << endl;

	offset = 0;
	int offset2 = 0;
	for (int i = 0; i < n; i += firstGroupTerms) {
		int64_t secondTerm = i + offset;
		int64_t thirdTerm = secondTerm + offset2 * 3 + 2;
		PairItr *itr = q->get(0, i, secondTerm, thirdTerm);
		if (itr != NULL) {
			int64_t count = 0;
			while (itr->has_next()) {
				itr->next_pair();
				if (itr->getValue1() != secondTerm) {
					cerr << "Problem1" << endl;
					break;
				}
				if (itr->getValue2() != thirdTerm) {
					cerr << "Problem3" << endl;
					break;
				}
				count += 3;
			}

			if (count != 3) {
				cerr << "Problem2" << endl;
				break;
			}
			q->releaseItr(itr);

			offset = (offset + secondGroupTerms) % firstGroupTerms;
			offset2 = (offset + 1) % secondGroupTerms;
		} else {
			cerr << "Not found " << i << endl;
			break;
		}
	}

	cout << "Finished N1 with x y z" << endl;

	//Delete everything
	cout << "Delete everything" << endl;
	delete q;
	delete kb;
	return 0;
}
