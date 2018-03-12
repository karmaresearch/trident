#include <iostream>
#include <boost/chrono.hpp>
#include <cstdlib>
#include "../src/kb/kb.h"
#include "../src/kb/inserter.h"
#include "../src/kb/querier.h"
#include "../src/utils/propertymap.h"
#include <sstream>
#include <fstream>

using namespace std;
namespace timens = boost::chrono;

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

	int n1 = 10000000 * 3;
	int64_t *array1 = new int64_t[n1];
	for (int64_t i = 0; i < n1; ++i) {
		array1[i] = i;
	}

	int64_t firstKey = n1;
	int64_t *array2 = new int64_t[n1];
	for (int64_t i = 0; i < n1; ++i) {
		if (i % 300 == 0)
			firstKey = i + n1;
		if (i % 3 == 0)
			array2[i] = firstKey;
		else
			array2[i] = i + n1;
	}

	int64_t startN3 = 2 * n1;
	firstKey = startN3;
	int64_t *array3 = new int64_t[n1];
	for (int64_t i = 0; i < n1; ++i) {
		if (i % 30000 == 0)
			firstKey = i + startN3;
		if (i % 3 == 0)
			array3[i] = firstKey;
		else
			array3[i] = i + startN3;
	}
	cout << "Finished filling the array" << endl;

	KBConfig c;
	c.setParamInt(STORAGE_MAX_FILE_SIZE, 16 * 1024 * 1024);
	std::string inPath("/Users/jacopo/Desktop/kb");
	KB *kb = new KB(inPath, false, c);
	Inserter *ins = kb->insert();

	//Populate the tree
	boost::chrono::system_clock::time_point start =
			boost::chrono::system_clock::now();
	cout << "Start inserting" << endl;
	MyTreeInserter tins(ins);
	for (int i = 0; i < n1; i += 3) {
		if (i % 1000000 == 0) {
			cout << "Inserting n1 " << i << endl;
		}
		ins->insert(0, array1[i], array1[i + 1], array1[i + 2], NULL, tins);
	}
	for (int i = 0; i < n1; i += 3) {
		if (i % 1000000 == 0) {
			cout << "Inserting n2 " << i << endl;
		}
		ins->insert(0, array2[i], array2[i + 1], array2[i + 2], NULL, tins);
	}
	for (int i = 0; i < n1; i += 3) {
		if (i % 1000000 == 0) {
			cout << "Inserting n3 " << i << endl;
		}
		ins->insert(0, array3[i], array3[i + 1], array3[i + 2], NULL, tins);
	}

	ins->flush(0, NULL, tins);
	boost::chrono::duration<double> sec = boost::chrono::system_clock::now()
			- start;
	cout << "Duration insertion " << sec.count() / 1000 << endl;

	delete[] array1;
	delete[] array2;
	delete[] array3;
	delete ins;
	delete kb;

	kb = new KB(inPath, true, c);
	Querier *q = kb->query();

	//* TEST QUERIES *//
	for (int i = 0; i < n1; i += 3) {
		PairItr *itr = q->get(0, i, -1, -1);
		if (itr != NULL) {
			if (itr->has_next()) {
				itr->next_pair();
				if (itr->getValue1() != i + 1 || itr->getValue2() != i + 2) {
					cerr << "Problem1" << endl;
					;
					break;
				}
			} else {
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

	for (int i = 0; i < n1; i += 3) {
		PairItr *itr = q->get(0, i, i + 1, -1);
		if (itr != NULL) {
			if (itr->has_next()) {
				itr->next_pair();
				if (itr->getValue1() != i + 1 || itr->getValue2() != i + 2) {
					cerr << "Problem1" << endl;
					;
					break;
				}
			} else {
				cerr << "Problem2" << endl;
				break;
			}
			q->releaseItr(itr);
		} else {
			cerr << "Not found " << i << endl;
			break;
		}
	}

	cout << "Finished N1 with x y -1" << endl;

	for (int i = 0; i < n1; i += 3) {
		PairItr *itr = q->get(0, i, i + 1, i + 2);
		if (itr != NULL) {
			if (itr->has_next()) {
				itr->next_pair();
				if (itr->getValue1() != i + 1 || itr->getValue2() != i + 2) {
					cerr << "Problem1" << endl;
					;
					break;
				}
			} else {
				cerr << "Problem2" << endl;
				break;
			}
			q->releaseItr(itr);
		} else {
			cerr << "Not found " << i << endl;
			break;
		}
	}

	cout << "Finished N1 with x y z" << endl;

	for (int i = 0; i < n1; i += 300) {
		PairItr *itr = q->get(0, n1 + i, -1, -1);
		if (itr != NULL) {
			int count = 0;
			int countTriples = 0;
			while (itr->has_next()) {
				itr->next_pair();
				if (itr->getValue1() != i + n1 + count + 1
						|| itr->getValue2() != i + n1 + count + 2) {
					cerr << "Problem " << itr->getValue1() << " "
							<< itr->getValue2() << endl;
					break;
				}
				count += 3;
				countTriples++;
			}

			if (countTriples != 100) {
				cerr << "Problem2" << endl;
				break;
			}

			q->releaseItr(itr);
		} else {
			cout << "Not found " << i << endl;
			break;
		}
	}

	cout << "Finished N2 with x -1 -1" << endl;

	int offset = 0;
	for (int i = 0; i < n1; i += 300) {
		PairItr *itr = q->get(0, n1 + i, n1 + offset + i + 1, -1);
		if (itr != NULL) {
			int count = 0;
			while (itr->has_next()) {
				itr->next_pair();
				if (itr->getValue1() != i + n1 + count + offset + 1
						|| itr->getValue2() != i + n1 + count + offset + 2) {
					cerr << "Problem " << itr->getValue1() << " "
							<< itr->getValue2() << endl;
					break;
				}
				count += 3;
			}

			if (count != 3) {
				cerr << "Problem2" << endl;
				break;
			}

			q->releaseItr(itr);
		} else {
			cout << "Not found " << i << endl;
			break;
		}
		offset = (offset + 3) % 300;
	}

	cout << "Finished N2 with x y -1" << endl;

	offset = 0;
	for (int i = 0; i < n1; i += 300) {
		PairItr *itr = q->get(0, n1 + i, n1 + offset + i + 1,
				n1 + offset + i + 2);
		if (itr != NULL) {
			int count = 0;
			while (itr->has_next()) {
				itr->next_pair();
				if (itr->getValue1() != i + n1 + count + offset + 1
						|| itr->getValue2() != i + n1 + count + offset + 2) {
					cerr << "Problem " << itr->getValue1() << " "
							<< itr->getValue2() << endl;
					break;
				}
				count += 3;
			}

			if (count != 3) {
				cerr << "Problem2" << endl;
				break;
			}

			q->releaseItr(itr);
		} else {
			cout << "Not found " << i << endl;
			break;
		}
		offset = (offset + 3) % 300;
	}

	cout << "Finished N2 with x y z" << endl;

	for (int i = 0; i < n1; i += 30000) {
		PairItr *itr = q->get(0, startN3 + i, -1, -1);
		if (itr != NULL) {
			int count = 0;
			while (itr->has_next()) {
				itr->next_pair();
				if (itr->getValue1() != i + startN3 + count + 1
						|| itr->getValue2() != i + startN3 + count + 2) {
					cerr << "Problem " << itr->getValue1() << " "
							<< itr->getValue2() << endl;
					break;
				}
				count += 3;
			}

			if (count != 30000) {
				cerr << "Problem2" << endl;
				break;
			}

			q->releaseItr(itr);
		} else {
			cout << "Not found " << i << endl;
			break;
		}
	}

	cout << "Finished N3 with x -1 -1" << endl;

	offset = 0;
		for (int i = 0; i < n1; i += 30000) {
			PairItr *itr = q->get(0, startN3 + i, startN3 + offset + i + 1,
					-1);
			if (itr != NULL) {
				int count = 0;
				while (itr->has_next()) {
					itr->next_pair();
					if (itr->getValue1() != i + startN3 + count + offset + 1
							|| itr->getValue2() != i + startN3 + count + offset + 2) {
						cerr << "Problem " << itr->getValue1() << " "
								<< itr->getValue2() << endl;
						break;
					}
					count += 3;
				}

				if (count != 3) {
					cerr << "Problem2" << endl;
					break;
				}

				q->releaseItr(itr);
			} else {
				cout << "Not found " << i << endl;
				break;
			}
			offset = (offset + 3) % 30000;
		}

		cout << "Finished N3 with x y -1" << endl;

	offset = 0;
	for (int i = 0; i < n1; i += 30000) {
		PairItr *itr = q->get(0, startN3 + i, startN3 + offset + i + 1,
				startN3 + offset + i + 2);
		if (itr != NULL) {
			int count = 0;
			while (itr->has_next()) {
				itr->next_pair();
				if (itr->getValue1() != i + startN3 + count + offset + 1
						|| itr->getValue2() != i + startN3 + count + offset + 2) {
					cerr << "Problem " << itr->getValue1() << " "
							<< itr->getValue2() << endl;
					break;
				}
				count += 3;
			}

			if (count != 3) {
				cerr << "Problem2" << endl;
				break;
			}

			q->releaseItr(itr);
		} else {
			cout << "Not found " << i << endl;
			break;
		}
		offset = (offset + 3) % 30000;
	}

	cout << "Finished N3 with x y z" << endl;

	//Delete everything
	cout << "Delete everything" << endl;
	delete q;
	delete kb;
	return 0;
}
