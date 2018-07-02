/*
 * test_joins.cpp
 *
 *  Created on: May 16, 2014
 *      Author: jacopo
 */

#include <iostream>
#include <string>
#include <fstream>

#include "../src/kb/kb.h"
#include "../src/main/loader.h"
#include "../src/kb/memoryopt.h"
#include "../src/sparql/plan.h"
#include "../src/sparql/executor.h"
#include "../src/sparql/query.h"
#include "../src/kb/kbconfig.h"

using namespace std;

void loadDB(string file, string kbDir) {
	Loader loader;
	loader.load(file, kbDir, 1, 1);
}

int checkResults(string file, std::vector<string> results) {
	//Read the file
	std::ifstream infile(file);
	std::string line;
	while (std::getline(infile, line)) {
		bool found = false;
		for (vector<string>::iterator it = results.begin(); it != results.end();
             ++it) {
			if (it->compare(line) == 0) {
				found = true;
				results.erase(it);
				break;
			}
		}
		if (!found) {
			cerr << "*** the tuple " << line << " was not found " << endl;
			return 1;
		}
	}
	infile.close();
	return results.size() != 0;
}

std::vector<string> createTwoJoinsInput(string filePath, int size1, int size2, int pos1, int pos2, int pos3, int pos4) {
    ofstream file;
	file.open(filePath);
	int64_t counter = 0;
	string triple[3];
    vector<string> results;
    
	for (int i = 0; i < size1; ++i) {
        triple[pos1] = "<join" + to_string(i) + ">";
        triple[pos2] = "<join" + to_string(i + 1) + ">";
        
		for (int j = 0; j < 3; ++j) {
			if (j != pos1 && j != pos2) {
				triple[j] = "<" + to_string(counter++) + ">";
			}
		}
		file << triple[0] << " " << triple[1] << " " << triple[2] << " ."
        << endl;
        
	}
	file.flush();
    
    for (int i = 0; i < size2; ++i) {
        triple[pos3] = "<join" + to_string(i) + ">";
        triple[pos4] = "<join" + to_string(i + 1) + ">";
		for (int j = 0; j < 3; ++j) {
			if (j != pos3 && j != pos4) {
				triple[j] = "<" + to_string(counter++) + ">";
			}
		}
		file << triple[0] << " " << triple[1] << " " << triple[2] << " ."
        << endl;
        
		//Add the results in a container
		if (i < size1) {
            string t1 = "<join" + to_string(i) + "> ";
            string t2 = "<join" + to_string(i + 1) + "> ";
            string t3 = "<" + to_string(i) + "> ";
            string t4 = "<" + to_string(counter - 1) + "> ";
            string line = t1 + t2 + t3 + t4;
            results.push_back(line);
        }
	}
	file.close();
    return results;
}

std::vector<string> createOneJoinInput(string filePath, int pos1, int pos2,
                                       int size1, int size2, int posConstA1, int posConstA2, int posConstB1,
                                       int posConstB2) {
	std::vector<string> results;
    
	ofstream file;
	file.open(filePath);
	int64_t counter = 0;
	string triple[3];
    
	if (posConstA1 != -1) {
		triple[posConstA1] = "<http://constA>";
	}
	if (posConstA2 != -1) {
		triple[posConstA2] = "<http://constB>";
	}
    
	int joinId = 0;
	for (int i = 0; i < size1; ++i) {
		if (posConstA1 != -1 && posConstA2 != -1)
			triple[pos1] = "<join" + to_string(joinId++) + ">";
		else
			triple[pos1] = "<join>";
        
		for (int j = 0; j < 3; ++j) {
			if (j != pos1 && j != posConstA1 && j != posConstA2) {
				triple[j] = "<" + to_string(counter++) + ">";
			}
		}
		file << triple[0] << " " << triple[1] << " " << triple[2] << " ."
        << endl;
        
	}
	file.flush();
    
	if (posConstB1 != -1) {
		triple[posConstB1] = "<http://constC>";
	}
	if (posConstB2 != -1) {
		triple[posConstB2] = "<http://constD>";
	}
	joinId = 0;
	for (int i = 0; i < size2; ++i) {
		if (posConstB1 != -1 && posConstB2 != -1)
			triple[pos2] = "<join" + to_string(joinId++) + ">";
		else {
			triple[pos2] = "<join>";
		}
		for (int j = 0; j < 3; ++j) {
			if (j != pos2 && j != posConstB1 && j != posConstB2) {
				triple[j] = "<" + to_string(counter++) + ">";
			}
		}
		file << triple[0] << " " << triple[1] << " " << triple[2] << " ."
        << endl;
        
		//Add the results in a container
		if (i < size1 || (posConstA1 == -1 || posConstA2 == -1)) {
			int64_t c = 0;
			for (int m = 0; m < size1; ++m) {
                if (m == 0 || posConstB1 == -1 || posConstB2 == -1) {
                    string t1 =
                    (posConstA1 != -1) ? "" : "<" + to_string(c++) + "> ";
                    string t2 =
                    (posConstA2 != -1) ? "" : "<" + to_string(c++) + "> ";
                    int64_t counterT3 = (posConstB2 == -1) ? counter - 2 : counter - 1;
                    string t3 =
                    (posConstB1 != -1) ?
                    "" : "<" + to_string(counterT3) + "> ";
                    string t4 =
                    (posConstB2 != -1) ?
                    "" : "<" + to_string(counter - 1) + "> ";
                    
                    string line;
                    
                    if (posConstA1 == -1 || posConstA2 == -1) {
                        line = "<join> " + t1 + t2 + t3 + t4;
                    } else {
                        line = "<join" + to_string(i) + "> " + t1 + t2 + t3 + t4;
                    }
                    results.push_back(line);
                }
            }
        }
	}
	file.close();
    
	return results;
}

void testTwoJoinsCreateQuery(int pos1, int pos2, int pos3, int pos4, string queryFile) {
    string projections = "?x ?y ?v ?z ";
    
	string pattern1[3];
	pattern1[pos1] = "?x";
    pattern1[pos2] = "?y";
	for (int i = 0; i < 3; ++i) {
		if (i != pos1 && i != pos2) {
            pattern1[i] = "?v";
		}
	}
    
	string pattern2[3];
	pattern2[pos3] = "?x";
	pattern2[pos4] = "?y";
	for (int i = 0; i < 3; ++i) {
		if (i != pos3 && i != pos4) {
            pattern2[i] = "?z";
		}
	}
    
	ofstream file;
	file.open(queryFile);
	file << "SELECT " << projections << "WHERE {" << endl;
	file << pattern1[0] << " " << pattern1[1] << " " << pattern1[2] << " ."
    << endl;
	file << pattern2[0] << " " << pattern2[1] << " " << pattern2[2] << " ."
    << endl;
	file << "}" << endl;
	file.close();
}

int testTwoJoinsQueryAndTest(int pos1, int pos2, int pos3, int pos4, std::vector<string> results,
                             string kbDir, string queryFile) {
	KBConfig kbc;
	MemoryOptimizer::optimizeForReading(kbc);
	KB kb(kbDir.c_str(), true, kbc);
    
	Querier *q = kb.query();
    
	//Parse the query
	Query query;
	testTwoJoinsCreateQuery(pos1, pos2, pos3, pos4, queryFile);
	query.parseQuery(kb.getDictMgmt(), queryFile);
    
	//Prepare the query plan
	Plan plan;
	plan.prepare(q, &query);
    
	//I don't need the dictionary anymore. I can free up some memory...
	kb.closeDict();
    
	//Execute the query
	Executor exec(q);
	std::ofstream out(queryFile + string(".results"));
	std::streambuf *coutbuf = std::cout.rdbuf();
	std::cout.rdbuf(out.rdbuf());
	int64_t nElements = exec.executePlan(&plan, true, true);
	std::cout.flush();
	std::cout.rdbuf(coutbuf);
	out.close();
    
	if (nElements != results.size()) {
		cerr << "Elements are not the same " << nElements << " "
        << results.size() << endl;
		return 1;
	}
    
	if (checkResults(queryFile + string(".results"), results)) {
		return 1;
	}
    
	cout << "*** OK" << endl;
    
	Utils::remove(queryFile);
	Utils::remove(queryFile + string(".results"));
	delete q;
	return 0;
}

void testOneJoinCreateQuery(int pos1, int pos2, int posConstA1, int posConstA2,
                            int posConstB1, int posConstB2, string queryFile) {
	string projections = "?x ";
    
	string pattern1[3];
	pattern1[pos1] = "?x";
	int countVars = 0;
	for (int i = 0; i < 3; ++i) {
		if (i != pos1) {
			if (i == posConstA1) {
				pattern1[i] = "<http://constA>";
			} else if (i == posConstA2) {
				pattern1[i] = "<http://constB>";
			} else {
				pattern1[i] = "?V" + to_string(countVars++);
				projections += pattern1[i] + " ";
			}
		}
	}
    
	string pattern2[3];
	pattern2[pos2] = "?x";
	for (int i = 0; i < 3; ++i) {
		if (i != pos2) {
			if (i == posConstB1) {
				pattern2[i] = "<http://constC>";
			} else if (i == posConstB2) {
				pattern2[i] = "<http://constD>";
			} else {
				pattern2[i] = "?V" + to_string(countVars++);
				projections += pattern2[i] + " ";
			}
		}
	}
    
	ofstream file;
	file.open(queryFile);
	file << "SELECT " << projections << "WHERE {" << endl;
	file << pattern1[0] << " " << pattern1[1] << " " << pattern1[2] << " ."
    << endl;
	file << pattern2[0] << " " << pattern2[1] << " " << pattern2[2] << " ."
    << endl;
	file << "}" << endl;
	file.close();
}

int testOneJoinQueryAndTest(int pos1, int pos2, int posConstA1, int posConstA2,
                            int posConstB1, int posConstB2, std::vector<string> results,
                            string kbDir, string queryFile) {
	KBConfig kbc;
	MemoryOptimizer::optimizeForReading(kbc);
	KB kb(kbDir.c_str(), true, kbc);
    
	Querier *q = kb.query();
    
	//Parse the query
	Query query;
	testOneJoinCreateQuery(pos1, pos2, posConstA1, posConstA2, posConstB1,
                           posConstB2, queryFile);
	query.parseQuery(kb.getDictMgmt(), queryFile);
    
	//Prepare the query plan
	Plan plan;
	plan.prepare(q, &query);
    
	//I don't need the dictionary anymore. I can free up some memory...
	kb.closeDict();
    
	//Execute the query
	Executor exec(q);
	std::ofstream out(queryFile + string(".results"));
	std::streambuf *coutbuf = std::cout.rdbuf();
	std::cout.rdbuf(out.rdbuf());
	int64_t nElements = exec.executePlan(&plan, true, true);
	std::cout.flush();
	std::cout.rdbuf(coutbuf);
	out.close();
    
	if (nElements != results.size()) {
		cerr << "Elements are not the same " << nElements << " "
        << results.size() << endl;
		return 1;
	}
    
	if (checkResults(queryFile + string(".results"), results)) {
		return 1;
	}
    
	cout << "*** OK" << endl;
    
	Utils::remove(queryFile);
	Utils::remove(queryFile + string(".results"));
	delete q;
	return 0;
}

int testOneJoin(string dir, int pos1, int pos2, int size1, int size2,
                int posConstA1, int posConstA2, int posConstB1, int posConstB2) {
	//Generate the input
	string filePath = dir + string("/inputOneJoin-") + to_string(pos1)
    + string("-") + to_string(pos2) + string(".nt");
	string kbDir = dir + string("/kb");
	Utils::create_directories(kbDir);
    
	std::vector<string> results = createOneJoinInput(filePath, pos1, pos2,
                                                     size1, size2, posConstA1, posConstA2, posConstB1, posConstB2);
    
	//Load the input in the database
	Logger::setMinLevel(WARNINGL);
	loadDB(filePath, kbDir);
    
	//Query it and check the results
	string queryFile = dir + string("/query");
	if (testOneJoinQueryAndTest(pos1, pos2, posConstA1, posConstA2, posConstB1,
                                posConstB2, results, kbDir, queryFile)) {
		return 1;
	}
    
	//Delete everything
	Utils::remove(filePath);
	Utils::remove_all(kbDir);
	return 0;
}

int testOneJoin(string dir, int size1, int size2) {
	//Two constants
	cout << "* Testing with two constants" << endl;
	for (int pos1 = 0; pos1 < 3; ++pos1) {
		for (int pos2 = 0; pos2 < 3; ++pos2) {
			if (pos1 != pos2) {
				if (testOneJoin(dir, pos1, pos2, size1, size2, (pos1 + 1) % 3,
                                (pos1 + 2) % 3, (pos2 + 1) % 3, (pos2 + 2) % 3)) {
					return 1;
				}
			}
		}
	}
    
	//One constant
    cout << "* Testing with one constant" << endl;
    for (int pos1 = 0; pos1 < 3; ++pos1) {
        for (int pos2 = 0; pos2 < 3; ++pos2) {
            if (pos1 != pos2) {
				for (int posConst1 = 0; posConst1 < 3; ++posConst1) {
					if (posConst1 != pos1) {
						for (int posConst2 = 0; posConst2 < 3; ++posConst2) {
							if (posConst2 != pos2) {
								if (testOneJoin(dir, pos1, pos2, size1, size2,
                                                posConst1, -1, posConst2, -1)) {
									return 1;
								}
							}
						}
					}
				}
			}
		}
	}
	
    //No constants
	cout << "* Testing with no constants" << endl;
	for (int pos1 = 0; pos1 < 3; ++pos1) {
		for (int pos2 = 0; pos2 < 3; ++pos2) {
			if (pos1 != pos2)
				if (testOneJoin(dir, pos1, pos2, size1, size2, -1, -1, -1,
                                -1)) {
					return 1;
				}
		}
	}
	return 0;
}

int testOneJoin(string dir) {
	//Test different sizes
	int sizes[] = { 10, 100, 1 };
	for (int i = 0; i < 3; ++i) {
		for (int j = 0; j < 3; ++j) {
			if (i != j) {
				cout << "** Testing with size " << sizes[i] << " and "
                << sizes[j] << endl;
				if (testOneJoin(dir, sizes[i], sizes[j])) {
					return 1;
				}
			}
		}
	}
	return 0;
}

int testTwoJoins(string dir, int size1, int size2, int pos1, int pos2, int pos3, int pos4) {
    //Generate the input
	string filePath = dir + string("/inputTwoJoins-") + to_string(pos1)
    + string("-") + to_string(pos2) + string(".nt");
	string kbDir = dir + string("/kb");
	Utils::create_directories(kbDir);
    
	std::vector<string> results = createTwoJoinsInput(filePath, size1, size2, pos1, pos2, pos3, pos4);
    
	//Load the input in the database
	Logger::setMinLevel(WARNINGL);
	loadDB(filePath, kbDir);
    
	//Query it and check the results
	string queryFile = dir + string("/query");
	if (testTwoJoinsQueryAndTest(pos1, pos2, pos3, pos4, results, kbDir, queryFile)) {
		return 1;
	}
    
	//Delete everything
	Utils::remove(filePath);
	Utils::remove_all(kbDir);
	return 0;
}

int testTwoJoins(string dir, int size1, int size2) {
    //No constants
	cout << "* Testing with no constants" << endl;
	for (int pos1 = 0; pos1 < 3; ++pos1) {
		for (int pos2 = 0; pos2 < 3; ++pos2) {
			if (pos1 != pos2) {
                for(int pos3 = 0; pos3 < 3; ++pos3) {
                    if (pos3 != pos1 && pos3 != pos2) {
                        for(int pos4 = 0; pos4 < 3; ++pos4) {
                            if (pos3 != pos4) {
                                if (testTwoJoins(dir, size1, size2, pos1, pos2, pos3, pos4)) {
                                    return 1;
                                }
                            }
                        }
                    }
                }
            }
		}
	}
	return 0;
}

int testTwoJoins(string dir) {
    //Test different sizes
	int sizes[] = { 10, 100, 1 };
	for (int i = 0; i < 3; ++i) {
		for (int j = 0; j < 3; ++j) {
            cout << "** Testing with size " << sizes[i] << " and "
            << sizes[j] << endl;
            if (testTwoJoins(dir, sizes[i], sizes[j])) {
                return 1;
            }
		}
	}
	return 0;
}

int main(int argc, const char** args) {
	string tmpDir = string(args[1]);
    
    cout << "Testing joins on two elements ..." << endl;
	if (testTwoJoins(tmpDir)) {
		cerr << "Error" << endl;
		return 1;
	}
    
	cout << "Testing joins on one element ..." << endl;
	if (testOneJoin(tmpDir)) {
		cerr << "Error" << endl;
		return 1;
	}
    
	cout << "ALL OK" << endl;
    
	return 0;
}
