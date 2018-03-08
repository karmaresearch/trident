/*
 * main.cpp
 *
 *  Created on: Oct 1, 2013
 *      Author: jacopo
 */
#include <iostream>
#include <boost/chrono.hpp>
#include <cstdlib>
#include "../src/sparql/plan.h"
#include "../src/sparql/executor.h"
#include "../src/kb/kb.h"
#include <sstream>
#include <fstream>

using namespace std;
namespace timens = boost::chrono;

int test1(int argc, const char** argv) {

	KB *kb;
	Querier *q;
	Executor *exec;

	kb = new KB(argv[1], 4);
	q = kb->query();
	exec = new Executor(q);

	//Read the compiled query from a file
	std::ifstream is(argv[2]);
	is.seekg(0, std::ios_base::end);
	std::size_t size = is.tellg();
	is.seekg(0, std::ios_base::beg);
	char raw_input[size];
	is.read(raw_input, size);
	is.close();
	Plan plan;
	plan.unserialize(raw_input);

	timens::system_clock::time_point start = timens::system_clock::now();
	exec->executeJoin(&plan, true, true);
	boost::chrono::duration<double> sec = boost::chrono::system_clock::now()
			- start;
	cout << "Cold runtime = " << sec.count() * 1000 << " milliseconds\n";

	int times = 5;
	start = timens::system_clock::now();
	for (int i = 0; i < times; ++i) {
		exec->executeJoin(&plan, false, true);
	}
	sec = boost::chrono::system_clock::now() - start;
	std::cout << "Warm runtime = " << (sec.count() / times) * 1000
			<< " milliseconds\n";

	delete exec;
	delete kb;

	return 0;

//	TwoTermStorage* s = new TwoTermStorage(argv[1]);
//
//	storagestrat strat;
//	pairitr itr;
//
//	itr.init(s, 0, 1121, strat.pair_handler(121), 0, 9787);
//	while (itr.has_next()) {
//		itr.next_pair();
//		int64_t v1 = itr.getValue1();
//		int64_t v2 = itr.getValue2();
//		printf("Pair %ld %ld\n", v1, v2);
//	}

//	boost::chrono::system_clock::time_point start =
//			boost::chrono::system_clock::now();
//	for (int i = 0; i < 100; ++i) {
//		itr.init(s, 0, 135, handler, 26620, -1);
//		while (itr.has_next()) {
//			itr.next_pair();
//		}
//	}
//
//	boost::chrono::duration<double> sec = boost::chrono::system_clock::now()
//			- start;
//	std::cout << "took " << sec.count() << " seconds\n";
//	delete s;
}

