/*
 * test_stxxl.cpp
 *
 *  Created on: Jan 15, 2014
 *      Author: jacopo
 */

#include <iostream>
#include <cstdlib>
#include <stxxl/vector>
#include <stxxl/sort>

typedef struct Triple {
	long t1, t2, t3;

	Triple(long s, long p, long o) {
		this->t1 = s;
		this->t2 = p;
		this->t3 = o;
	}

	Triple() {
		t1 = t2 = t3 = 0;
	}
} Triple;

const Triple minv(std::numeric_limits<long>::min(),
		std::numeric_limits<long>::min(), std::numeric_limits<long>::min());

const Triple maxv(std::numeric_limits<long>::max(),
		std::numeric_limits<long>::max(), std::numeric_limits<long>::max());

struct cmp: std::less<Triple> {
	bool operator ()(const Triple& a, const Triple& b) const {
		if (a.t1 < b.t1) {
			return true;
		} else if (a.t1 == b.t1) {
			if (a.t2 < b.t2) {
				return true;
			} else if (a.t2 == b.t2) {
				return a.t3 < b.t3;
			}
		}
		return false;
	}

	Triple min_value() const {
		return minv;
	}

	Triple max_value() const {
		return maxv;
	}
};

typedef stxxl::VECTOR_GENERATOR<Triple>::result vector_type;

int main(int argc, const char** argv) {
	vector_type vector;
	vector_type::bufwriter_type writer(vector);
	for (int i = 0; i < 1000000000; ++i) {
		if (i % 10000000 == 0)
			std::cout << "Inserting element " << i << std::endl;
		Triple t;
		t.t1 = rand();
		t.t2 = rand();
		t.t3 = rand();
		writer << t;
	}
	writer.finish();

	//Sort the vector
	stxxl::sort(vector.begin(), vector.end(), cmp(), 1024*1024*1024);

	std::cout << vector.size() << std::endl;
}

