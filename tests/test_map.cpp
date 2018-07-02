#include <iostream>
#include <list>
#include <chrono>
#include <sparsehash/dense_hash_map>

struct eqint {
	bool operator()(int i1, int i2) const {
		return i1 == i2;
	}
};

typedef std::list<int> mylist;
typedef google::dense_hash_map<int, mylist::iterator, std::hash<int> , eqint> map;

using namespace std;
namespace timens = std::chrono;

int main(int argc, const char** args) {
	//Testing
	mylist l;
	map m;
	m.set_empty_key(-1);
	cout << "Insert keys into the map" << endl;
	timens::system_clock::time_point start = timens::system_clock::now();
	for (int i = 0; i < 10; ++i) {
		unsigned int key = random() % 10;
		m.insert(make_pair(key, l.insert(l.end(), i)));
	}
	std::chrono::duration<double> sec = std::chrono::system_clock::now()
			- start;
	cout << "Time insert " << m.size() << ": " << (sec.count() * 1000) << " ms"
			<< endl;

	cout << "List: ";
	for (mylist::iterator itr = l.begin(); itr != l.end(); ++itr) {
		cout << *itr << " ";
	}
	cout << endl;

	start = timens::system_clock::now();
	map::iterator itr;
	for (int i = 0; i < 10; ++i) {
		unsigned int key = random() % 10;
		cout << "Query " << key << endl;

		itr = m.find(key);
		if (itr != m.end()) {
			cout << "Splice" << *(itr->second) << endl;
			l.splice(l.end(), l, itr->second);
		} else {
			cout << "Not found" << endl;
		}

		cout << "List: ";
		for (mylist::iterator itr = l.begin(); itr != l.end(); ++itr) {
			cout << *itr << " ";
		}
		cout << endl;
	}
	sec = std::chrono::system_clock::now() - start;
	cout << "Time querying: " << (sec.count() * 1000) << " ms " << endl;
}
