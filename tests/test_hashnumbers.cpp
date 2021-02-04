#include <iostream>
#include <chrono>
#include <cstdlib>
#include <sstream>
#include <fstream>
#include <vector>
#include <cstdint>
#include <cstddef>
#include <functional>
#include <memory>

#include <google/dense_hash_set>
#include <vlog/term.h>


using namespace std;
namespace timens = std::chrono;

struct PairTermHash
{
	template <class T1, class T2>
	std::size_t operator() (const std::pair<T1, T2> &pair) const
	{
		return std::hash<T1>{}(pair.first) ^ std::hash<T2>{}(pair.second);
	}
};

struct _PairHash {
    typedef std::pair<uint64_t, uint64_t> argument_type;
    typedef std::size_t result_type;
    result_type operator()(argument_type const& s) const {
        result_type const h1(std::hash<uint64_t>{}(s.first));
        result_type const h2(std::hash<uint64_t>{}(s.second));
        return h1 ^ (h2 << 1);
    }
};

int main(int argc, const char** argv)
{
            auto h = std::hash<std::pair<Term_t, Term_t>>{}(std::make_pair(0, 0));
            std::cout << "*******" << h << "*******";
    int n = 20000;
    google::dense_hash_set<std::pair<unsigned long, unsigned long>, _PairHash> cnt;
    cnt.set_empty_key(std::make_pair(n+1, n+1));

    cout << "Testing speed ..." << endl;
    std::chrono::system_clock::time_point start = std::chrono::system_clock::now();
    int64_t sum = 0;
    for (int i = 0; i < n; ++i) {
        cnt.insert(std::make_pair(i, i));
    }
    std::chrono::duration<double> sec = std::chrono::system_clock::now()
        - start;
    cout << "Time: " << sec.count() * 1000 << "ms. Size=" << cnt.size() << std::endl;
    return 0;
}
