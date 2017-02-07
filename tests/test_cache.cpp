#include <boost/chrono.hpp>

#include <sstream>
#include <fstream>
#include <iostream>
#include <vector>
#include <cstdlib>

namespace timens = boost::chrono;
using namespace std;

int main(int argc, const char** argv) {

    long fakeSum = 0;
    long found = 0;
    int i = 16;
    for (; i < 2048; i *= 2) {
        std::vector<long> vector1;
        //std::vector<int> vector2;


        //Fill the table
        for (int j = 0; j < i; ++j) {
            vector1.push_back(rand());
            //vector2.push_back(rand());
        }

        //Sort the array
        std::sort(vector1.begin(), vector1.end());

        //measure the time for some random lookups

        const long* values = &vector1[0];
        //const int* values2 = &vector2[0];
        timens::system_clock::time_point start = timens::system_clock::now();
        for (int lookups = 0; lookups < 100000; ++lookups) {
            const int idx = rand() % i;
            fakeSum += values[idx];
            //fakeSum += values2[idx];
        }
        boost::chrono::duration<double> sec = boost::chrono::system_clock::now() - start;

        std::vector<size_t> idsToSearch;
        for (int j = 0; j < 1000; ++j) {
            idsToSearch.push_back(rand() % i);
        }

        //Search linearly
        start = timens::system_clock::now();
        for (std::vector<size_t>::const_iterator itr = idsToSearch.begin();
                itr != idsToSearch.end(); ++itr) {
            std::vector<long>::const_iterator itrP = vector1.begin();
            while (itrP != vector1.end()) {
                if (*itrP >= vector1[*itr]) {
                    break;
                }
                itrP++;
            }
            found += (itrP != vector1.end() && *itrP == vector1[*itr]) ? 1 : 0;
        }
        boost::chrono::duration<double> linearSec = boost::chrono::system_clock::now() - start;

        //Search with binary search
        start = timens::system_clock::now();
        for (std::vector<size_t>::const_iterator itr = idsToSearch.begin();
                itr != idsToSearch.end(); ++itr) {
            found += (std::binary_search(vector1.begin(), vector1.end(), vector1[*itr])) ? 1 : 0;
        }
        boost::chrono::duration<double> binarySec = boost::chrono::system_clock::now() - start;

        cout << i << "\t" << sec.count() * 1000 << "\t" << linearSec.count() * 1000 << "\t" << binarySec.count() * 1000 << endl;
    }
    cout << "Fake sum=" << fakeSum << " " << found << endl;


    return 0;
}
