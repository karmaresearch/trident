#include <tridentcompr/utils/utils.h>
#include <cstdio>
#include <boost/chrono.hpp>
#include <stdlib.h>

#include <iostream>
#include <random>
#include <cmath>

int main(int argc, const char** args) {

    long n = 100000000;
    uint64_t *numbers = new uint64_t[n];
    //Fill the numbers
    std::random_device rd;
    std::mt19937 e2(rd());
    std::uniform_int_distribution<long long int> dist(std::llround(std::pow(2, 0)), std::llround(std::pow(2, 48)));
    for (long i = 0; i < n; ++i) {
        numbers[i] = dist(e2);
    }
    cout << "Finished generating the numbers" << endl;

    char test[9];

    boost::chrono::system_clock::time_point start =
        boost::chrono::system_clock::now();
    for (long i = 0; i < n; ++i) {
        uint64_t key = numbers[i];
        if (i % 10000000 == 0) {
            cout << i << endl;
        }

        Utils::encode_long(test, 0, key);
        uint64_t j = Utils::decode_long(test, 0);
        if (j != key) {
            cout << "Wrong j=" << j << " key=" << key << endl;
            break;
        }
    }
    boost::chrono::duration<double> sec = boost::chrono::system_clock::now()
                                          - start;
    cout << "Duration encode long " << sec.count() * 1000 << " ms" << endl;

    int offset = 0;
    char test1[9];
    start = boost::chrono::system_clock::now();
    for (long i = 0; i < n; ++i) {
        uint64_t key = numbers[i];
        if (i % 10000000 == 0) {
            cout << i << endl;
        }

        offset = 0;
        Utils::encode_vlong2(test1, offset, key);
        offset = 0;
        uint64_t j = Utils::decode_vlong2(test1, &offset);
        if (j != key) {
            cout << "i=" << i << " Wrong j=" << j << " key=" << key << endl;
            throw 10;
        }
    }
    sec = boost::chrono::system_clock::now() - start;
    cout << "Duration encode vlong2 " << sec.count() * 1000 << " ms" << endl;

    offset = 0;
    start = boost::chrono::system_clock::now();
    for (long i = 0; i < n; ++i) {
        uint64_t key = numbers[i];
        if (i % 10000000 == 0) {
            cout << i << endl;
        }

        offset = 0;
        Utils::encode_vlong(test1, offset, key);
        offset = 0;
        uint64_t j = Utils::decode_vlong(test1, &offset);
        if (j != key) {
            cout << "i=" << i << " Wrong j=" << j << " key=" << key << endl;
            throw 10;
        }
    }
    sec = boost::chrono::system_clock::now() - start;
    cout << "Duration encode vlong " << sec.count() * 1000 << " ms" << endl;


    delete[] numbers;
}
