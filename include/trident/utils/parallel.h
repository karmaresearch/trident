#ifndef _PARALLEL_H
#define _PARALLEL_H

#include <queue>
#include <mutex>
#include <future>
#include <algorithm>

class ParallelTasks {
    public:
        //Procedure inspired by https://stackoverflow.com/questions/24130307/performance-problems-in-parallel-mergesort-c
        template<typename It, typename Cmp>
            static void sort_int(It begin, It end, const Cmp &cmp, uint32_t nthreads) {
                auto len = std::distance(begin, end);
                if (len <= 1024 || nthreads < 2) {
                    std::sort(begin, end, cmp);
                } else {
                    It mid = std::next(begin, len / 2);
                    if (nthreads > 1) {
                        auto fn = std::async(ParallelTasks::sort_int<It, Cmp>, begin, mid, std::ref(cmp), nthreads - 2);
                        sort_int<It,Cmp>(mid, end, cmp, nthreads - 2);
                        fn.wait();
                    } else {
                        sort_int<It,Cmp>(begin, mid, cmp, 0);
                        sort_int<It,Cmp>(mid, end, cmp, 0);
                    }
                    std::inplace_merge(begin, mid, end, cmp);
                }
            }

        /*template<typename It, typename Cmp>
            static void sort(It begin, It end, const Cmp &cmp, int nthreads = std::thread::hardware_concurrency()/2) {
                sort_int<It,Cmp>(begin, end, cmp, nthreads);
            }*/
};

template<typename El>
class ConcurrentQueue {
    private:
        std::queue<El> q;
        std::mutex mtx;

    public:
        void push(El el) {
            std::lock_guard<std::mutex> lock(mtx);
            q.push(el);
        }

        void pop(El &el) {
            std::lock_guard<std::mutex> lock(mtx);
            el = q.front();
            q.pop();
        }
};
#endif
