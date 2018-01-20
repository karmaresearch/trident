#ifndef _PARALLEL_H
#define _PARALLEL_H

#include <queue>
#include <mutex>
#include <future>
#include <algorithm>

class ParallelRange {
    private:
        size_t b, e;

    public:
        ParallelRange(size_t b, size_t e) : b(b), e(e) {}

        size_t begin() const {
            return b;
        }

        size_t end() const {
            return e;
        }
};

class ParallelTasks {
    private:
        static long nthreads;

    public:
        static void setNThreads(uint32_t nthreads) {
            ParallelTasks::nthreads = nthreads;
        }

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

        template<typename It>
            static void sort_int(It begin, It end, uint32_t nthreads = std::thread::hardware_concurrency() / 2) {
                auto len = std::distance(begin, end);
                if (len <= 1024 || nthreads < 2) {
                    std::sort(begin, end);
                } else {
                    It mid = std::next(begin, len / 2);
                    if (nthreads > 1) {
                        auto fn = std::async(ParallelTasks::sort_int<It>, begin,
                                mid, nthreads - 2);
                        sort_int<It>(mid, end, nthreads - 2);
                        fn.wait();
                    } else {
                        sort_int<It>(begin, mid, 0);
                        sort_int<It>(mid, end, 0);
                    }
                    std::inplace_merge(begin, mid, end);
                }
            }


        template<typename Container>
            static void parallel_for(size_t begin,
                    size_t end,
                    size_t incr,
                    Container c) {
                //TODO
            }
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
