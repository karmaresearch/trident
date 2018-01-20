#ifndef _PARALLEL_H
#define _PARALLEL_H

#include <queue>
#include <mutex>
#include <future>
#include <algorithm>
#include <assert.h>

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
            static void sort_int(It begin, It end, long nthreads = -1) {
                if (nthreads == -1) {
                    if (ParallelTasks::nthreads != -1) {
                        //Limit the number of threads to the one specified by the user
                        nthreads = ParallelTasks::nthreads;
                    } else {
                        nthreads = std::max((unsigned int)1, std::thread::hardware_concurrency() / 2);
                    }
                }
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
                    size_t grainsize,
                    Container c,
                    long nthreads = -1) {
                if (nthreads == -1) {
                    if (ParallelTasks::nthreads != -1) {
                        //Limit the number of threads to the one specified by the user
                        nthreads = ParallelTasks::nthreads;
                    } else {
                        nthreads = std::max((unsigned int) 1,
                                std::thread::hardware_concurrency() / 2);
                    }
                }
                assert(nthreads >= 1);
                const size_t delta = std::max(grainsize, (size_t) (end - begin) / nthreads);
                std::vector<std::thread> threads;
                size_t currentbegin = 0;
                size_t currentend = 0;
                for (size_t i = 0; i < nthreads && currentend < end; ++i) {
                    currentbegin = begin + i * delta;
                    currentend = currentbegin + delta;
                    if (currentend > end)
                        currentend = end;
                    ParallelRange range(currentbegin, currentend);
                    threads.push_back(std::thread(&Container::operator(), c, range));
                }
                for(auto &t : threads) {
                    t.join();
                }
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
