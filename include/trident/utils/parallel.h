#ifndef _PARALLEL_H
#define _PARALLEL_H

#include <trident/kb/consts.h>
#include <kognac/logs.h>

#include <queue>
#include <mutex>
#include <future>
#include <algorithm>
#include <assert.h>
#include <condition_variable>

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
        LIBEXP static int32_t nthreads;

        template<typename It, typename Cmp>
            static void inplace_merge(It begin,
                    It mid,
                    It end,
                    const Cmp &cmp) {
                It a = begin;
                while (std::distance(a, mid)) {
                    bool res = cmp(*a, *mid);
                    if (!res) {
                        std::iter_swap(a, mid);
                        //Keep swapping
                        It c = mid;
                        It d = std::next(mid, 1);
                        while (std::distance(d, end) && !cmp(*c, *d)) {
                            std::iter_swap(c, d);
                            std::advance(c, 1);
                            std::advance(d, 1);
                        }
                    }
                    std::advance(a, 1);
                }
            }

        template<typename It, typename Cmp>
            static void merge(It begin,
                    It mid,
                    It end,
                    const Cmp &cmp) {
                std::vector<typename std::iterator_traits<It>::value_type> support;
                support.resize(std::distance(begin, end));
                size_t idxSupport = 0;
                It a = begin;
                It b = mid;
                while (true) {
                    auto dist1 = std::distance(a, mid);
                    auto dist2 = std::distance(b, end);
                    if (!dist1 && !dist2)
                        break;
                    bool res = dist1 &&
                        (!dist2 || cmp(*a, *b));
                    if (!res) {
                        support[idxSupport] = *b;
                        std::advance(b, 1);
                    } else {
                        support[idxSupport] = *a;
                        std::advance(a, 1);
                    }
                    idxSupport++;
                }
                //Copy back ...
                std::copy(support.begin(), support.end(), begin);
            }


    public:
        static void setNThreads(int32_t nthreads) {
            ParallelTasks::nthreads = nthreads;
        }

        //Procedure inspired by https://stackoverflow.com/questions/24130307/performance-problems-in-parallel-mergesort-c
        template<typename It, typename Cmp>
            static void sort_int(It begin, It end, const Cmp &cmp, int32_t nthreads) {
                auto len = std::distance(begin, end);
                if (len <= 1024 || nthreads < 2) {
                    std::sort(begin, end, cmp);
                } else {
                    It mid = std::next(begin, len / 2);
                    auto fn = std::async(ParallelTasks::sort_int<It, Cmp>, begin, mid, std::ref(cmp), nthreads / 2);
                    sort_int<It,Cmp>(mid, end, cmp, nthreads / 2);
                    fn.wait();
                    // ParallelTasks::merge(begin, mid, end, cmp);
                    std::inplace_merge(begin, mid, end, cmp);
                }
            }

        template<typename It>
            static void sort_int(It begin, It end, int32_t nthreads = -1) {
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
                    auto fn = std::async(ParallelTasks::sort_int<It>, begin,
                            mid, nthreads / 2);
                    sort_int<It>(mid, end, nthreads / 2);
                    fn.wait();
                    std::inplace_merge(begin, mid, end);
                }
            }

        template<typename Container>
            static void parallel_for(size_t begin,
                    size_t end,
                    size_t grainsize,
                    Container c,
                    int32_t nthreads = -1) {
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
                const size_t delta = std::max(grainsize, (size_t) (end - begin + nthreads - 1) / nthreads);
                std::vector<std::future<void>> threads;
                size_t currentbegin = 0;
                size_t currentend = 0;
                for (size_t i = 0; i < nthreads && currentend < end; ++i) {
                    currentbegin = currentend;
                    currentend = currentbegin + delta;
                    if (currentend >= end) {
                        // Leave the last chunk for "our" thread.
                        break;
                    }
                    ParallelRange range(currentbegin, currentend);
                    threads.push_back(std::async(&Container::operator(), c, range));
                }
                ParallelRange range(currentbegin, end);
                c(range);
                for(auto &t : threads) {
                    t.wait();
                }
            }
};

template<typename El>
class ConcurrentQueue {
    private:
        std::queue<El> q;
        std::mutex mtx;
        std::condition_variable cv;

    public:
        void push(El el) {
            std::unique_lock<std::mutex> lock(mtx);
            q.push(el);
            cv.notify_one();
        }

        void pop(El &el) {
            std::unique_lock<std::mutex> lock(mtx);
            el = q.front();
            q.pop();
        }

        bool isEmpty() {
            std::unique_lock<std::mutex> lock(mtx);
            return q.empty();
        }

        void pop_wait(El &el) {
            std::unique_lock<std::mutex> lock(mtx);
            while (q.empty()) {
                cv.wait(lock);
            }
            el = q.front();
            q.pop();
        }
};
#endif
