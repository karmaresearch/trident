#ifndef _PARALLEL_H
#define _PARALLEL_H

#include <queue>
#include <mutex>

class ParallelTasks {
    public:
        template<typename It, typename Cmp>
            static void sort(It begin, It end, const Cmp &cmp) {
                //TODO
                std::sort(begin, end, cmp);
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
