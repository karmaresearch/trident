#ifndef _PARALLEL_H
#define _PARALLEL_H

class ParallelTasks {
    public:
        template<typename It, typename Cmp>
            static void sort(It begin, It end, const Cmp &cmp) {
            }
};

template<typename El>
class ConcurrentBoundedQueue {
    public:
        void push(El el) {
        }

        void pop(El &el) {
        }
};
#endif
