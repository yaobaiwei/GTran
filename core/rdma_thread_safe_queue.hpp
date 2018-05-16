#pragma once

#include "base/abstract_thread_safe_queue.hpp"
#include "core/ring_buffer.hpp"

template <typename T>
class RdmaThreadSafeQueue : public AbstractThreadSafeQueue<T> {
public:
    RdmaThreadSafeQueue();

    void Push(T elem) override {

    }

    void WaitAndPop(T* elem) override {

    }

    int Size() override {

    }

private:
    // for thread safe
    std::mutex mu_;
    std::condition_variable cond_;
    
    // memory related
    char* buffer_;
    int size;
};
