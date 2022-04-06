// Copyright 2020 BigGraph Team @ Husky Data Lab, CUHK
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <condition_variable>
#include <mutex>
#include <queue>
#include <utility>

#include "base/abstract_thread_safe_queue.hpp"

template <typename T>
class ThreadSafeQueue : public AbstractThreadSafeQueue<T> {
 public:
    ThreadSafeQueue() = default;
    ~ThreadSafeQueue() = default;
    ThreadSafeQueue(const ThreadSafeQueue &) = delete;
    ThreadSafeQueue &operator=(const ThreadSafeQueue &) = delete;
    ThreadSafeQueue(ThreadSafeQueue &&) = delete;
    ThreadSafeQueue &operator=(ThreadSafeQueue &&) = delete;

    void Push(T elem) override {
        mu_.lock();
        queue_.push(std::move(elem));
        mu_.unlock();
        cond_.notify_all();
    }

    void WaitAndPop(T & elem) override {
        std::unique_lock<std::mutex> lk(mu_);
        cond_.wait(lk, [this] { return !queue_.empty(); });
        elem = std::move(queue_.front());
        queue_.pop();
    }

    int Size() override {
        std::lock_guard<std::mutex> lk(mu_);
        return queue_.size();
    }

 private:
    std::mutex mu_;
    std::queue<T> queue_;
    std::condition_variable cond_;
};
