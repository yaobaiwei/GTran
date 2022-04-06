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

#include <pthread.h>

class SimpleSpinLockGuard {
 public:
    explicit SimpleSpinLockGuard(pthread_spinlock_t* lock) {
        lock_ = lock;
        if (lock_ != nullptr)
            pthread_spin_lock(lock);
    }

    ~SimpleSpinLockGuard() {
        Unlock();
    }

    void Unlock() {
        if (!unlocked_) {
            unlocked_ = true;
            if (lock_ != nullptr)
                pthread_spin_unlock(lock_);
        }
    }

 private:
    pthread_spinlock_t* lock_ = nullptr;
    bool unlocked_ = false;
};
