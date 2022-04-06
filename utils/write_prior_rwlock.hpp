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

#include <assert.h>
#include <memory.h>
#include <pthread.h>
#include <stdint.h>

#if defined(__INTEL_COMPILER)
#include <malloc.h>
#else
#include <mm_malloc.h>
#endif  // defined(__GNUC__)

#include <atomic>
#include <cstdio>
#include <string>

class WritePriorRWLock {
 private:
    pthread_rwlock_t lock_;

 public:
    WritePriorRWLock();
    ~WritePriorRWLock();

    void GetReadLock();
    void GetWriteLock();

    void ReleaseLock();
};

class ReaderLockGuard {
 public:
    explicit ReaderLockGuard(WritePriorRWLock& lock);
    ~ReaderLockGuard();
    bool unlocked_ = false;

    void Unlock();

 private:
    WritePriorRWLock* lock_;
};

class WriterLockGuard {
 public:
    explicit WriterLockGuard(WritePriorRWLock& lock);
    ~WriterLockGuard();
    bool unlocked_ = false;

    void Unlock();

 private:
    WritePriorRWLock* lock_;
};

class RWLockGuard {
 public:
    explicit RWLockGuard(WritePriorRWLock& lock, bool is_writer);
    ~RWLockGuard();
    bool unlocked_ = false;

    void Unlock();

 private:
    WritePriorRWLock* lock_;
};
