/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Chenghuan Huang (chhuang@cse.cuhk.edu.hk)
*/

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
    void ReleaseReadLock();

    void GetWriteLock();
    void ReleaseWriteLock();
};

class ReaderLockGuard {
 public:
    explicit ReaderLockGuard(WritePriorRWLock& lock);
    ~ReaderLockGuard();

 private:
    WritePriorRWLock* lock_;
};

class WriterLockGuard {
 public:
    explicit WriterLockGuard(WritePriorRWLock& lock);
    ~WriterLockGuard();

 private:
    WritePriorRWLock* lock_;
};
