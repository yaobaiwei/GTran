/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Chenghuan Huang (chhuang@cse.cuhk.edu.hk)
*/

#include "write_prior_rwlock.hpp"

WritePriorRWLock::WritePriorRWLock() {
    pthread_rwlockattr_t attr;
    pthread_rwlockattr_init(&attr);
    pthread_rwlockattr_setkind_np(&attr, PTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP);
    assert(pthread_rwlock_init(&lock_, &attr) == 0);
}

WritePriorRWLock::~WritePriorRWLock() {
    assert(pthread_rwlock_destroy(&lock_) == 0);
}

void WritePriorRWLock::GetReadLock() {
    assert(pthread_rwlock_rdlock(&lock_) == 0);
}

void WritePriorRWLock::GetWriteLock() {
    assert(pthread_rwlock_wrlock(&lock_) == 0);
}

void WritePriorRWLock::ReleaseLock() {
    assert(pthread_rwlock_unlock(&lock_) == 0);
}

ReaderLockGuard::ReaderLockGuard(WritePriorRWLock& lock) {
    lock_ = &lock;
    if (lock_)
        lock_->GetReadLock();
}

ReaderLockGuard::~ReaderLockGuard() {
    Unlock();
}

void ReaderLockGuard::Unlock() {
    if (!unlocked_) {
        unlocked_ = true;
        if (lock_)
            lock_->ReleaseLock();
    }
}

WriterLockGuard::WriterLockGuard(WritePriorRWLock& lock) {
    lock_ = &lock;
    if (lock_)
        lock_->GetWriteLock();
}

WriterLockGuard::~WriterLockGuard() {
    Unlock();
}

void WriterLockGuard::Unlock() {
    if (!unlocked_) {
        unlocked_ = true;
        if (lock_)
            lock_->ReleaseLock();
    }
}

RWLockGuard::RWLockGuard(WritePriorRWLock& lock, bool is_writer) {
    lock_ = &lock;
    if (lock_) {
        if (is_writer)
            lock_->GetWriteLock();
        else
            lock_->GetReadLock();
    }
}

RWLockGuard::~RWLockGuard() {
    Unlock();
}

void RWLockGuard::Unlock() {
    if (!unlocked_) {
        unlocked_ = true;
        if (lock_)
            lock_->ReleaseLock();
    }
}
