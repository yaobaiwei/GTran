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

#include "tbb/concurrent_unordered_map.h"
#include "write_prior_rwlock.hpp"

// ConcurrentUnorderedMap is an extension of tbb::concurrent_unordered_map.
// It is safe to traverse the map while inserting elements to it, thanks to the feature of tbb::concurrent_unordered_map.
// The feature of easy-to-use built-in rwlock with accessor and const_accessor in tbb::concurrent_hash_map are adopted.

template <typename Key, typename T, typename Hasher = tbb::tbb_hash<Key>, typename Key_equality = std::equal_to<Key>,
         typename Allocator = tbb::tbb_allocator<std::pair<const Key, T> > >
class ConcurrentUnorderedMap :
    public tbb::concurrent_unordered_map<Key, T, Hasher, Key_equality, Allocator> {
 public:
    struct accessor;
    struct const_accessor;

    typedef tbb::concurrent_unordered_map<Key, T, Hasher, Key_equality, Allocator> BaseType;
    typedef ConcurrentUnorderedMap<Key, T, Hasher, Key_equality, Allocator> MapType;
    typedef typename BaseType::reference reference;
    typedef typename BaseType::const_reference const_reference;
    typedef typename BaseType::pointer pointer;
    typedef typename BaseType::const_pointer const_pointer;
    typedef typename BaseType::iterator iterator;
    typedef typename BaseType::const_iterator const_iterator;

    explicit ConcurrentUnorderedMap(size_t lock_virtualization_count = 10240) {
        // With Hasher hasher_, a item with a specific key will use locks_[hasher_(key) % lock_virtualization_count_]
        lock_virtualization_count_ = lock_virtualization_count;
        locks_ = new WritePriorRWLock[lock_virtualization_count_];
    }

    ~ConcurrentUnorderedMap() {
        delete[] locks_;
    }

    // "Insert" and "Find" interfaces are like "insert" and "find" in concurrent_hash_map
    bool Insert(accessor& ac, const Key& key) {
        return ac.Insert(*this, key);
    }

    bool Find(accessor& ac, const Key& key) {
        return ac.Find(*this, key);
    }

    bool Find(const_accessor& ac, const Key& key) {
        return ac.Find(*this, key);
    }

    // can be used as concurrent_hash_map::accessor
    struct accessor {
        accessor() {}

        ~accessor() {
            // If the iterator is valid, release the lock acquired before getting the iterator.
            if (valid) {
                lock_ptr_->ReleaseWriteLock();
            }
        }

        reference operator*() const {
            return *it;
        }

        pointer operator->() const {
            return it.operator->();
        }

     private:
        bool valid = false;
        iterator it;
        WritePriorRWLock* lock_ptr_ = nullptr;

        bool Find(MapType& map, const Key& key) {
            assert(!valid);
            size_t lock_pos = map.hasher_(key) % map.lock_virtualization_count_;
            // Use the technique of lock-virtualization.
            // Similarly hereinafter.
            lock_ptr_ = &map.locks_[lock_pos];
            lock_ptr_->GetWriteLock();

            it = map.find(key);
            if (it != map.end()) {
                valid = true;
                // The I
            } else {
                // The iterator is not valid. Release the lock.
                // Similarly hereinafter.
                lock_ptr_->ReleaseWriteLock();
            }
            return valid;
        }

        bool Insert(MapType& map, const Key& key) {
            assert(!valid);
            size_t lock_pos = map.hasher_(key) % map.lock_virtualization_count_;
            lock_ptr_ = &map.locks_[lock_pos];
            lock_ptr_->GetWriteLock();

            std::pair<Key, T> p;
            p.first = key;

            auto insert_ret = map.insert(p);  // pair<iterator it_to_inserted_item, bool insert_success>
            it = move(insert_ret.first);

            if (insert_ret.second) {
                valid = true;
            } else {
                valid = false;
                lock_ptr_->ReleaseWriteLock();
            }

            return valid;
        }

        friend class ConcurrentUnorderedMap;
    };

    // can be used as concurrent_hash_map::const_accessor
    struct const_accessor {
        const_accessor() {}

        ~const_accessor() {
            if (valid) {
                lock_ptr_->ReleaseReadLock();
            }
        }

        const_reference operator*() const {
            return *it;
        }

        const_pointer operator->() const {
            return it.operator->();
        }

     private:
        bool valid = false;
        const_iterator it;
        WritePriorRWLock* lock_ptr_ = nullptr;

        bool Find(const MapType& map, const Key& key) {
            assert(!valid);
            size_t lock_pos = map.hasher_(key) % map.lock_virtualization_count_;
            lock_ptr_ = &map.locks_[lock_pos];
            lock_ptr_->GetReadLock();

            it = map.find(key);
            if (it != map.end()) {
                valid = true;
            } else {
                lock_ptr_->ReleaseReadLock();
            }
            return valid;
        }

        friend class ConcurrentUnorderedMap;
    };

 private:
    mutable WritePriorRWLock* locks_;
    int lock_virtualization_count_;
    Hasher hasher_;
};
