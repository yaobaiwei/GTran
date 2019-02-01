/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Hongzhi Chen (hzchen@cse.cuhk.edu.hk)
*/

//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// tile_group_header.h
//
// Identification: src/include/storage/tile_group_header.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#ifndef SPIN_LATCH_HPP_
#define SPIN_LATCH_HPP_


#include <atomic>


//===--------------------------------------------------------------------===//
// Cheap & Easy Spin Latch
//===--------------------------------------------------------------------===//

enum class LatchState : bool { Unlocked = 0, Locked };

class SpinLatch {
 public:
  SpinLatch() : state_(LatchState::Unlocked) {}

  void Lock() {
    while (!TryLock()) {
      _mm_pause();  // helps the cpu to detect busy-wait loop
    }
  }

  bool IsLocked() { return state_.load() == LatchState::Locked; }

  bool TryLock() {
    // exchange returns the value before locking, thus we need
    // to make sure the lock wasn't already in Locked state before
    return state_.exchange(LatchState::Locked, std::memory_order_acquire) !=
           LatchState::Locked;
  }

  void Unlock() {
    state_.store(LatchState::Unlocked, std::memory_order_release);
  }

 private:
  /*the exchange method on this atomic is compiled to a lockfree xchgl
   * instruction*/
  std::atomic<LatchState> state_;
};

#endif /* SPIN_LATCH_HPP_ */
