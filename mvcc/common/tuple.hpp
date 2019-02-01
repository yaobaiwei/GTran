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

#ifndef TUPLE_HPP_
#define TUPLE_HPP_

#include <atomic>
#include <cstring>

#include "item_pointer.hpp"

class SpinLatch;

struct TupleHeader {
  SpinLatch latch;
  std::atomic<txn_id_t> txn_id;
  cid_t read_ts;
  cid_t begin_ts;
  cid_t end_ts;
  ItemPointer next;
  ItemPointer prev;
  ItemPointer *indirection;
} __attribute__((aligned(64)));



#endif /* TUPLE_HPP_ */
