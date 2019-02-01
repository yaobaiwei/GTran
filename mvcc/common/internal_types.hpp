/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Hongzhi Chen (hzchen@cse.cuhk.edu.hk)
*/


#ifndef INTERNAL_TYPES_HPP_
#define INTERNAL_TYPES_HPP_

#include <stdint.h>

//===--------------------------------------------------------------------===//
// Type definitions.
//===--------------------------------------------------------------------===//

typedef size_t hash_t;

typedef uint32_t oid_t;

static const oid_t START_OID = 0;

static const oid_t INVALID_OID = std::numeric_limits<oid_t>::max();

static const oid_t MAX_OID = std::numeric_limits<oid_t>::max() - 1;

#define NULL_OID MAX_OID

// For transaction id

typedef uint64_t txn_id_t;

static const txn_id_t INVALID_TXN_ID = 0;

static const txn_id_t INITIAL_TXN_ID = 1;

static const txn_id_t MAX_TXN_ID = std::numeric_limits<txn_id_t>::max();

// For commit id

typedef uint64_t cid_t;

static const cid_t INVALID_CID = 0;

static const cid_t MAX_CID = std::numeric_limits<cid_t>::max();

// For epoch id

typedef uint64_t eid_t;

static const cid_t INVALID_EID = 0;

static const cid_t MAX_EID = std::numeric_limits<eid_t>::max();




#endif /* INTERNAL_TYPES_HPP_ */
