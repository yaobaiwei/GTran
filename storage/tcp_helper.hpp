#pragma once

#include <mutex>
#include <string>
#include <stdlib.h>

#include "glog/logging.h"

#include "storage/vkvstore.hpp"
#include "storage/ekvstore.hpp"
#include "base/type.hpp"

using __gnu_cxx::hash_map;
using __gnu_cxx::hash_set;

class TCPHelper {
public:
	TCPHelper(VKVStore * vp_store, EKVStore * ep_store, bool use_rdma);

	~TCPHelper();

	bool GetPropertyForVertex(uint64_t vp_id_v, value_t & val);
	bool GetPropertyForEdge(uint64_t ep_id_v, value_t & val);

private:

    VKVStore * vpstore_;
    EKVStore * epstore_;

	bool use_rdma_;
};
