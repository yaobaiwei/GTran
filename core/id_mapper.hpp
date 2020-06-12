// Copyright 2019 BigGraph Team @ Husky Data Lab, CUHK
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

#ifndef IDMAPPER_HPP_
#define IDMAPPER_HPP_

#include <vector>

#include "core/abstract_id_mapper.hpp"
#include "utils/config.hpp"
#include "utils/unit.hpp"
#include "utils/mymath.hpp"
#include "base/type.hpp"
#include "base/node.hpp"

#include "glog/logging.h"

static uint64_t _VIDFLAG = 0xFFFFFFFFFFFFFFFF >> (64-VID_BITS);
static uint64_t _PIDLFLAG = 0xFFFFFFFFFFFFFFFF >> (64- 2*VID_BITS);

class NaiveIdMapper : public AbstractIdMapper {
 public:
    explicit NaiveIdMapper(Node & node) : my_node_(node) {
        config_ = Config::GetInstance();
    }

    bool IsVertex(uint64_t v_id) {
        bool has_v = v_id & _VIDFLAG;
        return has_v;
    }

    bool IsEdge(uint64_t e_id) {
        bool has_src_v = e_id & _VIDFLAG;
        e_id >>= VID_BITS;
        bool has_dst_v = e_id & _VIDFLAG;
        return has_src_v && has_dst_v;
    }

    bool IsVProperty(uint64_t vp_id) {
        bool has_p = vp_id & _PIDLFLAG;
        vp_id >>= PID_BITS;
        vp_id >>= VID_BITS;
        bool has_v = vp_id & _VIDFLAG;
        return has_p && has_v;
    }

    bool IsEProperty(uint64_t ep_id) {
        bool has_p = ep_id & _PIDLFLAG;
        ep_id >>= PID_BITS;
        bool has_src_v = ep_id & _VIDFLAG;
        ep_id >>= VID_BITS;
        bool has_dst_v = ep_id & _VIDFLAG;
        return has_p && has_src_v && has_dst_v;
    }

    // judge if vertex/edge/property local
    bool IsVertexLocal(const vid_t v_id) {
        return GetMachineIdForVertex(v_id) == my_node_.get_local_rank();
    }

    bool IsEdgeLocal(const eid_t e_id) {
        return GetMachineIdForEdge(e_id) == my_node_.get_local_rank();
    }

    bool IsVPropertyLocal(const vpid_t vp_id) {
        return GetMachineIdForVProperty(vp_id) == my_node_.get_local_rank();
    }

    bool IsEPropertyLocal(const epid_t ep_id) {
        return GetMachineIdForEProperty(ep_id) == my_node_.get_local_rank();
    }

    // vertex/edge/property -> machine index mapping
    int GetMachineIdForVertex(vid_t v_id) {
        return mymath::hash_mod(v_id.hash(), my_node_.get_local_size());
    }

    int GetMachineIdForEdge(eid_t e_id, bool considerDstVtx = false) {
        return mymath::hash_mod(e_id.hash(), my_node_.get_local_size());
    }

// #define BY_EV_ID
#ifdef BY_EV_ID
    int GetMachineIdForVProperty(vpid_t vp_id) {
        vid_t v(vp_id.vid);
        return mymath::hash_mod(v.hash(), my_node_.get_local_size());
    }

    int GetMachineIdForEProperty(epid_t ep_id) {
        eid_t e(ep_id.dst_vid, ep_id.src_vid);
        return mymath::hash_mod(e.hash(), my_node_.get_local_size());
    }
#else
    int GetMachineIdForVProperty(vpid_t vp_id) {
        return mymath::hash_mod(vp_id.hash(), my_node_.get_local_size());
    }

    int GetMachineIdForEProperty(epid_t ep_id) {
        return mymath::hash_mod(ep_id.hash(), my_node_.get_local_size());
    }
#endif

 private:
    Config * config_;
    Node my_node_;
};

class SimpleIdMapper : public AbstractIdMapper {
 public:
    bool IsVertex(uint64_t v_id) {
        bool has_v = v_id & _VIDFLAG;
        return has_v;
    }

    bool IsEdge(uint64_t e_id) {
        bool has_src_v = e_id & _VIDFLAG;
        e_id >>= VID_BITS;
        bool has_dst_v = e_id & _VIDFLAG;
        return has_src_v && has_dst_v;
    }

    bool IsVProperty(uint64_t vp_id) {
        bool has_p = vp_id & _PIDLFLAG;
        vp_id >>= PID_BITS;
        vp_id >>= VID_BITS;
        bool has_v = vp_id & _VIDFLAG;
        return has_p && has_v;
    }

    bool IsEProperty(uint64_t ep_id) {
        bool has_p = ep_id & _PIDLFLAG;
        ep_id >>= PID_BITS;
        bool has_src_v = ep_id & _VIDFLAG;
        ep_id >>= VID_BITS;
        bool has_dst_v = ep_id & _VIDFLAG;
        return has_p && has_src_v && has_dst_v;
    }

    // judge if vertex/edge/property local
    bool IsVertexLocal(const vid_t v_id) {
        return GetMachineIdForVertex(v_id) == my_node_.get_local_rank();
    }

    bool IsEdgeLocal(const eid_t e_id) {
        return GetMachineIdForEdge(e_id) == my_node_.get_local_rank();
    }

    bool IsVPropertyLocal(const vpid_t vp_id) {
        return GetMachineIdForVProperty(vp_id) == my_node_.get_local_rank();
    }

    bool IsEPropertyLocal(const epid_t ep_id) {
        return GetMachineIdForEProperty(ep_id) == my_node_.get_local_rank();
    }

    // vertex/edge/property -> machine index mapping
    int GetMachineIdForVertex(vid_t v_id) {
        return v_id.value() % my_node_.get_local_size();
    }

    int GetMachineIdForEdge(eid_t e_id, bool considerDstVtx = false) {
        if (considerDstVtx) {
            return e_id.dst_v % my_node_.get_local_size();
        }
        return e_id.src_v % my_node_.get_local_size();
    }

    int GetMachineIdForVProperty(vpid_t vp_id) {
        return vp_id.vid % my_node_.get_local_size();
    }

    int GetMachineIdForEProperty(epid_t ep_id) {
        return ep_id.src_vid % my_node_.get_local_size();
    }

    static SimpleIdMapper* GetInstance(Node* node = nullptr) {
        static SimpleIdMapper* p = nullptr;

        // null and var avail
        if (p == nullptr && node != nullptr) {
            p = new SimpleIdMapper(*node);
        }

        return p;
    }

 private:
    Config * config_;
    Node my_node_;

    explicit SimpleIdMapper(Node & node) : my_node_(node) {
        config_ = Config::GetInstance();
    }
};

#endif /* IDMAPPER_HPP_ */
