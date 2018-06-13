/*
 * config.hpp
 *
 *  Created on: May 9, 2018
 *      Author: Hongzhi Chen
 */
//
#ifndef IDMAPPER_HPP_
#define IDMAPPER_HPP_

#include <vector>
#include <mutex>

#include "core/abstract_id_mapper.hpp"
#include "utils/config.hpp"
#include "utils/unit.hpp"
#include "utils/mymath.hpp"
#include "base/type.hpp"
#include "base/node.hpp"

#include "glog/logging.h"


class NaiveIdMapper : public AbstractIdMapper {
public:
	NaiveIdMapper(Config * config, Node & node) : config_(config), my_node_(node) {}

	virtual void Init(){
		node_offset_.resize(config_->global_num_machines, 0);
	}

	bool IsVertex(uint64_t v_id) {
		bool has_v = v_id & 0x3FFFFFF;
		return has_v;
	}

	bool IsEdge(uint64_t e_id) {
		bool has_out_v = e_id & 0x3FFFFFF;
		e_id >>= VID_BITS;
		bool has_in_v = e_id & 0x3FFFFFF;
		return has_out_v && has_in_v;
	}

	bool IsVProperty(uint64_t vp_id) {
		bool has_p = vp_id & 0xFFF;
		vp_id >>= PID_BITS;
		vp_id >>= VID_BITS;
		bool has_v = vp_id & 0x3FFFFFF;
		return has_p && has_v;
	}

	bool IsEProperty(uint64_t ep_id) {
		bool has_p = ep_id & 0xFFF;
		ep_id >>= PID_BITS;
		bool has_out_v = ep_id & 0x3FFFFFF;
		ep_id >>= VID_BITS;
		bool has_in_v = ep_id & 0x3FFFFFF;
		return has_p && has_out_v && has_in_v;
	}

	// judge if vertex/edge/property local
	bool IsVertexLocal(const vid_t v_id) {
		return GetMachineIdForVertex(v_id) == my_node_.id;
	}

	bool IsEdgeLocal(const eid_t e_id) {
		return GetMachineIdForEdge(e_id) == my_node_.id;
	}
  
	bool IsVPropertyLocal(const vpid_t vp_id) {
		return GetMachineIdForVProperty(vp_id) == my_node_.id;
	}

	bool IsEPropertyLocal(const epid_t ep_id) {
		return GetMachineIdForEProperty(ep_id) == my_node_.id;
	}

	// vertex/edge/property -> machine index mapping
	int GetMachineIdForVertex(vid_t v_id) {
		return mymath::hash_mod(v_id.hash(), get_num_nodes());
//		return v_id.vid % config_->global_num_machines;
	}

	int GetMachineIdForEdge(eid_t e_id) {
		return mymath::hash_mod(e_id.hash(), get_num_nodes());
//		return (e_id.in_v + e_id.out_v) % config_->global_num_machines;
	}

	int GetMachineIdForVProperty(vpid_t vp_id) {
		vid_t v(vp_id.vid);
		return mymath::hash_mod(v.hash(), get_num_nodes());
//		return vp_id.vid % config_->global_num_machines;
	}

	int GetMachineIdForEProperty(epid_t ep_id) {
		eid_t e(ep_id.in_vid, ep_id.out_vid);
		return mymath::hash_mod(e.hash(), get_num_nodes());
//		return (ep_id.in_vid + ep_id.out_vid) % config_->global_num_machines;
	}

	// TODO: every entry of node_offset_ need a mutex, but the mutex need to be wrapped
	// hzchen 2018.5.29
	int GetAndIncrementRdmaRingBufferOffset(const int nid, const int msg_sz) {
		  CHECK_LT(nid, config_->global_num_machines);
		  std::lock_guard<std::mutex> lk(node_offset_mu_);
		  int return_offset = node_offset_[nid];
		  node_offset_[nid] = (node_offset_[nid] + msg_sz) % MiB2B(config_->global_per_recv_buffer_sz_mb);
		  return return_offset;
	}

private:
	Config * config_;
	Node my_node_;

	// Node Id -> RDMA ring buffer offest
	std::mutex node_offset_mu_;
	std::vector<int> node_offset_;
};

#endif /* IDMAPPER_HPP_ */
