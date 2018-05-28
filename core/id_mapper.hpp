/*
 * config.hpp
 *
 *  Created on: May 9, 2018
 *      Author: Hongzhi Chen
 */

#ifndef IDMAPPER_HPP_
#define IDMAPPER_HPP_

#include "base/abstract_id_mapper.hpp"
#include "utils/config.hpp"
#include "utils/unit.hpp"
#include "glog/logging.h"

#include <vector>
#include <mutex>

class NaiveIdMapper : public AbstractIdMapper {
public:
	NaiveIdMapper(Config * config, Node & node) : config_(config), my_node_(node) {
		node_offset_.resize(config_->global_num_machines, 0);
	}

	// judge if vertex/edge/property local
	virtual bool IsVertexLocal(const uint32_t v_id) override {
		return GetMachineIdForVertex(v_id) == my_node_.id;
	}

	virtual bool IsEdgeLocal(const uint32_t e_id) override {
		return GetMachineIdForEdge(e_id) == my_node_.id;
	}
  
	virtual bool IsPropertyLocal(const uint32_t p_id) override {
		return GetMachineIdForVertex(p_id) == my_node_.id;
	}

	// vertex/edge/property -> machine index mapping
	virtual int GetMachineIdForVertex(const uint32_t v_id) override {
		return v_id % config_->global_num_machines;
	}

	virtual int GetMachineIdForEdge(const uint32_t e_id) override {
		return e_id % config_->global_num_machines;
	}

	virtual int GetMachineIdForProperty(const uint32_t p_id) override {
		return p_id % config_->global_num_machines;
	}

	// vertex/edge/property -> internal index mapping
	virtual uint32_t GetInternalIdForVertex(const uint32_t v_id) override {

	}
	virtual uint32_t GetInternalIdForEdge(const uint32_t e_id) override {

	}
	virtual uint32_t GetInternalIdForProperty(const uint32_t p_id) override {

	}

	// node Id -> RDMA ring buffer offset
	virtual int GetAndIncrementRdmaRingBufferOffset(const int nid, const int msg_sz) override {
		  CHECK_LT(nid, config_->global_num_machines);
		  std::lock_guard<std::mutex> lk(node_offset_mu_);
		  int return_offset = node_offset_[nid];
		  node_offset_[nid] = (node_offset_[nid] + msg_sz) % MiB2B(config_->global_per_recv_buffer_sz_mb);
		  return return_offset;
	}

	// TODO
	virtual bool IsVertex(uint32_t v_id) override {

	}
	virtual bool IsEdge(uint32_t e_id) override {

	}
	virtual bool isProperty(uint32_t p_id) override {

	}

private:
	Config * config_;
	Node my_node_;

	// Node Id -> RDMA ring buffer offest
	// TODO: every entry of node_offset_ will need a mutex, but the mutex need to be wrapped
	std::mutex node_offset_mu_;
	std::vector<int> node_offset_;
};

#endif /* IDMAPPER_HPP_ */
