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
	NaiveIdMapper(Config * config) : config_(config) {
    node_offset_.resize(config_->global_num_machines, 0);
  }

  // judge if vertex/edge/property local
  virtual bool IsVertexLocal(const uint32_t v_id) override {
      return GetMachineIdForVertex(v_id) == config_->my_node.id;
  }

  virtual bool IsEdgeLocal(const uint32_t e_id) override {
      return GetMachineIdForEdge(e_id) == config_->my_node.id;
  }
  
  virtual bool IsPropertyLocal(const uint32_t p_id) override {
      return GetMachineIdForVertex(p_id) == config_->my_node.id;
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
      return (v_id - 0) / config_->global_num_machines;
  }
  virtual uint32_t GetInternalIdForEdge(const uint32_t e_id) override {
      return (e_id - config_->VID_EID) / config_->global_num_machines;
  }
  virtual uint32_t GetInternalIdForProperty(const uint32_t p_id) override {
      return (p_id - config_->EID_PID) / config_->global_num_machines;
  }

  // node Id -> RDMA ring buffer offset
  virtual int GetAndIncrementRdmaRingBufferOffset(const int nid, const int msg_sz) override { 
      CHECK_LT(nid, config_->global_num_machines);
      std::lock_guard<std::mutex> lk(node_offset_mu_);
      int return_offset = node_offset_[nid];
      node_offset_[nid] = (node_offset_[nid] + msg_sz) % MiB2B(config_->global_per_recv_buffer_sz_mb);
      return return_offset; 
  }

  // TODO: not carefully thought, and uint32_t should be used
  // judge if it is vertex/edge/property id
  virtual bool IsVertex(uint32_t v_id) override {
      return v_id >= 0 && v_id < config_->VID_EID;
  } 
  virtual bool IsEdge(uint32_t e_id) override {
      return e_id >= config_->VID_EID && e_id < config_->EID_PID;
  }
  virtual bool isProperty(uint32_t p_id) override {
      return p_id >= config_->EID_PID;
  }

private:
  Config * config_;

  // Node Id -> RDMA ring buffer offest
  // TODO: every entry of node_offset_ will need a mutex, but the mutex need to be wrapped
  std::mutex node_offset_mu_;
  std::vector<int> node_offset_;
};

#endif /* IDMAPPER_HPP_ */
