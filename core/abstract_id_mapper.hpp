/*
 * config.hpp
 *
 *  Created on: May 9, 2018
 *      Author: Hongzhi Chen
 */
//
#ifndef ABSTRACT_IDMAPPER_HPP_
#define ABSTRACT_IDMAPPER_HPP_

#include <stdint.h>
#include "base/type.hpp"

class AbstractIdMapper {
public:
  virtual ~AbstractIdMapper() {}

  virtual bool IsVertex(uint64_t v_id) = 0;
  virtual bool IsEdge(uint64_t e_id) = 0;
  virtual bool IsVProperty(uint64_t vp_id) = 0;
  virtual bool IsEProperty(uint64_t ep_id) = 0;

  virtual bool IsVertexLocal(const vid_t v_id) = 0;
  virtual bool IsEdgeLocal(const eid_t e_id) = 0;
  virtual bool IsVPropertyLocal(const vpid_t p_id) = 0;
  virtual bool IsEPropertyLocal(const epid_t p_id) = 0;

  // vertex/edge/property -> machine index mapping
  virtual int GetMachineIdForVertex(const vid_t v_id) = 0;
  virtual int GetMachineIdForEdge(const eid_t e_id) = 0;
  virtual int GetMachineIdForVProperty(const vpid_t p_id) = 0;
  virtual int GetMachineIdForEProperty(const epid_t p_id) = 0;

  // Node Id -> RDMA ring buffer offset
  virtual int GetAndIncrementRdmaRingBufferOffset(const int n_id, const int msg_sz) = 0;
};

#endif /* ABSTRACT_IDMAPPER_HPP_ */
