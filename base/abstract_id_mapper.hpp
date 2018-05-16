/*
 * config.hpp
 *
 *  Created on: May 9, 2018
 *      Author: Hongzhi Chen
 */

#ifndef ABSTRACT_IDMAPPER_HPP_
#define ABSTRACT_IDMAPPER_HPP_

class AbstractIdMapper {
public:
  virtual ~AbstractIdMapper() {}

  // judge if it is vertex/edge/property
  virtual bool IsVertex(uint32_t v_id) = 0;
  virtual bool IsEdge(uint32_t e_id) = 0;
  virtual bool isProperty(uint32_t p_id) = 0;

  // judge if vertex/edge/property is local
  virtual bool IsVertexLocal(const uint32_t v_id) = 0;
  virtual bool IsEdgeLocal(const uint32_t e_id) = 0;
  virtual bool IsPropertyLocal(const uint32_t p_id) = 0;

  // vertex/edge/property -> machine index mapping
  virtual int GetMachineIdForVertex(const uint32_t v_id) = 0;
  virtual int GetMachineIdForEdge(const uint32_t e_id) = 0;
  virtual int GetMachineIdForProperty(const uint32_t p_id) = 0;

  // vertex/edge/property -> internal index mapping
  virtual uint32_t GetInternalIdForVertex(const uint32_t v_id) = 0;
  virtual uint32_t GetInternalIdForEdge(const uint32_t e_id) = 0;
  virtual uint32_t GetInternalIdForProperty(const uint32_t p_id) = 0;

  // Node Id -> RDMA ring buffer offset
  virtual int GetAndIncrementRdmaRingBufferOffset(const int n_id, const int msg_sz) = 0;
};

#endif /* ABSTRACT_IDMAPPER_HPP_ */
