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
  virtual int GetMachineIdForVertex(vid_t v_id) = 0;
  virtual int GetMachineIdForEdge(eid_t e_id, bool considerDstVtx = false) = 0;
  virtual int GetMachineIdForVProperty(vpid_t p_id) = 0;
  virtual int GetMachineIdForEProperty(epid_t p_id) = 0;
};

#endif /* ABSTRACT_IDMAPPER_HPP_ */
