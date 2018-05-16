#pragma once

#include <map>
#include <vector>

#include "storage/layout.hpp"
#include "glog/logging.h"

class AbstractStorage {
public:
  /*
  // Return all the vertex in local machine
  virtual std::vector<int> V() = 0;

  // Return all the edges in local machine
  virtual std::vector<int> E() = 0;
  */

  // Get a vertex
  virtual Vertex* GetVertex(uint32_t v_id) = 0;

  // Get an edge
  virtual Edge* GetEdge(uint32_t e_id) = 0;

  // Get a property
  //virtual PropertyBlock* GetProperty(int p_id) = 0;

  /*
  // For a vertex and a filter(can be empty), return the corresponding edges
  // Notice that either the vertex or the edge could be remote
  virtual std::vector<int> GetEdgesForVertex(std::map<int, int> filter) = 0;
  */

  // Get properties(a vector of property id) for a vertex
  //virtual std::vector<int> GetPropertyForVertex(int vid, std::vector<int>);

  // Get properties(a vector of property id) for a edge
  //virtual std::vector<int> GetPropertyForEdge(int eid, std::vector<int>);

  // Get properties(a vector of property id) for a vertex
  virtual Property_KV* GetPropertyForVertex(uint32_t vp_id);

  // Get properties(a vector of property id) for a edge
  virtual Property_KV* GetPropertyForEdge(uint32_t ep_id);
};
