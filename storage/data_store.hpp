/*
 * gquery.cpp
 *
 *  Created on: May 9, 2018
 *      Author: Hongzhi Chen
 */

#pragma once

#include <new>

#include "core/id_mapper.hpp"
#include "utils/unit.hpp"
#include "utils/config.hpp"
#include "storage/abstract_storage.hpp"

// might be revised if RDMA is enabled
class DataStore : public AbstractStorage {
public:
	DataStore(){}

	void Set(char* data_store, Config* config, AbstractIdMapper* id_mapper) {
		data_store_ = data_store;
		config_ = config;
		id_mapper_ = id_mapper;
	}

	/*
	virtual std::vector<int> V() override { return V; }

	virtual std::vector<int> E() override { return E; }
	*/

	virtual Vertex* GetVertex(uint32_t v_id) override {
		CHECK(id_mapper_->IsVertexLocal(v_id));
		return new (reinterpret_cast<Vertex*>(data_store_ +
					sizeof(Vertex) * id_mapper_->GetInternalIdForVertex(v_id))) Vertex;
	  // destructor is not necessary because destructor is trivial
	}

  virtual Edge* GetEdge(uint32_t e_id) override {
      CHECK(id_mapper_->IsEdgeLocal(e_id));
      return new (reinterpret_cast<Edge*>(data_store_ +
    		  	  GiB2B(config_->global_vertex_nodes_sz_gb) +
                  sizeof(Edge) * id_mapper_->GetInternalIdForEdge(e_id))) Edge;
      // destructor is not necessary because destructor is trivial
  }


  virtual Property_KV* GetPropertyForVertex(uint32_t vp_id) override {
      // TODO: check p_id is local or remote
      return new (reinterpret_cast<Property_KV*>(data_store_ +
    		  	  GiB2B(config_->global_vertex_nodes_sz_gb) + GiB2B(config_->global_edge_nodes_sz_gb) +
                  sizeof(Property_KV) * id_mapper_->GetInternalIdForProperty(vp_id))) Property_KV;
      // destructor is not necessary because destructor is trivial
  }


  virtual Property_KV* GetPropertyForEdge(uint32_t ep_id) override {
      // TODO: check p_id is local or remote
      return new (reinterpret_cast<Property_KV*>(data_store_ +
    		  	  GiB2B(config_->global_vertex_nodes_sz_gb) + GiB2B(config_->global_edge_nodes_sz_gb) + GiB2B(config_->global_vertex_property_kv_sz_gb) +
                  sizeof(Property_KV) * id_mapper_->GetInternalIdForProperty(ep_id))) Property_KV;
      // destructor is not necessary because destructor is trivial
  }

  private:
    // vertex | edge | property
    char* data_store_;
    Config* config_;
    AbstractIdMapper* id_mapper_;

    /*
    // All the vertex in local machine
    std::vector<int> V;

    // All the edge in local machine
    std::vector<int> E;
    */
  };
