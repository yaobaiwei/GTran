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

#pragma once

#include <cstdio>

#include "base/communication.hpp"
#include "base/node.hpp"
#include "core/id_mapper.hpp"
#include "layout/mpi_snapshot_manager.hpp"
#include "utils/config.hpp"
#include "utils/hdfs_core.hpp"
#include "utils/timer.hpp"
#include "utils/tool.hpp"


/* =================== TMP datatypes for data loading =================== */
// used for data loading from HDFS: V_KVpair, E_KVpair, VProperty, EProperty, TMPVertexInfo

struct V_KVpair {
    vpid_t key;
    value_t value;
    string DebugString() const;
};

struct E_KVpair {
    epid_t key;
    value_t value;
    string DebugString() const;
};

struct VProperty {
    vid_t id;
    vector<V_KVpair> plist;
    string DebugString() const;
};

struct EProperty {
    eid_t id;
    vector<E_KVpair> plist;
    string DebugString() const;
};

struct TMPVertexInfo {
    vid_t id;
    vector<vid_t> in_nbs;  // this_vtx <- in_nbs
    vector<vid_t> out_nbs;  // this_vtx -> out_nbs
    vector<label_t> vp_list;

    string DebugString() const;
};

ibinstream& operator<<(ibinstream& m, const V_KVpair& pair);
obinstream& operator>>(obinstream& m, V_KVpair& pair);
ibinstream& operator<<(ibinstream& m, const VProperty& vp);
obinstream& operator>>(obinstream& m, VProperty& vp);
ibinstream& operator<<(ibinstream& m, const E_KVpair& pair);
obinstream& operator>>(obinstream& m, E_KVpair& pair);
ibinstream& operator<<(ibinstream& m, const EProperty& ep);
obinstream& operator>>(obinstream& m, EProperty& ep);
ibinstream& operator<<(ibinstream& m, const TMPVertexInfo& v);
obinstream& operator>>(obinstream& m, TMPVertexInfo& v);


// used for data loading after shuffling: TMPVertex, TMPOutEdge, TMPInEdge

struct TMPVertex {
    vid_t id;
    label_t label;
    std::vector<vid_t> in_nbs;  // this_vtx <- in_nbs
    std::vector<vid_t> out_nbs;  // this_vtx -> out_nbs
    std::vector<label_t> vp_label_list;
    std::vector<value_t> vp_value_list;

    std::string DebugString() const;
};

struct TMPOutEdge {
    eid_t id;  // id.src_v -> e -> id.dst_v, follows src_v
    label_t label;
    std::vector<label_t> ep_label_list;
    std::vector<value_t> ep_value_list;

    std::string DebugString() const;
};

struct TMPInEdge {
    eid_t id;  // id.src_v -> e -> id.dst_v, follows src_v
    label_t label;
};

ibinstream& operator<<(ibinstream& m, const TMPVertex& v);
obinstream& operator>>(obinstream& m, TMPVertex& v);
ibinstream& operator<<(ibinstream& m, const TMPOutEdge& v);
obinstream& operator>>(obinstream& m, TMPOutEdge& v);
ibinstream& operator<<(ibinstream& m, const TMPInEdge& v);
obinstream& operator>>(obinstream& m, TMPInEdge& v);


/* =================== HDFSDataLoader =================== */

class HDFSDataLoader {
 private:
    Config* config_;
    Node node_;

    HDFSDataLoader() {}
    HDFSDataLoader(const HDFSDataLoader&);

    // free them after calling Shuffle()
    vector<TMPVertexInfo*> vertices_;
    vector<VProperty*> vplist_;
    vector<EProperty*> eplist_;
    hash_map<uint32_t, TMPVertex*> vtx_part_map_;

    void LoadVertices(const char* inpath);
    void LoadVPList(const char* inpath);
    void LoadEPList(const char* inpath);
    TMPVertexInfo* ToVertex(char* line);
    void ToVP(char* line);
    void ToEP(char* line);

    bool ReadVertexSnapshot();
    void WriteVertexSnapshot();
    bool ReadEdgeSnapshot();
    void WriteEdgeSnapshot();

    void GetVertices();
    void GetVPList();
    void GetEPList();
    void ShuffleVertex();
    void ShuffleEdge();

 public:
    static HDFSDataLoader* GetInstance() {
        static HDFSDataLoader* hdfs_data_loader_instance_ptr = nullptr;

        if (hdfs_data_loader_instance_ptr == nullptr) {
            hdfs_data_loader_instance_ptr = new HDFSDataLoader();
            hdfs_data_loader_instance_ptr->Init();
        }

        return hdfs_data_loader_instance_ptr;
    }

    ~HDFSDataLoader();

    void Init();
    void GetStringIndexes();
    void LoadVertexData();
    void LoadEdgeData();
    void FreeVertexMemory();
    void FreeEdgeMemory();

    // "schema" related
    string_index* indexes_;

    vector<TMPVertex> shuffled_vtx_;
    vector<TMPOutEdge> shuffled_out_edge_;
    vector<TMPInEdge> shuffled_in_edge_;

    // ep just follows the src_v
    // src_v -> e -> dst_v
    SimpleIdMapper* id_mapper_ = nullptr;

    MPISnapshotManager* snapshot_manager_ = nullptr;
};
