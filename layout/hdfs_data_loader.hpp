/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Chenghuan Huang (chhuang@cse.cuhk.edu.hk)
*/


#pragma once

#include <cstdio>

#include "base/communication.hpp"
#include "base/node.hpp"
#include "layout/layout_type.hpp"
#include "storage/layout.hpp"
#include "utils/config.hpp"
#include "utils/hdfs_core.hpp"
#include "utils/timer.hpp"
#include "utils/tool.hpp"

namespace std {

class HDFSDataLoader {
 private:
    Config* config_;
    Node node_;

    HDFSDataLoader() {}
    HDFSDataLoader(const HDFSDataLoader&);

    // free them after calling Shuffle()
    vector<Edge*> edges_;
    vector<Vertex*> vertices_;
    vector<VProperty*> vplist_;
    vector<EProperty*> eplist_;

    typedef map<string, uint8_t> TypeMap;
    TypeMap vtx_pty_key_to_type_;
    TypeMap edge_pty_key_to_type_;
    void LoadVertices(const char* inpath);
    void LoadVPList(const char* inpath);
    void LoadEPList(const char* inpath);
    Vertex* ToVertex(char* line);
    void ToVP(char* line);
    void ToEP(char* line);

    // ep just follows the src_v
    // src_v -> e -> dst_v
    inline int VidMapping(vid_t vid) {
        return vid.value() % node_.get_local_size();
    }

    inline int EidMapping(eid_t eid) {
        return eid.out_v % node_.get_local_size();
    }

    inline int VPidMapping(vpid_t vpid) {
        return vpid.vid % node_.get_local_size();
    }

    inline int EPidMappingIn(epid_t epid) {
        return epid.in_vid % node_.get_local_size();
    }

    inline int EPidMapping(epid_t epid) {
        return epid.out_vid % node_.get_local_size();
    }

 public:
    static HDFSDataLoader* GetInstance() {
        static HDFSDataLoader* hdfs_data_loader_instance_ptr = nullptr;

        if (hdfs_data_loader_instance_ptr == nullptr) {
            hdfs_data_loader_instance_ptr = new HDFSDataLoader();
            hdfs_data_loader_instance_ptr->Initial();
        }

        return hdfs_data_loader_instance_ptr;
    }

    // data loading function based on how data stored on HDFS by the original GQuery
    void GetStringIndexes();
    void GetVertices();
    void GetVPList();
    void GetEPList();

    void Initial();
    void LoadData();
    void Shuffle();
    void FreeMemory();

    // "schema" related
    // this can be copy to DataStorage
    string_index* indexes_;

    // shuffled data
    hash_map<uint32_t, TMPVertex*> vtx_part_map_;
    hash_map<uint64_t, TMPEdge*> edge_part_map_;
    vector<TMPVertex> shuffled_vtx_;
    vector<TMPEdge> shuffled_edge_;
};

}  // namespace std
