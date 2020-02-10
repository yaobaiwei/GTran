/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Chenghuan Huang (chhuang@cse.cuhk.edu.hk)
*/


#include "layout/hdfs_data_loader.hpp"

string V_KVpair::DebugString() const {
    stringstream ss;
    ss << "V_KVpair: { key = " << key.vid << "|"
        << key.pid << ", value.type = "
        << static_cast<int>(value.type) << " }" << endl;
    return ss.str();
}

string E_KVpair::DebugString() const {
    stringstream ss;
    ss << "E_KVpair: { key = " << key.dst_vid << "|"
       << key.src_vid << "|"
       << key.pid << ", value.type = "
       << static_cast<int>(value.type) << " }" << endl;
    return ss.str();
}

string VProperty::DebugString() const {
    stringstream ss;
    ss << "VProperty: { id = " << id.vid <<  " plist = [" << endl;
    for (auto & vp : plist)
        ss << vp.DebugString();
    ss << "]}" << endl;
    return ss.str();
}

string EProperty::DebugString() const {
    stringstream ss;
    ss << "EProperty: { id = " << id.dst_v << "," << id.src_v <<  " plist = [" << endl;
    for (auto & ep : plist)
        ss << ep.DebugString();
    ss << "]}" << endl;
    return ss.str();
}

string TMPVertexInfo::DebugString() const {
    stringstream ss;
    ss << "TMPVertexInfo: { id = " << id.vid << " in_nbs = [";
    for (auto & vid : in_nbs)
        ss << vid.vid << ", ";
    ss << "] out_nbs = [";
    for (auto & vid : out_nbs)
        ss << vid.vid << ", ";
    ss << "] vp_list = [";
    for (auto & p : vp_list)
        ss << p << ", ";
    ss << "]}" << endl;
    return ss.str();
}

ibinstream& operator<<(ibinstream& m, const V_KVpair& pair) {
    m << pair.key;
    m << pair.value;
    return m;
}

obinstream& operator>>(obinstream& m, V_KVpair& pair) {
    m >> pair.key;
    m >> pair.value;
    return m;
}

ibinstream& operator<<(ibinstream& m, const E_KVpair& pair) {
    m << pair.key;
    m << pair.value;
    return m;
}

obinstream& operator>>(obinstream& m, E_KVpair& pair) {
    m >> pair.key;
    m >> pair.value;
    return m;
}

ibinstream& operator<<(ibinstream& m, const VProperty& vp) {
    m << vp.id;
    m << vp.plist;
    return m;
}

obinstream& operator>>(obinstream& m, VProperty& vp) {
    m >> vp.id;
    m >> vp.plist;
    return m;
}

ibinstream& operator<<(ibinstream& m, const EProperty& ep) {
    m << ep.id;
    m << ep.plist;
    return m;
}

obinstream& operator>>(obinstream& m, EProperty& ep) {
    m >> ep.id;
    m >> ep.plist;
    return m;
}

ibinstream& operator<<(ibinstream& m, const TMPVertexInfo& v) {
    m << v.id;
    m << v.in_nbs;
    m << v.out_nbs;
    m << v.vp_list;
    return m;
}

obinstream& operator>>(obinstream& m, TMPVertexInfo& v) {
    m >> v.id;
    m >> v.in_nbs;
    m >> v.out_nbs;
    m >> v.vp_list;
    return m;
}

string TMPVertex::DebugString() const {
    string ret = "vid: " + to_string(id.value()) + ", label: " + to_string(label);

    ret += ", in_nbs: [";
    for (int i = 0; i < in_nbs.size(); i++) {
        if (i != 0) {
            ret += ", ";
        }
        ret += to_string(in_nbs[i].value());
    }
    ret += "], out_nbs: [";
    for (int i = 0; i < out_nbs.size(); i++) {
        if (i != 0) {
            ret += ", ";
        }
        ret += to_string(out_nbs[i].value());
    }

    ret += "], properties: [";

    for (int i = 0; i < vp_label_list.size(); i++) {
        if (i != 0) {
            ret += ", ";
        }
        ret += "{" + to_string(vp_label_list[i]) + ", " + vp_value_list[i].DebugString() + "}";
    }
    ret += "]";

    return ret;
}

string TMPOutEdge::DebugString() const {
    string ret = "eid: " + to_string(id.src_v) + "->" + to_string(id.dst_v) + ", label: " + to_string(label);

    ret += ", properties: [";

    for (int i = 0; i < ep_label_list.size(); i++) {
        if (i != 0) {
            ret += ", ";
        }
        ret += "{" + to_string(ep_label_list[i]) + ", " + ep_value_list[i].DebugString() + "}";
    }
    ret += "]";

    return ret;
}

ibinstream& operator<<(ibinstream& m, const TMPVertex& v) {
    m << v.id;
    m << v.label;
    m << v.in_nbs;
    m << v.out_nbs;
    m << v.vp_label_list;
    m << v.vp_value_list;
    return m;
}

obinstream& operator>>(obinstream& m, TMPVertex& v) {
    m >> v.id;
    m >> v.label;
    m >> v.in_nbs;
    m >> v.out_nbs;
    m >> v.vp_label_list;
    m >> v.vp_value_list;
    return m;
}

ibinstream& operator<<(ibinstream& m, const TMPOutEdge& e) {
    m << e.id;
    m << e.label;
    m << e.ep_label_list;
    m << e.ep_value_list;
    return m;
}

obinstream& operator>>(obinstream& m, TMPOutEdge& e) {
    m >> e.id;
    m >> e.label;
    m >> e.ep_label_list;
    m >> e.ep_value_list;
    return m;
}

ibinstream& operator<<(ibinstream& m, const TMPInEdge& e) {
    m << e.id;
    m << e.label;
    return m;
}

obinstream& operator>>(obinstream& m, TMPInEdge& e) {
    m >> e.id;
    m >> e.label;
    return m;
}

void HDFSDataLoader::Init() {
    config_ = Config::GetInstance();
    node_ = Node::StaticInstance();
    id_mapper_ = SimpleIdMapper::GetInstance();
    snapshot_manager_ = MPISnapshotManager::GetInstance();

    snapshot_manager_->SetRootPath(config_->SNAPSHOT_PATH);
    snapshot_manager_->AppendConfig("HDFS_INDEX_PATH", config_->HDFS_INDEX_PATH);
    snapshot_manager_->AppendConfig("HDFS_VTX_SUBFOLDER", config_->HDFS_VTX_SUBFOLDER);
    snapshot_manager_->AppendConfig("HDFS_VP_SUBFOLDER", config_->HDFS_VP_SUBFOLDER);
    snapshot_manager_->AppendConfig("HDFS_EP_SUBFOLDER", config_->HDFS_EP_SUBFOLDER);
    snapshot_manager_->SetComm(node_.local_comm);
    snapshot_manager_->ConfirmConfig();

    indexes_ = new string_index;
}

HDFSDataLoader::~HDFSDataLoader() {
    delete snapshot_manager_;
}

void HDFSDataLoader::GetStringIndexes() {
    hdfsFS fs = get_hdfs_fs();

    string el_path = config_->HDFS_INDEX_PATH + "./edge_label";
    hdfsFile el_file = get_r_handle(el_path.c_str(), fs);
    LineReader el_reader(fs, el_file);
    while (true) {
        el_reader.read_line();
        if (!el_reader.eof()) {
            char * line = el_reader.get_line();
            char * pch;
            pch = strtok(line, "\t");
            string key(pch);
            pch = strtok(nullptr, "\t");
            label_t id = atoi(pch);

            // both string and ID are unique
            assert(indexes_->str2el.find(key) == indexes_->str2el.end());
            assert(indexes_->el2str.find(id) == indexes_->el2str.end());

            indexes_->str2el[key] = id;
            indexes_->el2str[id] = key;
        } else {
            break;
        }
    }
    hdfsCloseFile(fs, el_file);

    string epk_path = config_->HDFS_INDEX_PATH + "./edge_property_index";
    hdfsFile epk_file = get_r_handle(epk_path.c_str(), fs);
    LineReader epk_reader(fs, epk_file);
    while (true) {
        epk_reader.read_line();
        if (!epk_reader.eof()) {
            char * line = epk_reader.get_line();
            char * pch;
            pch = strtok(line, "\t");
            string key(pch);
            pch = strtok(nullptr, "\t");
            label_t id = atoi(pch);
            pch = strtok(nullptr, "\t");
            indexes_->str2eptype[to_string(id)] = atoi(pch);

            // both string and ID are unique
            assert(indexes_->str2epk.find(key) == indexes_->str2epk.end());
            assert(indexes_->epk2str.find(id) == indexes_->epk2str.end());

            indexes_->str2epk[key] = id;
            indexes_->epk2str[id] = key;
        } else {
            break;
        }
    }
    hdfsCloseFile(fs, epk_file);

    string vl_path = config_->HDFS_INDEX_PATH + "./vtx_label";
    hdfsFile vl_file = get_r_handle(vl_path.c_str(), fs);
    LineReader vl_reader(fs, vl_file);
    while (true) {
        vl_reader.read_line();
        if (!vl_reader.eof()) {
            char * line = vl_reader.get_line();
            char * pch;
            pch = strtok(line, "\t");
            string key(pch);
            pch = strtok(nullptr, "\t");
            label_t id = atoi(pch);

            // both string and ID are unique
            assert(indexes_->str2vl.find(key) == indexes_->str2vl.end());
            assert(indexes_->vl2str.find(id) == indexes_->vl2str.end());

            indexes_->str2vl[key] = id;
            indexes_->vl2str[id] = key;
        } else {
            break;
        }
    }
    hdfsCloseFile(fs, vl_file);

    string vpk_path = config_->HDFS_INDEX_PATH + "./vtx_property_index";
    hdfsFile vpk_file = get_r_handle(vpk_path.c_str(), fs);
    LineReader vpk_reader(fs, vpk_file);
    while (true) {
        vpk_reader.read_line();
        if (!vpk_reader.eof()) {
            char * line = vpk_reader.get_line();
            char * pch;
            pch = strtok(line, "\t");
            string key(pch);
            pch = strtok(nullptr, "\t");
            label_t id = atoi(pch);
            pch = strtok(nullptr, "\t");
            indexes_->str2vptype[to_string(id)] = atoi(pch);

            // both string and ID are unique
            assert(indexes_->str2vpk.find(key) == indexes_->str2vpk.end());
            assert(indexes_->vpk2str.find(id) == indexes_->vpk2str.end());

            indexes_->str2vpk[key] = id;
            indexes_->vpk2str[id] = key;
        } else {
            break;
        }
    }
    hdfsCloseFile(fs, vpk_file);
    hdfsDisconnect(fs);
}

void HDFSDataLoader::GetVertices() {
    const char * indir = config_->HDFS_VTX_SUBFOLDER.c_str();

    // topo
    if (node_.get_local_rank() == MASTER_RANK) {
        vector<vector<string>> arrangement = dispatch_locality(indir, node_.get_local_size());
        master_scatter(node_, false, arrangement);
        vector<string>& assigned_splits = arrangement[0];
        // reading assigned splits (map)
        for (vector<string>::iterator it = assigned_splits.begin(); it != assigned_splits.end(); it++)
            LoadVertices(it->c_str());
    } else {
        vector<string> assigned_splits;
        slave_scatter(node_, false, assigned_splits);
        // reading assigned splits (map)
        for (vector<string>::iterator it = assigned_splits.begin(); it != assigned_splits.end(); it++)
            LoadVertices(it->c_str());
    }
}

void HDFSDataLoader::LoadVertices(const char* inpath) {
    hdfsFS fs = get_hdfs_fs();
    hdfsFile in = get_r_handle(inpath, fs);
    LineReader reader(fs, in);
    while (true) {
        reader.read_line();
        if (!reader.eof()) {
            TMPVertexInfo * v = ToVertex(reader.get_line());
            vertices_.push_back(v);
        } else {
            break;
        }
    }
    hdfsCloseFile(fs, in);
    hdfsDisconnect(fs);
}

// Format
// vid [\t] #in_nbs [\t] nb1 [space] nb2 [space] ... #out_nbs [\t] nb1 [space] nb2 [space] ...
TMPVertexInfo* HDFSDataLoader::ToVertex(char* line) {
    TMPVertexInfo * v = new TMPVertexInfo;

    char * pch;
    pch = strtok(line, "\t");

    vid_t vid(atoi(pch));
    v->id = vid;

    pch = strtok(nullptr, "\t");
    int num_in_nbs = atoi(pch);
    for (int i = 0 ; i < num_in_nbs; i++) {
        pch = strtok(nullptr, " ");
        v->in_nbs.push_back(atoi(pch));
    }
    pch = strtok(nullptr, "\t");
    int num_out_nbs = atoi(pch);
    for (int i = 0 ; i < num_out_nbs; i++) {
        pch = strtok(nullptr, " ");
        v->out_nbs.push_back(atoi(pch));
    }
    return v;
}

void HDFSDataLoader::GetVPList() {
    const char * indir = config_->HDFS_VP_SUBFOLDER.c_str();

    if (node_.get_local_rank() == MASTER_RANK) {
        if (dir_check(indir) == -1)
            exit(-1);
    }

    if (node_.get_local_rank() == MASTER_RANK) {
        vector<vector<string>> arrangement = dispatch_locality(indir, node_.get_local_size());
        master_scatter(node_, false, arrangement);
        vector<string>& assigned_splits = arrangement[0];
        // reading assigned splits (map)
        for (vector<string>::iterator it = assigned_splits.begin(); it != assigned_splits.end(); it++)
            LoadVPList(it->c_str());
    } else {
        vector<string> assigned_splits;
        slave_scatter(node_, false, assigned_splits);
        // reading assigned splits (map)
        for (vector<string>::iterator it = assigned_splits.begin(); it != assigned_splits.end(); it++)
            LoadVPList(it->c_str());
    }
}

void HDFSDataLoader::LoadVPList(const char* inpath) {
    hdfsFS fs = get_hdfs_fs();
    hdfsFile in = get_r_handle(inpath, fs);
    LineReader reader(fs, in);
    while (true) {
        reader.read_line();
        if (!reader.eof())
            ToVP(reader.get_line());
        else
            break;
    }
    hdfsCloseFile(fs, in);
    hdfsDisconnect(fs);
}

// Format
// vid [\t] label[\t] [kid:value,kid:value,...]
void HDFSDataLoader::ToVP(char* line) {
    VProperty * vp = new VProperty;

    char * pch;
    pch = strtok(line, "\t");
    vid_t vid(atoi(pch));
    vp->id = vid;

    pch = strtok(nullptr, "\t");
    label_t label = (label_t)atoi(pch);

    // insert label to VProperty
    V_KVpair v_pair;
    v_pair.key = vpid_t(vid, 0);
    Tool::str2int(to_string(label), v_pair.value);
    // push to property_list of v
    vp->plist.push_back(v_pair);

    pch = strtok(nullptr, "");
    string s(pch);

    vector<string> kvpairs;
    Tool::splitWithEscape(s, "[],:", kvpairs);
    assert(kvpairs.size() % 2 == 0);
    for (int i = 0 ; i < kvpairs.size(); i += 2) {
        kv_pair p;
        Tool::get_kvpair(kvpairs[i], kvpairs[i+1], indexes_->str2vptype[kvpairs[i]], p);
        V_KVpair v_pair;
        v_pair.key = vpid_t(vid, p.key);
        v_pair.value = p.value;

        // push to property_list of v
        vp->plist.push_back(v_pair);
    }

    vplist_.push_back(vp);
}

void HDFSDataLoader::GetEPList() {
    const char * indir = config_->HDFS_EP_SUBFOLDER.c_str();

    if (node_.get_local_rank() == MASTER_RANK) {
        vector<vector<string>> arrangement = dispatch_locality(indir, node_.get_local_size());
        master_scatter(node_, false, arrangement);
        vector<string>& assigned_splits = arrangement[0];
        // reading assigned splits (map)
        for (vector<string>::iterator it = assigned_splits.begin(); it != assigned_splits.end(); it++)
            LoadEPList(it->c_str());
    } else {
        vector<string> assigned_splits;
        slave_scatter(node_, false, assigned_splits);
        // reading assigned splits (map)
        for (vector<string>::iterator it = assigned_splits.begin(); it != assigned_splits.end(); it++)
            LoadEPList(it->c_str());
    }
}

void HDFSDataLoader::LoadEPList(const char* inpath) {
    hdfsFS fs = get_hdfs_fs();
    hdfsFile in = get_r_handle(inpath, fs);
    LineReader reader(fs, in);
    while (true) {
        reader.read_line();
        if (!reader.eof())
            ToEP(reader.get_line());
        else
            break;
    }
    hdfsCloseFile(fs, in);
    hdfsDisconnect(fs);
}

void HDFSDataLoader::ToEP(char* line) {
    EProperty * ep = new EProperty;

    uint64_t atoi_time = timer::get_usec();
    char * pch;
    pch = strtok(line, "\t");
    int src_v = atoi(pch);
    pch = strtok(nullptr, "\t");
    int dst_v = atoi(pch);

    eid_t eid(dst_v, src_v);
    ep->id = eid;

    pch = strtok(nullptr, "\t");
    label_t label = (label_t)atoi(pch);
    // insert label to EProperty
    E_KVpair e_pair;
    e_pair.key = epid_t(dst_v, src_v, 0);
    Tool::str2int(to_string(label), e_pair.value);
    // push to property_list of v
    ep->plist.push_back(e_pair);

    pch = strtok(nullptr, "");
    string s(pch);
    vector<label_t> pkeys;

    vector<string> kvpairs;
    Tool::splitWithEscape(s, "[],:", kvpairs);
    assert(kvpairs.size() % 2 == 0);

    for (int i = 0 ; i < kvpairs.size(); i += 2) {
        kv_pair p;
        Tool::get_kvpair(kvpairs[i], kvpairs[i+1], indexes_->str2eptype[kvpairs[i]], p);

        E_KVpair e_pair;
        e_pair.key = epid_t(dst_v, src_v, p.key);
        e_pair.value = p.value;

        ep->plist.push_back(e_pair);
        pkeys.push_back((label_t)p.key);
    }

    sort(pkeys.begin(), pkeys.end());
    eplist_.push_back(ep);
}

void HDFSDataLoader::LoadVertexData() {
    bool success = ReadVertexSnapshot();

    // Make sure that all workers have successfully read the snapshot
    MPI_Allreduce(MPI_IN_PLACE, &success, 1, MPI_C_BOOL, MPI_LAND, node_.local_comm);

    if (!success) {
        node_.Rank0PrintfWithWorkerBarrier("!HDFSDataLoader::ReadVertexSnapshot()\n");
        vector<TMPVertex>().swap(shuffled_vtx_);
        GetVertices();
        node_.Rank0PrintfWithWorkerBarrier("HDFSDataLoader::GetVertices() finished\n");
        GetVPList();
        node_.Rank0PrintfWithWorkerBarrier("HDFSDataLoader::GetVPList() finished\n");
        ShuffleVertex();
        node_.Rank0PrintfWithWorkerBarrier("HDFSDataLoader::ShuffleVertex() finished\n");
        WriteVertexSnapshot();
    }
    node_.Rank0PrintfWithWorkerBarrier("HDFSDataLoader::LoadVertexData() finished\n");
}

void HDFSDataLoader::LoadEdgeData() {
    bool success = ReadEdgeSnapshot();

    // Make sure that all workers have successfully read the snapshot
    MPI_Allreduce(MPI_IN_PLACE, &success, 1, MPI_C_BOOL, MPI_LAND, node_.local_comm);

    if (!success) {
        node_.Rank0PrintfWithWorkerBarrier("!HDFSDataLoader::ReadEdgeSnapshot()\n");
        vector<TMPOutEdge>().swap(shuffled_out_edge_);
        vector<TMPInEdge>().swap(shuffled_in_edge_);
        GetEPList();
        node_.Rank0PrintfWithWorkerBarrier("HDFSDataLoader::GetEPList() finished\n");
        ShuffleEdge();
        node_.Rank0PrintfWithWorkerBarrier("HDFSDataLoader::ShuffleEdge() finished\n");
        WriteEdgeSnapshot();
    }
    node_.Rank0PrintfWithWorkerBarrier("HDFSDataLoader::LoadEdgeData() finished\n");
}

bool HDFSDataLoader::ReadVertexSnapshot() {
    if (!snapshot_manager_->ReadData("HDFSDataLoader::shuffled_vtx_", shuffled_vtx_))
        return false;

    return true;
}

bool HDFSDataLoader::ReadEdgeSnapshot() {
    if (!snapshot_manager_->ReadData("HDFSDataLoader::shuffled_out_edge_", shuffled_out_edge_))
        return false;
    if (!snapshot_manager_->ReadData("HDFSDataLoader::shuffled_in_edge_", shuffled_in_edge_))
        return false;

    return true;
}

void HDFSDataLoader::WriteVertexSnapshot() {
    node_.Rank0PrintfWithWorkerBarrier("HDFSDataLoader::WriteSnapshot() shuffled_vtx_\n");
    snapshot_manager_->WriteData("HDFSDataLoader::shuffled_vtx_", shuffled_vtx_);
    node_.Rank0PrintfWithWorkerBarrier("HDFSDataLoader::WriteVertexSnapshot() finished\n");
}

void HDFSDataLoader::WriteEdgeSnapshot() {
    node_.Rank0PrintfWithWorkerBarrier("HDFSDataLoader::WriteSnapshot() shuffled_out_edge_\n");
    snapshot_manager_->WriteData("HDFSDataLoader::shuffled_out_edge_", shuffled_out_edge_);
    node_.Rank0PrintfWithWorkerBarrier("HDFSDataLoader::WriteSnapshot() shuffled_in_edge_\n");
    snapshot_manager_->WriteData("HDFSDataLoader::shuffled_in_edge_", shuffled_in_edge_);
    node_.Rank0PrintfWithWorkerBarrier("HDFSDataLoader::WriteEdgeSnapshot() finished\n");
}

void HDFSDataLoader::ShuffleVertex() {
    // vertices_
    vector<vector<TMPVertexInfo*>> vtx_parts;
    vtx_parts.resize(node_.get_local_size());
    for (int i = 0; i < vertices_.size(); i++) {
        TMPVertexInfo* v = vertices_[i];
        vtx_parts[id_mapper_->GetMachineIdForVertex(v->id)].push_back(v);
    }
    all_to_all(node_, false, vtx_parts);
    vertices_.clear();

    node_.Rank0PrintfWithWorkerBarrier("HDFSDataLoader Shuffle vertices done\n");

    for (int i = 0; i < node_.get_local_size(); i++) {
        vertices_.insert(vertices_.end(), vtx_parts[i].begin(), vtx_parts[i].end());
    }
    vtx_parts.clear();
    vector<vector<TMPVertexInfo*>>().swap(vtx_parts);

    // VProperty
    vector<vector<VProperty*>> vp_parts;
    vp_parts.resize(node_.get_local_size());
    for (int i = 0; i < vplist_.size(); i++) {
        VProperty* vp = vplist_[i];
        map<int, vector<V_KVpair>> node_map;
        for (auto& kv_pair : vp->plist)
            node_map[id_mapper_->GetMachineIdForVProperty(kv_pair.key)].push_back(kv_pair);
        for (auto& item : node_map) {
            VProperty* vp_ = new VProperty();
            vp_->id = vp->id;
            vp_->plist = move(item.second);
            vp_parts[item.first].push_back(vp_);
        }
        delete vp;
    }

    all_to_all(node_, false, vp_parts);

    node_.Rank0PrintfWithWorkerBarrier("HDFSDataLoader Shuffle vp done\n");

    vplist_.clear();

    for (int i = 0; i < node_.get_local_size(); i++) {
        vplist_.insert(vplist_.end(), vp_parts[i].begin(), vp_parts[i].end());
    }
    vp_parts.clear();
    vector<vector<VProperty*>>().swap(vp_parts);

    shuffled_vtx_.resize(vertices_.size());

    // construct TMPVertex
    int auto_iter_counter;

    auto_iter_counter = 0;
    for (auto vtx_topo : vertices_) {
        TMPVertex& vtx_ref = shuffled_vtx_[auto_iter_counter];
        vtx_part_map_[vtx_topo->id.vid] = &vtx_ref;

        vtx_ref.id = vtx_topo->id;
        vtx_ref.in_nbs = vtx_topo->in_nbs;
        vtx_ref.out_nbs = vtx_topo->out_nbs;

        auto_iter_counter++;
    }

    // insert vp
    for (auto vps : vplist_) {
        TMPVertex& vtx_ref = *vtx_part_map_[vps->id.vid];

        for (auto vp : vps->plist) {
            // label
            if (vp.key.pid == 0) {
                vtx_ref.label = Tool::value_t2int(vp.value);
            } else {
                vtx_ref.vp_label_list.push_back(vp.key.pid);
                vtx_ref.vp_value_list.push_back(vp.value);
            }
        }
    }

    // free shuffled buffer's memory
    for (auto ptr : vertices_)
        delete ptr;
    for (auto ptr : vplist_)
        delete ptr;

    vector<TMPVertexInfo*>().swap(vertices_);
    vector<VProperty*>().swap(vplist_);
    vtx_part_map_.clear();
}

void HDFSDataLoader::ShuffleEdge() {
    // EProperty
    vector<vector<EProperty*>> ep_parts;
    ep_parts.resize(node_.get_local_size());
    for (int i = 0; i < eplist_.size(); i++) {
        EProperty* ep = eplist_[i];
        map<int, vector<E_KVpair>> node_map;
        for (auto& kv_pair : ep->plist) {
            int src_v_node_id = id_mapper_->GetMachineIdForEProperty(kv_pair.key);
            node_map[src_v_node_id].push_back(kv_pair);
            // label should be stored on two side
            if (kv_pair.key.pid == 0) {
                int dst_v_node_id = id_mapper_->GetMachineIdForVertex(vid_t(kv_pair.key.dst_vid));
                if (dst_v_node_id != src_v_node_id) {
                    node_map[dst_v_node_id].push_back(kv_pair);
                }
            }
        }
        for (auto& item : node_map) {
            EProperty* ep_ = new EProperty();
            ep_->id = ep->id;
            ep_->plist = move(item.second);
            ep_parts[item.first].push_back(ep_);
        }
        delete ep;
    }

    all_to_all(node_, false, ep_parts);

    node_.Rank0PrintfWithWorkerBarrier("HDFSDataLoader Shuffle ep done\n");

    eplist_.clear();
    for (int i = 0; i < node_.get_local_size(); i++) {
        eplist_.insert(eplist_.end(), ep_parts[i].begin(), ep_parts[i].end());
    }
    ep_parts.clear();
    vector<vector<EProperty*>>().swap(ep_parts);

    // For an edge, if src_v and dst_v are both on this node, it will be stored in out_edge_map_
    size_t ine_cnt = 0, oute_cnt = 0;
    for (auto eps : eplist_) {
        if (id_mapper_->IsVertexLocal(eps->id.src_v)) {
            oute_cnt++;
        } else {
            ine_cnt++;
        }
    }

    shuffled_out_edge_.resize(oute_cnt);
    shuffled_in_edge_.resize(ine_cnt);

    size_t ine_iter_counter = 0, oute_iter_counter = 0;

    // construct shuffled_out_edge_ and shuffled_in_edge_
    for (auto eps : eplist_) {
        if (id_mapper_->IsVertexLocal(eps->id.src_v)) {
            auto& edge_ref = shuffled_out_edge_[oute_iter_counter];
            edge_ref.id = eps->id;
            for (auto ep : eps->plist) {
                if (ep.key.pid == 0) {
                    // label
                    edge_ref.label = Tool::value_t2int(ep.value);
                } else {
                    edge_ref.ep_label_list.push_back(ep.key.pid);
                    edge_ref.ep_value_list.push_back(ep.value);
                }
            }

            oute_iter_counter++;
        } else {
            auto& edge_ref = shuffled_in_edge_[ine_iter_counter];
            edge_ref.id = eps->id;
            edge_ref.label = Tool::value_t2int(eps->plist[0].value);

            ine_iter_counter++;
        }
    }

    // free shuffled buffer's memory
    for (auto ptr : eplist_)
        delete ptr;
    vector<EProperty*>().swap(eplist_);
}

void HDFSDataLoader::FreeVertexMemory() {
    vector<TMPVertex>().swap(shuffled_vtx_);
}

void HDFSDataLoader::FreeEdgeMemory() {
    vector<TMPOutEdge>().swap(shuffled_out_edge_);
    vector<TMPInEdge>().swap(shuffled_in_edge_);
}
