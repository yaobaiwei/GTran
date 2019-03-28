/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Chenghuan Huang (chhuang@cse.cuhk.edu.hk)
*/

#include "layout/layout_type.hpp"

namespace std {

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

ibinstream& operator<<(ibinstream& m, const TMPEdge& e) {
    m << e.id;
    m << e.label;
    m << e.ep_label_list;
    m << e.ep_value_list;
    return m;
}

obinstream& operator>>(obinstream& m, TMPEdge& e) {
    m >> e.id;
    m >> e.label;
    m >> e.ep_label_list;
    m >> e.ep_value_list;
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
        ret += "{" + to_string(vp_label_list[i]) + ", " + Tool::DebugString(vp_value_list[i]) + "}";
    }
    ret += "]";

    return ret;
}

string TMPEdge::DebugString() const {
    string ret = "eid: " + to_string(id.out_v) + "->" + to_string(id.in_v) + ", label: " + to_string(label);

    ret += ", properties: [";

    for (int i = 0; i < ep_label_list.size(); i++) {
        if (i != 0) {
            ret += ", ";
        }
        ret += "{" + to_string(ep_label_list[i]) + ", " + Tool::DebugString(ep_value_list[i]) + "}";
    }
    ret += "]";

    return ret;
}

tbb::concurrent_hash_map<uint64_t, depend_trx_lists> dep_trx_map;

string Vertex::DebugString() const {
    stringstream ss;
    ss << "Vertex: { id = " << id.vid << " in_nbs = [";
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

ibinstream& operator<<(ibinstream& m, const Vertex& v) {
    m << v.id;
    // m << v.label;
    m << v.in_nbs;
    m << v.out_nbs;
    m << v.vp_list;
    return m;
}

obinstream& operator>>(obinstream& m, Vertex& v) {
    m >> v.id;
    // m >> v.label;
    m >> v.in_nbs;
    m >> v.out_nbs;
    m >> v.vp_list;
    return m;
    return m;
}

string Edge::DebugString() const {
    stringstream ss;
    ss << "Edge: { id = " << id.in_v << "," << id.out_v <<  " ep_list = [";
    for (auto & ep : ep_list)
        ss << ep << ", ";
    ss << "]}" << endl;
    return ss.str();
}

ibinstream& operator<<(ibinstream& m, const Edge& e) {
    m << e.id;
    // m << e.label;
    m << e.ep_list;
    return m;
}

obinstream& operator>>(obinstream& m, Edge& e) {
    m >> e.id;
    // m >> e.label;
    m >> e.ep_list;
    return m;
}

string V_KVpair::DebugString() const {
    stringstream ss;
    ss << "V_KVpair: { key = " << key.vid << "|"
        << key.pid << ", value.type = "
        << static_cast<int>(value.type) << " }" << endl;
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

string VProperty::DebugString() const {
    stringstream ss;
    ss << "VProperty: { id = " << id.vid <<  " plist = [" << endl;
    for (auto & vp : plist)
        ss << vp.DebugString();
    ss << "]}" << endl;
    return ss.str();
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

string E_KVpair::DebugString() const {
    stringstream ss;
    ss << "E_KVpair: { key = " << key.in_vid << "|"
       << key.out_vid << "|"
       << key.pid << ", value.type = "
       << static_cast<int>(value.type) << " }" << endl;
    return ss.str();
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

string EProperty::DebugString() const {
    stringstream ss;
    ss << "EProperty: { id = " << id.in_v << "," << id.out_v <<  " plist = [" << endl;
    for (auto & ep : plist)
        ss << ep.DebugString();
    ss << "]}" << endl;
    return ss.str();
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

}  // namespace std
