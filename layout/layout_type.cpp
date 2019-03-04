/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Chenghuan Huang (chhuang@cse.cuhk.edu.hk)
*/

#include "layout/layout_type.hpp"

using namespace std;

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
