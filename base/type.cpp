/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Hongzhi Chen (hzchen@cse.cuhk.edu.hk)
*/

#include <string>
#include <vector>
#include "base/type.hpp"
#include "utils/tool.hpp"

const unordered_map<PROCESS_STAT, string, EnumClassHash<PROCESS_STAT>> abort_reason_map = {
    {PROCESS_STAT::ABORT, "[Undefined abort reason]"},
    {PROCESS_STAT::ABORT_DROP_V_GET_CONN_E, "[ABORT_DROP_V_GET_CONN_E]"},
    {PROCESS_STAT::ABORT_DROP_V_APPEND, "[ABORT_DROP_V_APPEND]"},
    {PROCESS_STAT::ABORT_ADD_E_INVISIBLE_V, "[ABORT_ADD_E_INVISIBLE_V]"},
    {PROCESS_STAT::ABORT_ADD_E_APPEND, "[ABORT_ADD_E_APPEND]"},
    {PROCESS_STAT::ABORT_DROP_E_APPEND, "[ABORT_DROP_E_APPEND]"},
    {PROCESS_STAT::ABORT_MODIFY_VP_INVISIBLE_V, "[ABORT_MODIFY_VP_INVISIBLE_V]"},
    {PROCESS_STAT::ABORT_MODIFY_VP_APPEND, "[ABORT_MODIFY_VP_APPEND]"},
    {PROCESS_STAT::ABORT_MODIFY_EP_EITEM, "[ABORT_MODIFY_EP_EITEM]"},
    {PROCESS_STAT::ABORT_MODIFY_EP_DELETED_E, "[ABORT_MODIFY_EP_DELETED_E]"},
    {PROCESS_STAT::ABORT_MODIFY_EP_MODIFY, "[ABORT_MODIFY_EP_MODIFY]"},
    {PROCESS_STAT::ABORT_DROP_VP_INVISIBLE_V, "[ABORT_DROP_VP_INVISIBLE_V]"},
    {PROCESS_STAT::ABORT_DROP_VP_DROP, "[ABORT_DROP_VP_DROP]"},
    {PROCESS_STAT::ABORT_DROP_EP_EITEM, "[ABORT_DROP_EP_EITEM]"},
    {PROCESS_STAT::ABORT_DROP_EP_DELETED_E, "[ABORT_DROP_EP_DELETED_E]"},
    {PROCESS_STAT::ABORT_DROP_EP_DROP, "[ABORT_DROP_EP_DROP]"},
};

const unordered_map<TRX_STAT, string, EnumClassHash<TRX_STAT>> trx_stat_str_map = {
    {TRX_STAT::PROCESSING, "[PROCESSING]"},
    {TRX_STAT::VALIDATING, "[VALIDATING]"},
    {TRX_STAT::ABORT, "[ABORT]"},
    {TRX_STAT::COMMITTED, "[COMMITTED]"},
};

ibinstream& operator<<(ibinstream& m, const ptr_t& p) {
    uint64_t v = ptr_t2uint(p);
    m << v;
    return m;
}

obinstream& operator>>(obinstream& m, ptr_t& p) {
    uint64_t v;
    m >> v;
    uint2ptr_t(v, p);
    return m;
}

uint64_t ptr_t2uint(const ptr_t & p) {
    uint64_t r = 0;
    r += p.off;
    r <<= NBITS_SIZE;
    r += p.size;
    return r;
}

void uint2ptr_t(uint64_t v, ptr_t & p) {
    p.size = (v & _28LFLAG);
    p.off = (v >> NBITS_SIZE);
}

bool operator == (const ptr_t &p1, const ptr_t &p2) {
    if ((p1.off == p2.off) && (p1.size == p2.size))
        return true;
    else
        return false;
}

ibinstream& operator<<(ibinstream& m, const ikey_t& p) {
    m << p.pid;
    m << p.ptr;
    return m;
}

obinstream& operator>>(obinstream& m, ikey_t& p) {
    m >> p.pid;
    m >> p.ptr;
    return m;
}

bool operator == (const ikey_t &p1, const ikey_t &p2) {
    if ((p1.pid == p2.pid) && (p1.ptr == p2.ptr))
        return true;
    else
        return false;
}

ibinstream& operator<<(ibinstream& m, const vid_t& v) {
    uint32_t value = vid_t2uint(v);
    m << value;
    return m;
}

obinstream& operator>>(obinstream& m, vid_t& v) {
    uint32_t value;
    m >> value;
    uint2vid_t(value, v);
    return m;
}

uint32_t vid_t2uint(const vid_t & vid) {
    uint32_t r = vid.vid;
    return r;
}

void uint2vid_t(uint32_t v, vid_t & vid) {
    vid.vid = v;
}

bool operator == (const vid_t &p1, const vid_t &p2) {
    if (p1.vid == p2.vid)
        return true;
    else
        return false;
}

ibinstream& operator<<(ibinstream& m, const eid_t& e) {
    uint64_t value = eid_t2uint(e);
    m << value;
    return m;
}

obinstream& operator>>(obinstream& m, eid_t& e) {
    uint64_t value;
    m >> value;
    uint2eid_t(value, e);
    return m;
}

uint64_t eid_t2uint(const eid_t & eid) {
    uint64_t r = 0;
    r += eid.in_v;
    r <<= VID_BITS;
    r += eid.out_v;
    return r;
}

void uint2eid_t(uint64_t v, eid_t & eid) {
    eid.in_v = v >> VID_BITS;
    eid.out_v = v - (eid.in_v << VID_BITS);
}

bool operator == (const eid_t &p1, const eid_t &p2) {
    if ((p1.in_v == p2.in_v) && (p1.out_v == p2.out_v))
        return true;
    else
        return false;
}

ibinstream& operator<<(ibinstream& m, const vpid_t& vp) {
    uint64_t value = vpid_t2uint(vp);
    m << value;
    return m;
}

obinstream& operator>>(obinstream& m, vpid_t& vp) {
    uint64_t value;
    m >> value;
    uint2vpid_t(value, vp);
    return m;
}

uint64_t vpid_t2uint(const vpid_t & vp) {
    uint64_t r = 0;
    r += vp.vid;
    r <<= VID_BITS;
    r <<= PID_BITS;
    r += vp.pid;
    return r;
}

void uint2vpid_t(uint64_t v, vpid_t & vp) {
    vp.vid = v >> PID_BITS >> VID_BITS;
    vp.pid = v - (vp.vid << PID_BITS << VID_BITS);
}

bool operator ==(const vpid_t &p1, const vpid_t &p2) {
    if ((p1.pid == p2.pid) && (p1.vid == p2.vid))
        return true;
    else
        return false;
}

ibinstream& operator<<(ibinstream& m, const epid_t& ep) {
    uint64_t value = epid_t2uint(ep);
    m << value;
    return m;
}

obinstream& operator>>(obinstream& m, epid_t& ep) {
    uint64_t value;
    m >> value;
    uint2epid_t(value, ep);
    return m;
}

uint64_t epid_t2uint(const epid_t & ep) {
    uint64_t r = 0;
    r += ep.in_vid;
    r <<= VID_BITS;
    r += ep.out_vid;
    r <<= PID_BITS;
    r += ep.pid;
    return r;
}

void uint2epid_t(uint64_t v, epid_t & ep) {
    ep.in_vid = v >> PID_BITS >> VID_BITS;
    ep.out_vid = (v >> PID_BITS) - (ep.in_vid << VID_BITS);
    ep.pid = v - (v >> PID_BITS << PID_BITS);
}

bool operator ==(const epid_t &p1, const epid_t &p2) {
    if ((p1.in_vid == p2.in_vid) && (p1.out_vid == p2.out_vid) && (p1.pid == p2.pid))
        return true;
    else
        return false;
}

ibinstream& operator<<(ibinstream& m, const value_t& v) {
    m << v.content;
    m << v.type;
    return m;
}

obinstream& operator>>(obinstream& m, value_t& v) {
    m >> v.content;
    m >> v.type;
    return m;
}

string kv_pair::DebugString() const {
    stringstream ss;
    ss << "kv_pair: { key = " << key << ", value.type = " << static_cast<int>(value.type) << " }"<< endl;
    return ss.str();
}

ibinstream& operator<<(ibinstream& m, const kv_pair& p) {
    m << p.key;
    m << p.value;
    return m;
}

obinstream& operator>>(obinstream& m, kv_pair& p) {
    m >> p.key;
    m >> p.value;
    return m;
}

ibinstream& operator<<(ibinstream& m, const vp_list& vp) {
    m << vp.vid;
    m << vp.pkeys;
    return m;
}

obinstream& operator>>(obinstream& m, vp_list& vp) {
    m >> vp.vid;
    m >> vp.pkeys;
    return m;
}

ibinstream& operator<<(ibinstream& m, const elem_t& e) {
    vector<char> vec(e.content, e.content+e.sz);
    m << e.type;
    m << vec;
    return m;
}

obinstream& operator>>(obinstream& m, elem_t& e) {
    vector<char> vec;
    m >> e.type;
    m >> vec;
    e.content = new char[vec.size()];
    e.sz = vec.size();
    return m;
}

ibinstream& operator<<(ibinstream& m, const MSG_T& type) {
    int t = static_cast<int>(type);
    m << t;
    return m;
}

obinstream& operator>>(obinstream& m, MSG_T& type) {
    int tmp;
    m >> tmp;
    type = static_cast<MSG_T>(tmp);
    return m;
}

ibinstream& operator<<(ibinstream& m, const ACTOR_T& type) {
    int t = static_cast<int>(type);
    m << t;
    return m;
}

obinstream& operator>>(obinstream& m, ACTOR_T& type) {
    int tmp;
    m >> tmp;
    type = static_cast<ACTOR_T>(tmp);
    return m;
}

void uint2qid_t(uint64_t v, qid_t & qid) {
    qid.id = (v & _8LFLAG);
    qid.trxid = v & _56HFLAG;
}

string value_t::DebugString() const {
    double d;
    int i;
    uint64_t u;
    size_t uint64_sz = sizeof(uint64_t);
    vector<value_t> vec;
    string temp;
    switch (type) {
      case 6:  // v/epid(uint64_t) + {pkey : pvalue} (string)
        u = Tool::value_t2uint64_t(*this);
        return string(content.begin() + uint64_sz, content.end()); 
      case 5:
        u = Tool::value_t2uint64_t(*this);
        return to_string(u);
      case 4:
      case 3:
        return string(content.begin(), content.end());
      case 2:
        d = Tool::value_t2double(*this);
        temp = to_string(d);
        temp.resize(temp.find_last_not_of("0") + 1);
        if (temp.back() == '.')
            temp += "0";
        return temp;
      case 1:
        i = Tool::value_t2int(*this);
        return to_string(i);
      default:
        Tool::value_t2vec(*this, vec);
        temp = "[";
        for (auto& val : vec) {
            temp += val.DebugString() + ", ";
        }
        temp = Tool::trim(temp, ", ");
        temp += "]";
        return temp;
    }
}
