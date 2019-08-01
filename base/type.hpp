/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Hongzhi Chen (hzchen@cse.cuhk.edu.hk)
*/

#pragma once

#include <string.h>
#include <stdint.h>
#include <tbb/concurrent_hash_map.h>
#include <ext/hash_map>
#include <ext/hash_set>
#include <sstream>
#include <unordered_map>
#include <string>
#include <tuple>
#include <vector>

#include "utils/mymath.hpp"
#include "base/serialization.hpp"


using __gnu_cxx::hash_map;
using __gnu_cxx::hash_set;

using namespace std;

// 64-bit internal pointer (size < 256M and off < 64GB)
enum { NBITS_SIZE = 28 };
enum { NBITS_PTR = 36 };

#define QID_BITS sizeof(uint8_t)*8

static uint64_t _56HFLAG = 0xFFFFFFFFFFFFFF00;
static uint64_t _56LFLAG = 0xFFFFFFFFFFFFFF;
static uint64_t _32LFLAG = 0xFFFFFFFF;
static uint64_t _28LFLAG = 0xFFFFFFF;
static uint64_t _12LFLAG = 0xFFF;
static uint64_t _8LFLAG  = 0xFF;

enum class TRX_STAT {PROCESSING, VALIDATING, ABORT, COMMITTED};
enum class READ_STAT {ABORT, NOTFOUND, SUCCESS};
enum class PROCESS_STAT {
    SUCCESS,
    ABORT,
    ABORT_DROP_V_GET_CONN_E,
    ABORT_DROP_V_APPEND,
    ABORT_ADD_E_INVISIBLE_V,
    ABORT_ADD_E_APPEND,
    ABORT_DROP_E_APPEND,
    ABORT_MODIFY_VP_INVISIBLE_V,
    ABORT_MODIFY_VP_APPEND,
    ABORT_MODIFY_EP_EITEM,
    ABORT_MODIFY_EP_DELETED_E,
    ABORT_MODIFY_EP_MODIFY,
    ABORT_DROP_VP_INVISIBLE_V,
    ABORT_DROP_VP_DROP,
    ABORT_DROP_EP_EITEM,
    ABORT_DROP_EP_DELETED_E,
    ABORT_DROP_EP_DROP,
};

enum class TIMESTAMP_TYPE {BEGIN_TIME, COMMIT_TIME, FINISH_TIME};

enum class NOTIFICATION_TYPE {
    UPDATE_STATUS,
    RCT_TIDS,
    QUERY_RCT,
};

template <class EnumClass>
struct EnumClassHash {
    size_t operator() (const EnumClass& v) const {
        return hash<int>()((int)(v));
    }
    bool operator() (const EnumClass& a, const EnumClass& b) const{
        return a == b;
    }
};

extern const unordered_map<PROCESS_STAT, string, EnumClassHash<PROCESS_STAT>> abort_reason_map;
extern const unordered_map<TRX_STAT, string, EnumClassHash<TRX_STAT>> trx_stat_str_map;

struct ptr_t {
    uint64_t size: NBITS_SIZE;
    uint64_t off: NBITS_PTR;

    ptr_t(): size(0), off(0) { }

    ptr_t(uint64_t s, uint64_t o): size(s), off(o) {
        assert((size == s) && (off == o));
    }

    bool operator == (const ptr_t &ptr) {
        if ((size == ptr.size) && (off == ptr.off))
            return true;
        return false;
    }

    uint64_t value() const {
        uint64_t r = 0;
        r += off;
        r <<= NBITS_SIZE;
        r += size;
        return r;
    }

    uint64_t hash() {
        return mymath::hash_u64(value());  // the standard hash is too slow (i.e., std::hash<uint64_t>()(r))
    }
};

ibinstream& operator<<(ibinstream& m, const ptr_t& p);

obinstream& operator>>(obinstream& m, ptr_t& p);

uint64_t ptr_t2uint(const ptr_t & p);

void uint2ptr_t(uint64_t v, ptr_t & p);

bool operator == (const ptr_t &p1, const ptr_t &p2);


struct ikey_t {
    uint64_t pid;
    ptr_t ptr;

    ikey_t() {
        pid = 0;
    }

    ikey_t(uint64_t _pid, ptr_t _ptr) {
        pid = _pid;
        ptr = _ptr;
    }

    bool operator == (const ikey_t &key) {
        if (pid == key.pid)
            return true;
        return false;
    }

    bool is_empty() { return pid == 0; }
};

ibinstream& operator<<(ibinstream& m, const ikey_t& p);

obinstream& operator>>(obinstream& m, ikey_t& p);

bool operator == (const ikey_t &p1, const ikey_t &p2);

enum {VID_BITS = 26};  // <32; the total # of vertices should no be more than 2^26
enum {EID_BITS = (VID_BITS * 2)};  // eid = v1_id | v2_id (52 bits)
enum {PID_BITS = (64 - EID_BITS)};  // 12, the total # of property should no be more than 2^PID_BITS

// vid: 32bits 0000|00-vid
struct vid_t {
    uint32_t vid: VID_BITS;

    vid_t(): vid(0) {}
    vid_t(int _vid): vid(_vid) {}

    bool operator == (const vid_t & vid1) {
        if ((vid == vid1.vid))
            return true;
        return false;
    }

    vid_t& operator =(int i) {
        this->vid = i;
        return *this;
    }

    bool operator<(const vid_t& other) const {
        return (this->vid < other.vid);
    }

    uint32_t value() const {
        return (uint32_t)vid;
    }

    uint64_t validation_value() {
        return (uint64_t)vid;
    }

    uint64_t hash() {
        uint64_t r = value();
        return mymath::hash_u64(r);  // the standard hash is too slow (i.e., std::hash<uint64_t>()(r))
    }
};

ibinstream& operator<<(ibinstream& m, const vid_t& v);

obinstream& operator>>(obinstream& m, vid_t& v);

uint32_t vid_t2uint(const vid_t & vid);

void uint2vid_t(uint32_t v, vid_t & vid);

bool operator == (const vid_t &p1, const vid_t &p2);

namespace __gnu_cxx {
template <>
struct hash<vid_t> {
    size_t operator()(const vid_t& vid) const {
        int key = static_cast<int>(vid.vid);
        size_t seed = 0;
        mymath::hash_combine(seed, key);
        return seed;
    }
};
}  // namespace __gnu_cxx

// vid: 64bits 0000|0000|0000|in_v|out_v
struct eid_t {
uint64_t in_v : VID_BITS;  // dst_v
uint64_t out_v : VID_BITS;  // src_v

    eid_t(): in_v(0), out_v(0) { }

    eid_t(int _in_v, int _out_v): in_v(_in_v), out_v(_out_v) {
        assert((in_v == _in_v) && (out_v == _out_v) );  // no key truncate
    }

    bool operator == (const eid_t &eid) {
        if ((in_v == eid.in_v) && (out_v == eid.out_v))
            return true;
        return false;
    }

    bool operator<(const eid_t& other) const {
        return (this->value() < other.value());
    }

    uint64_t value() const {
        uint64_t r = 0;
        r += in_v;
        r <<= VID_BITS;
        r += out_v;
        return r;
    }

    uint64_t validation_value() {
        return value();
    }

    uint64_t hash() {
        return mymath::hash_u64(value());  // the standard hash is too slow (i.e., std::hash<uint64_t>()(r))
    }
};

ibinstream& operator<<(ibinstream& m, const eid_t& e);

obinstream& operator>>(obinstream& m, eid_t& e);

uint64_t eid_t2uint(const eid_t & eid);

void uint2eid_t(uint64_t v, eid_t & eid);

bool operator == (const eid_t &p1, const eid_t &p2);

namespace __gnu_cxx {
template <>
struct hash<eid_t> {
    size_t operator()(const eid_t& eid) const {
        int k1 = static_cast<int>(eid.in_v);
        int k2 = static_cast<int>(eid.out_v);
        size_t seed = 0;
        mymath::hash_combine(seed, k1);
        mymath::hash_combine(seed, k2);
        return seed;
    }
};
}  // namespace __gnu_cxx

// vpid: 64bits  vid|0x26|pid
struct vpid_t {
uint64_t vid : VID_BITS;
uint64_t pid : PID_BITS;

    vpid_t(): vid(0), pid(0) { }

    vpid_t(int _vid, int _pid): vid(_vid), pid(_pid) {
        assert((vid == _vid) && (pid == _pid) );  // no key truncate
    }

    vpid_t(vid_t _vid, int _pid): pid(_pid) {
        vid = _vid.vid;
        assert((vid == _vid.vid) && (pid == _pid));  // no key truncate
    }

    bool operator == (const vpid_t &vpid) {
        if ((vid == vpid.vid) && (pid == vpid.pid))
            return true;
        return false;
    }

    uint64_t value() const {
        uint64_t r = 0;
        r += vid;
        r <<= VID_BITS;
        r <<= PID_BITS;
        r += pid;
        return r;
    }

    uint64_t validation_value() {
        uint64_t r = 0;
        r += vid;
        return r;
    }

    uint64_t hash() {
        return mymath::hash_u64(value());  // the standard hash is too slow (i.e., std::hash<uint64_t>()(r))
    }
};

ibinstream& operator<<(ibinstream& m, const vpid_t& vp);

obinstream& operator>>(obinstream& m, vpid_t& vp);

uint64_t vpid_t2uint(const vpid_t & vp);

void uint2vpid_t(uint64_t v, vpid_t & vp);

bool operator == (const vpid_t &p1, const vpid_t &p2);

// vpid: 64bits  v_in|v_out|pid
struct epid_t {
uint64_t in_vid : VID_BITS;
uint64_t out_vid : VID_BITS;
uint64_t pid : PID_BITS;

    epid_t(): in_vid(0), out_vid(0), pid(0) { }

    epid_t(eid_t _eid, int _pid): pid(_pid) {
        in_vid = _eid.in_v;
        out_vid = _eid.out_v;
        assert((in_vid == _eid.in_v) && (out_vid == _eid.out_v) && (pid == _pid) );  // no key truncate
    }

    epid_t(int _in_v, int _out_v, int _pid): in_vid(_in_v), out_vid(_out_v), pid(_pid) {
        assert((in_vid == _in_v) && (out_vid == _out_v) && (pid == _pid) );  // no key truncate
    }

    bool operator == (const epid_t &epid) {
        if ((in_vid == epid.in_vid) && (out_vid == epid.out_vid) && (pid == epid.pid))
            return true;
        return false;
    }

    uint64_t value() const {
        uint64_t r = 0;
        r += in_vid;
        r <<= VID_BITS;
        r += out_vid;
        r <<= PID_BITS;
        r += pid;
        return r;
    }

    uint64_t validation_value() {
        uint64_t r = 0;
        r += in_vid;
        r <<= VID_BITS;
        r += out_vid;
        return r;
    }

    uint64_t hash() {
        return mymath::hash_u64(value());  // the standard hash is too slow (i.e., std::hash<uint64_t>()(r))
    }
};

ibinstream& operator<<(ibinstream& m, const epid_t& ep);

obinstream& operator>>(obinstream& m, epid_t& ep);

uint64_t epid_t2uint(const epid_t & ep);

void uint2epid_t(uint64_t v, epid_t & ep);

bool operator == (const epid_t &p1, const epid_t &p2);

typedef uint16_t label_t;

// type
// 1->int, 2->double, 3->char, 4->string, 5->uint64_t
struct value_t {
    uint8_t type;
    vector<char> content;
    string DebugString() const;
    bool empty = false;

    value_t() {
        empty = true;
    }

    bool isEmpty() {
        return empty;
    }
};

static uint8_t IntValueType = 1;
static uint8_t DoubleValueType = 2;
static uint8_t CharValueType = 3;
static uint8_t StringValueType = 4;
static uint8_t UintValueType = 5;
static uint8_t PropKeyValueType = 6;

ibinstream& operator<<(ibinstream& m, const value_t& v);

obinstream& operator>>(obinstream& m, value_t& v);

struct kv_pair {
    uint32_t key;
    value_t value;
    string DebugString() const;
};

ibinstream& operator<<(ibinstream& m, const kv_pair& p);

obinstream& operator>>(obinstream& m, kv_pair& p);

struct vp_list{
    vid_t vid;
    vector<label_t> pkeys;
};

ibinstream& operator<<(ibinstream& m, const vp_list& vp);

obinstream& operator>>(obinstream& m, vp_list& vp);

// the return value from kv_store
// type
// 1->int, 2->double, 3->char, 4->string
struct elem_t {
    elem_t():sz(0), type(0), content(NULL) {  }
    uint8_t type;
    uint32_t sz;
    char * content;
};

ibinstream& operator<<(ibinstream& m, const elem_t& e);

obinstream& operator>>(obinstream& m, elem_t& e);

struct string_index{
    unordered_map<string, label_t> str2el;  // map to edge_label
    unordered_map<label_t, string> el2str;
    unordered_map<string, label_t> str2epk;  // map to edge's property key
    unordered_map<label_t, string> epk2str;
    unordered_map<string, uint8_t> str2eptype;
    unordered_map<string, label_t> str2vl;  // map to vtx_label
    unordered_map<label_t, string> vl2str;
    unordered_map<string, label_t> str2vpk;  // map to vtx's property key
    unordered_map<label_t, string> vpk2str;
    unordered_map<string, uint8_t> str2vptype;
};

enum Index_T { E_LABEL, E_PROPERTY, V_LABEL, V_PROPERTY };

// Spawn: spawn a new actor
// Feed: "proxy" feed actor a input
// Reply: actor returns the intermidiate result to actor
enum class MSG_T : char { INIT, SPAWN, FEED, REPLY, BARRIER, BRANCH, EXIT, VALIDATION, COMMIT, ABORT };
static const char *MsgType[] = {"init", "spawn", "feed", "reply", "barrier", "branch", "exit", "validation", "commit", "abort"};

ibinstream& operator<<(ibinstream& m, const MSG_T& type);

obinstream& operator>>(obinstream& m, MSG_T& type);

enum class ACTOR_T : char {
    INIT, ADDE, ADDV, AGGREGATE, AS, BRANCH, BRANCHFILTER, CAP, CONFIG, COUNT, DEDUP, DROP,
    GROUP, HAS, HASLABEL, INDEX, IS, KEY, LABEL, MATH, ORDER, POSTVALIDATION, PROJECT, PROPERTIES, PROPERTY, RANGE,
    SELECT, TRAVERSAL, VALUES, WHERE, COIN, REPEAT, END, VALIDATION, COMMIT
};

static const char *ActorType[] = {
    "INIT", "ADDE", "ADDV", "AGGREGATE", "AS", "BRANCH", "BRANCHFILTER", "CAP", "CONFIG", "COUNT", "DEDUP",
    "DROP", "GROUP", "HAS", "HASLABEL", "INDEX", "IS", "KEY", "LABEL", "MATH", "ORDER", "POSTVALIDATION",
    "PROJECT", "PROPERTIES", "PROPERTY", "RANGE", "SELECT", "TRAVERSAL", "VALUES", "WHERE" , "COIN", "REPEAT",
    "END", "VALIDATION", "COMMIT"
};

enum class Step_T{
    IN, OUT, BOTH, INE, OUTE, BOTHE, INV, OUTV, BOTHV, ADDE, ADDV, AND, AGGREGATE, AS, CAP, COUNT, DEDUP,
    DROP, FROM, GROUP, GROUPCOUNT, HAS, HASLABEL, HASKEY, HASVALUE, HASNOT, IS, KEY, LABEL, LIMIT, MAX,
    MEAN, MIN, NOT, OR, ORDER, PROPERTIES, PROPERTY, RANGE, SELECT, SKIP, SUM, TO, UNION, VALUES, WHERE, COIN, REPEAT
};

ibinstream& operator<<(ibinstream& m, const ACTOR_T& type);

obinstream& operator>>(obinstream& m, ACTOR_T& type);

// Enums for actors
enum Filter_T{ AND, OR, NOT };
enum Math_T { SUM, MAX, MIN, MEAN };
enum Element_T{ VERTEX, EDGE };
enum Direction_T{ IN, OUT, BOTH };
enum Order_T {INCR, DECR};
enum Predicate_T{ ANY, NONE, EQ, NEQ, LT, LTE, GT, GTE, INSIDE, OUTSIDE, BETWEEN, WITHIN, WITHOUT };

struct qid_t{
    uint64_t trxid;  // lower 8 bits = 0, reserved for query index
    uint8_t id;

    qid_t(): trxid(0), id(0) {}

    qid_t(uint64_t _trxid, uint8_t _id): trxid(_trxid), id(_id) {  }

    bool operator == (const qid_t & qid) {
        if ((trxid == qid.trxid) && (id == qid.id))
            return true;
        return false;
    }

    uint64_t value() {
        return trxid + id;
    }
};

void uint2qid_t(uint64_t v, qid_t & qid);

// msg key for branch and barrier actors
struct mkey_t {
    uint64_t qid;    // qid
    uint64_t mid;    // msg id of branch parent
    int index;        // index of branch
    mkey_t() : qid(0), index(0), mid(0) {  }
    mkey_t(uint64_t qid_, uint64_t mid_, int index_) : qid(qid_), index(index_), mid(mid_) {  }

    bool operator==(const mkey_t& key) const {
        if ((qid == key.qid) && (mid == key.mid) && (index == key.index)) {
            return true;
        }
        return false;
    }

    bool operator<(const mkey_t& key) const {
        if (qid < key.qid) {
            return true;
        } else if (qid == key.qid) {
            if (mid < key.mid) {
                return true;
            } else if (mid == key.mid) {
                return index < key.index;
            } else {
                return false;
            }
        } else {
            return false;
        }
    }
};

// Provide hash function for tbb hash_map
struct MkeyHashCompare {
    static size_t hash(const mkey_t& x) {
        size_t value = mymath::hash_u128_to_u64(x.qid, x.mid);
        mymath::hash_combine(value, x.index);
        return value;
    }
    //! True if strings are equal
    static bool equal(const mkey_t& x, const mkey_t& y) {
        return x == y;
    }
};

// Aggregate data Key
//  trxid | side_effect_key
//  56  |        8
struct agg_t {
    uint64_t trxid;
    uint8_t label_key;

    agg_t() : trxid(0), label_key(0) {}

    agg_t(uint64_t qid_value, int _label_key) : label_key(_label_key) {
        trxid = qid_value & _56HFLAG;
    }

    bool operator==(const agg_t & key) const {
        if ((trxid == key.trxid) && (label_key == key.label_key)) {
            return true;
        }
        return false;
    }

    uint64_t value() {
        return trxid + label_key;
    }

    uint64_t hash() {
        return mymath::hash_u64(value());
    }
};

namespace std {
template <>
struct hash<agg_t> {
    size_t operator()(const agg_t& key) const {
        uint64_t k1 = static_cast<int>(key.trxid);
        int k2 = static_cast<int>(key.label_key);
        size_t seed = 0;
        mymath::hash_combine(seed, k1);
        mymath::hash_combine(seed, k2);
        return seed;
    }
};
}  // namespace std

// Thread_division for actors
// Cache_Sequential : LabelActor, HasLabelActor, PropertiesActor, ValuesActor, HasActor, KeyActor [1/4 #threads]
// Cache_Barrier : GroupActor, OrderActor [1/6 #threads]
// TRAVERSAL : TraversalActor [1/12 #threads]
// Normal_Barrier : CountActor, AggregateActor, CapActor, DedupActor, MathActor [1/6 #threads]
// Normal_Branch : RangeActor, CoinActor, BranchFilterActor, BranchActor [1/6 #threads]
// Normal_Sequential : AsActor, SelectActor, WhereActor, IsActor [1/6 #threads]
enum ActorDivisionType { CACHE_SEQ, CACHE_BARR, TRAVERSAL, NORMAL_BARR, NORMAL_BRANCH, NORMAL_SEQ };
enum ResidentThread_T { MAIN, RECVREQ, SENDQUERY, MONITOR };

typedef tuple<uint64_t, int, Element_T> rct_extract_data_t;
static const int NUM_THREAD_DIVISION = 6;
static const int NUM_RESIDENT_THREAD = 4;

// ====For Validation=====

// COUNT in Primitive_T is used for iterating enum and do not assign any
// value to each element.
enum class Primitive_T { IV, IE, DV, DE, IVP, IEP, DVP, DEP, MVP, MEP, COUNT };
enum ID_T { VID, EID, VPID, EPID };

struct PrimitiveEnumClassHash {
    std::size_t operator()(Primitive_T t) const {
        return static_cast<std::size_t>(t);
    }
    static bool equal(const Primitive_T& t1, const Primitive_T& t2) {
        return t1 == t2;
    }
};

// For Modification (AddE)
//  PlaceHolder, AsLabel, NotApplicable
enum AddEdgeMethodType { PlaceHolder, StepLabel, NotApplicable };

// For GCTask
enum class JobType { EraseV, EraseOutE, EraseInE, VMVCCGC, VPMVCCGC, EPMVCCGC, EMVCCGC, TopoIndexGC,
    PropIndexGC, RCTGC, TrxStatusTableGC, TopoRowGC, TopoRowDefrag, VPRowGC, VPRowDefrag, EPRowGC, EPRowDefrag };
