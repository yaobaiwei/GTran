/*
 * type.hpp
 *
 *  Created on: May 18, 2018
 *      Author: Hongzhi Chen
 */

#ifndef TYPE_HPP_
#define TYPE_HPP_

#include <stdint.h>
#include <unordered_map>
#include <string.h>
#include <sstream>
#include <ext/hash_map>
#include <ext/hash_set>

#include "utils/mymath.hpp"
#include "base/serialization.hpp"


using __gnu_cxx::hash_map;
using __gnu_cxx::hash_set;

using namespace std;

// 64-bit internal pointer (size < 256M and off < 64GB)
enum { NBITS_SIZE = 28 };
enum { NBITS_PTR = 36 };

static uint64_t _28LFLAG = 0xFFFFFFF;
static uint64_t _12LFLAG = 0xFFF;

struct ptr_t {
	uint64_t size: NBITS_SIZE;
	uint64_t off: NBITS_PTR;

	ptr_t(): size(0), off(0) { }

	ptr_t(uint64_t s, uint64_t o): size(s), off(o) {
        assert ((size == s) && (off == o));
    }

    bool operator == (const ptr_t &ptr) {
        if ((size == ptr.size) && (off == ptr.off))
            return true;
        return false;
    }

    uint64_t value(){
        uint64_t r = 0;
        r += off;
        r <<= NBITS_SIZE;
        r += size;
        return r;
    }

    uint64_t hash() {
        return mymath::hash_u64(value()); // the standard hash is too slow (i.e., std::hash<uint64_t>()(r))
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

	ikey_t(){
	    pid = 0;
	}

    ikey_t(uint64_t _pid, ptr_t _ptr){
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

enum {VID_BITS = 26}; // <32; the total # of vertices should no be more than 2^26
enum {EID_BITS = (VID_BITS * 2)}; //eid = v1_id | v2_id (52 bits)
enum {PID_BITS = (64 - EID_BITS)}; //12, the total # of property should no be more than 2^PID_BITS

//vid: 32bits 0000|00-vid
struct vid_t {
	uint32_t vid: VID_BITS;

	vid_t(): vid(0){ };
	vid_t(int _vid): vid(_vid){ }

    bool operator == (const vid_t & vid1) {
        if ((vid == vid1.vid))
            return true;
        return false;
    }

    vid_t& operator =(int i)
	{
    	this->vid = i;
		return *this;
	}

    uint32_t value(){
    	return (uint32_t)vid;
    }

    uint64_t hash() {
        uint64_t r = value();
        return mymath::hash_u64(r); // the standard hash is too slow (i.e., std::hash<uint64_t>()(r))
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
			//TODO
			int key = (int)vid.vid;
			size_t seed = 0;
			mymath::hash_combine(seed, key);
			return seed;
		}
	};
}

//vid: 64bits 0000|0000|0000|in_v|out_v
struct eid_t {
uint64_t in_v : VID_BITS;
uint64_t out_v : VID_BITS;

	eid_t(): in_v(0), out_v(0) { }

	eid_t(int _in_v, int _out_v): in_v(_in_v), out_v(_out_v) {
        assert((in_v == _in_v) && (out_v == _out_v) ); // no key truncate
    }

    bool operator == (const eid_t &eid) {
        if ((in_v == eid.in_v) && (out_v == eid.out_v))
            return true;
        return false;
    }

    uint64_t value(){
        uint64_t r = 0;
        r += in_v;
        r <<= VID_BITS;
        r += out_v;
        return r;
    }

    uint64_t hash() {
        return mymath::hash_u64(value()); // the standard hash is too slow (i.e., std::hash<uint64_t>()(r))
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
			//TODO
			int k1 = (int)eid.in_v;
			int k2 = (int)eid.out_v;
			size_t seed = 0;
			mymath::hash_combine(seed, k1);
			mymath::hash_combine(seed, k2);
			return seed;
		}
	};
}

//vpid: 64bits  vid|0x26|pid
struct vpid_t {
uint64_t vid : VID_BITS;
uint64_t pid : PID_BITS;

	vpid_t(): vid(0), pid(0) { }

	vpid_t(int _vid, int _pid): vid(_vid), pid(_pid) {
        assert((vid == _vid) && (pid == _pid) ); // no key truncate
    }

	vpid_t(vid_t _vid, int _pid): pid(_pid) {
		vid = _vid.vid;
        assert((vid == _vid.vid) && (pid == _pid) ); // no key truncate
    }

    bool operator == (const vpid_t &vpid) {
        if ((vid == vpid.vid) && (pid == vpid.pid))
            return true;
        return false;
    }

    uint64_t value(){
        uint64_t r = 0;
        r += vid;
        r <<= VID_BITS;
        r <<= PID_BITS;
        r += pid;
        return r;
    }

    uint64_t hash() {
        return mymath::hash_u64(value()); // the standard hash is too slow (i.e., std::hash<uint64_t>()(r))
    }
};

ibinstream& operator<<(ibinstream& m, const vpid_t& vp);

obinstream& operator>>(obinstream& m, vpid_t& vp);

uint64_t vpid_t2uint(const vpid_t & vp);

void uint2vpid_t(uint64_t v, vpid_t & vp);

bool operator == (const vpid_t &p1, const vpid_t &p2);

//vpid: 64bits  v_in|v_out|pid
struct epid_t {
uint64_t in_vid : VID_BITS;
uint64_t out_vid : VID_BITS;
uint64_t pid : PID_BITS;

	epid_t(): in_vid(0), out_vid(0), pid(0) { }

	epid_t(eid_t _eid, int _pid): pid(_pid) {
		in_vid = _eid.in_v;
		out_vid = _eid.out_v;
        assert((in_vid == _eid.in_v) && (out_vid == _eid.out_v) && (pid == _pid) ); // no key truncate
    }

	epid_t(int _in_v, int _out_v, int _pid): in_vid(_in_v), out_vid(_out_v), pid(_pid) {
		assert((in_vid == _in_v) && (out_vid == _out_v) && (pid == _pid) ); // no key truncate
    }

    bool operator == (const epid_t &epid) {
        if ((in_vid == epid.in_vid) && (out_vid == epid.out_vid) && (pid == epid.pid))
            return true;
        return false;
    }

    uint64_t value(){
        uint64_t r = 0;
        r += in_vid;
        r <<= VID_BITS;
        r += out_vid;
        r <<= PID_BITS;
        r += pid;
        return r;
    }

    uint64_t hash() {
        return mymath::hash_u64(value()); // the standard hash is too slow (i.e., std::hash<uint64_t>()(r))
    }
};

ibinstream& operator<<(ibinstream& m, const epid_t& ep);

obinstream& operator>>(obinstream& m, epid_t& ep);

uint64_t epid_t2uint(const epid_t & ep);

void uint2epid_t(uint64_t v, epid_t & ep);

bool operator == (const epid_t &p1, const epid_t &p2);

typedef uint16_t label_t;

//type
//1->int, 2->double, 3->char, 4->string, 5->uint64_t
struct value_t {
	uint8_t type;
	vector<char> content;
	string DebugString() const ;
};

ibinstream& operator<<(ibinstream& m, const value_t& v);

obinstream& operator>>(obinstream& m, value_t& v);

struct kv_pair {
	uint32_t key;
	value_t value;
	string DebugString() const ;
};

ibinstream& operator<<(ibinstream& m, const kv_pair& p);

obinstream& operator>>(obinstream& m, kv_pair& p);

struct vp_list{
	vid_t vid;
	vector<label_t> pkeys;
};

ibinstream& operator<<(ibinstream& m, const vp_list& vp);

obinstream& operator>>(obinstream& m, vp_list& vp);

//the return value from kv_store
//type
//1->int, 2->double, 3->char, 4->string
struct elem_t{
	elem_t():sz(0), type(0), content(NULL){}
	uint8_t type;
	uint32_t sz;
	char * content;
};

ibinstream& operator<<(ibinstream& m, const elem_t& e);

obinstream& operator>>(obinstream& m, elem_t& e);

struct string_index{
	unordered_map<string, label_t> str2el; //map to edge_label
	unordered_map<label_t, string> el2str;
	unordered_map<string, label_t> str2epk; //map to edge's property key
	unordered_map<label_t, string> epk2str;
	unordered_map<string, label_t> str2vl; //map to vtx_label
	unordered_map<label_t, string> vl2str;
	unordered_map<string, label_t> str2vpk; //map to vtx's property key
	unordered_map<label_t, string> vpk2str;
};

// Spawn: spawn a new actor
// Feed: "proxy" feed actor a input
// Reply: actor returns the intermidiate result to actor
enum class MSG_T : char { INIT, SPAWN, FEED, REPLY, BARRIER, BRANCH,EXIT };
static const char *MsgType[] = {"init", "spawn", "feed", "reply", "barrier", "branch", "exit"};

ibinstream& operator<<(ibinstream& m, const MSG_T& type);

obinstream& operator>>(obinstream& m, MSG_T& type);

enum class ACTOR_T : char {
	HW, INIT, AGGREGATE, AS, BRANCH, BRANCHFILTER, CAP, COIN, COUNT, DEDUP, GROUP, HAS,
	HASLABEL, IS, KEY, LABEL, LOOPS, MATH, ORDER, PROJECTION, PROPERTY, RANGE, REDIRECT, REPEAT, SELECT, TRAVERSAL, VALUES, WHERE, END
};

static const char *ActorType[] = { "HW", "INIT", "AGGREGATE", "AS", "BRANCH", "BRANCHFILTER", "CAP", "COIN", "COUNT", "DEDUP", "GROUP", "HAS",
"HASLABEL", "IS", "KEY", "LABEL", "LOOPS", "MATH", "ORDER", "PROJECTION", "PROPERTY", "RANGE", "REDIRECT", "REPEAT", "SELECT", "TRAVERSAL", "VALUES", "WHERE", "END"};

ibinstream& operator<<(ibinstream& m, const ACTOR_T& type);

obinstream& operator>>(obinstream& m, ACTOR_T& type);

// Enums for actors
enum Branch_T { UNION, COALESCE, CHOOSE };
enum Filter_T{ AND, OR, NOT };
enum Math_T { SUM, MAX, MIN, MEAN };
enum Element_T{ VERTEX, EDGE };
enum Direction_T{ IN, OUT, BOTH };
enum Predicate_T{ ANY, NONE, EQ, NEQ, LT, LTE, GT, GTE, INSDIE, OUTSIDE, BETWEEN, WITHIN, WITHOUT };

struct qid_t{
	uint32_t nid;
	uint32_t qr;

	qid_t(): nid(0), qr(0) {}

	qid_t(uint32_t _nid, uint32_t _qr): nid(_nid), qr(_qr){}

	bool operator == (const qid_t & qid) {
		if ((nid == qid.nid) && (qr == qid.qr))
			return true;
		return false;
	}

	uint64_t value(){
		uint64_t r = 0;
		r += qr;
		r <<= 32;
		r += nid;
		return r;
	}
};
#endif /* TYPE_HPP_ */
