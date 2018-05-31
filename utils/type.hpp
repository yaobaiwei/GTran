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
#include <ext/hash_map>
#include <ext/hash_set>
#include "utils/mymath.hpp"

using __gnu_cxx::hash_map;
using __gnu_cxx::hash_set;

using namespace std;

// 64-bit internal pointer (size < 256M and off < 64GB)
enum { NBITS_SIZE = 28 };
enum { NBITS_PTR = 36 };

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

    bool operator != (const ptr_t &ptr) {
        return !(operator == (ptr));
    }
};

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

    bool operator != (const vid_t &vid1) {
        return !(operator == (vid1));
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

    bool operator != (const eid_t &eid) { return !(operator == (eid)); }

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

    bool operator == (const vpid_t &vpid) {
        if ((vid == vpid.vid) && (pid == vpid.pid))
            return true;
        return false;
    }

    bool operator != (const vpid_t &vpid) { return !(operator == (vpid)); }

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

    bool operator != (const epid_t &epid) { return !(operator == (epid)); }

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

typedef uint16_t label_t;

//type
//1->int, 2->double, 3->char, 4->string
struct value_t {
	vector<char> content;
	uint8_t type;
};

struct kv_pair {
	int key;
	value_t value;
};

//the return value from kv_store
//type
//1->int, 2->double, 3->char, 4->string
struct elem_t{
	uint8_t type;
	uint32_t sz;
	char * content;
};

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
enum class MSG_T : char { SPAWN, FEED, REPLY, BARRIER, EXIT };
static const char *TypeName[] = {"spawn", "feed", "reply", "barrier", "exit"};

enum class ACTOR_T : char { ADD, PROXY, HW, OUT };
static const char *ActorType[] = {"add", "proxy", "hello world", ""};

#endif /* TYPE_HPP_ */
