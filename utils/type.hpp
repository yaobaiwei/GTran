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
enum {PID_BITS = (64 - EID_BITS)}; //the total # of property should no be more than 2^PID_BITS

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
};

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

    uint64_t hash() {
        uint64_t r = 0;
        r += in_v;
        r <<= VID_BITS;
        r += out_v;
        return mymath::hash_u64(r); // the standard hash is too slow (i.e., std::hash<uint64_t>()(r))
    }
};

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

    uint64_t hash() {
        uint64_t r = 0;
        r += vid;
        r <<= VID_BITS;
        r <<= VID_BITS; //twice
        r += pid;
        return mymath::hash_u64(r); // the standard hash is too slow (i.e., std::hash<uint64_t>()(r))
    }
};

struct epid_t {
uint64_t eid : EID_BITS;
uint64_t pid : PID_BITS;

	epid_t(): eid(0), pid(0) { }

	epid_t(int _eid, int _pid): eid(_eid), pid(_pid) {
        assert((eid == _eid) && (pid == _pid) ); // no key truncate
    }

    bool operator == (const epid_t &epid) {
        if ((eid == epid.eid) && (pid == epid.pid))
            return true;
        return false;
    }

    bool operator != (const epid_t &epid) { return !(operator == (epid)); }

    uint64_t hash() {
        uint64_t r = 0;
        r += eid;
        r <<= EID_BITS;
        r += pid;
        return mymath::hash_u64(r); // the standard hash is too slow (i.e., std::hash<uint64_t>()(r))
    }
};

typedef uint8_t label_t;
//typedef uint32_t value_t;

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

#endif /* TYPE_HPP_ */
