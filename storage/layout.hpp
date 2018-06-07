/*
 * layout.hpp
 *
 *  Created on: May 10, 2018
 *      Author: Hongzhi Chen
 */

#pragma once

#include <cstdint>
#include <vector>

#include "utils/type.hpp"
#include "base/serialization.hpp"

using namespace std;

struct Vertex {
	vid_t id;
	label_t label;
	vector<vid_t> in_nbs;
	vector<vid_t> out_nbs;
	vector<label_t> vp_list;

	friend ibinstream& operator<<(ibinstream& m, const Vertex& v)
	{
		m << v.id;
		m << v.label;
		m << v.in_nbs;
		m << v.out_nbs;
		m << v.vp_list;
		return m;
	}

	friend obinstream& operator>>(obinstream& m, Vertex& v)
	{
		m >> v.id;
		m >> v.label;
		m >> v.in_nbs;
		m >> v.out_nbs;
		m >> v.vp_list;
		return m;
		return m;
	}
};

struct Edge {
//	vid_t v_1;
//	vid_t v_2;
	eid_t id;
	label_t label;
	vector<label_t> ep_list;

	friend ibinstream& operator<<(ibinstream& m, const Edge& e)
	{
		m << e.id;
		m << e.label;
		m << e.ep_list;
		return m;
	}

	friend obinstream& operator>>(obinstream& m, Edge& e)
	{
		m >> e.id;
		m >> e.label;
		m >> e.ep_list;
		return m;
	}
};

struct V_KVpair {
	vpid_t key;
	value_t value;

	friend ibinstream& operator<<(ibinstream& m, const V_KVpair& pair)
	{
		m << pair.key;
		m << pair.value;
		return m;
	}

	friend obinstream& operator>>(obinstream& m, V_KVpair& pair)
	{
		m >> pair.key;
		m >> pair.value;
		return m;
	}
};

struct VProperty{
	vid_t id;
	vector<V_KVpair> plist;

	friend ibinstream& operator<<(ibinstream& m, const VProperty& vp)
	{
		m << vp.id;
		m << pair.plist;
		return m;
	}

	friend obinstream& operator>>(obinstream& m, VProperty& vp)
	{
		m >> vp.id;
		m >> pair.plist;
		return m;
	}
};

struct E_KVpair {
	epid_t key;
	value_t value;

	friend ibinstream& operator<<(ibinstream& m, const E_KVpair& pair)
	{
		m << pair.key;
		m << pair.value;
		return m;
	}

	friend obinstream& operator>>(obinstream& m, E_KVpair& pair)
	{
		m >> pair.key;
		m >> pair.value;
		return m;
	}
};

struct EProperty {
//	vid_t v_1;
//	vid_t v_2;
	eid_t id;
	vector<E_KVpair> plist;

	friend ibinstream& operator<<(ibinstream& m, const EProperty& ep)
	{
		m << ep.id;
		m << ep.plist;
		return m;
	}

	friend obinstream& operator>>(obinstream& m, EProperty& ep)
	{
		m >> ep.id;
		m >> ep.plist;
		return m;
	}
};
