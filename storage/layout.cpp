/*
 * layout.cpp
 *
 *  Created on: Jun 8, 2018
 *      Author: Hongzhi Chen
 */

#include "storage/layout.hpp"

ibinstream& operator<<(ibinstream& m, const Vertex& v)
{
	m << v.id;
	m << v.label;
	m << v.in_nbs;
	m << v.out_nbs;
	m << v.vp_list;
	return m;
}

obinstream& operator>>(obinstream& m, Vertex& v)
{
	m >> v.id;
	m >> v.label;
	m >> v.in_nbs;
	m >> v.out_nbs;
	m >> v.vp_list;
	return m;
	return m;
}

ibinstream& operator<<(ibinstream& m, const Edge& e)
{
	m << e.id;
	m << e.label;
	m << e.ep_list;
	return m;
}

obinstream& operator>>(obinstream& m, Edge& e)
{
	m >> e.id;
	m >> e.label;
	m >> e.ep_list;
	return m;
}

ibinstream& operator<<(ibinstream& m, const V_KVpair& pair)
{
	m << pair.key;
	m << pair.value;
	return m;
}

obinstream& operator>>(obinstream& m, V_KVpair& pair)
{
	m >> pair.key;
	m >> pair.value;
	return m;
}

ibinstream& operator<<(ibinstream& m, const VProperty& vp)
{
	m << vp.id;
	m << vp.plist;
	return m;
}

obinstream& operator>>(obinstream& m, VProperty& vp)
{
	m >> vp.id;
	m >> vp.plist;
	return m;
}

ibinstream& operator<<(ibinstream& m, const E_KVpair& pair)
{
	m << pair.key;
	m << pair.value;
	return m;
}

obinstream& operator>>(obinstream& m, E_KVpair& pair)
{
	m >> pair.key;
	m >> pair.value;
	return m;
}

ibinstream& operator<<(ibinstream& m, const EProperty& ep)
{
	m << ep.id;
	m << ep.plist;
	return m;
}

obinstream& operator>>(obinstream& m, EProperty& ep)
{
	m >> ep.id;
	m >> ep.plist;
	return m;
}
