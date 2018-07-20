/*
 * tool.hpp
 *
 *  Created on: May 23, 2018
 *      Author: Hongzhi Chen
 */

#ifndef TOOL_HPP_
#define TOOL_HPP_

#include <stdio.h>
#include <unistd.h>
#include <stdint.h>
#include <stdlib.h>
#include <assert.h>
#include <iostream>
#include <string>
#include <cstring>
#include <vector>
#include <regex>

#include "base/type.hpp"

using namespace std;
class Tool{
public:
	static vector<string> split(const string &s, const string &seperator){
		vector<string> result;
		typedef string::size_type string_size;
		string_size i = 0;

		while(i != s.size()){
			int flag = 0;
			while(i != s.size() && flag == 0){
				flag = 1;
				for(string_size x = 0; x < seperator.size(); ++x)
					if(s[i] == seperator[x]){
						++i;
						flag = 0;
						break;
					}
			}

			flag = 0;
			string_size j = i;
			while(j != s.size() && flag == 0){
				for(string_size x = 0; x < seperator.size(); ++x)
					if(s[j] == seperator[x]){
						flag = 1;
						break;
					}
				if(flag == 0) ++j;
			}
			if(i != j){
				result.push_back(s.substr(i, j-i));
				i = j;
			}
		}
		return result;
	}

	static string& trim(string &s, string sub)
	{
		if (s.empty())
	        return s;
	    s.erase(0,s.find_first_not_of(sub));
	    s.erase(s.find_last_not_of(sub) + 1);
	    return s;
	}

	static int value_t2int(const value_t & v){
		return *reinterpret_cast<const int *>(&(v.content[0]));
	}

	static double value_t2double(const value_t & v){
		return *reinterpret_cast<const double *>(&(v.content[0]));
	}

	static char value_t2char(const value_t & v){
		return v.content[0];
	}

	static string value_t2string(const value_t & v){
		return string(v.content.begin(), v.content.end());
	}

	static uint64_t value_t2uint64_t(const value_t & v){
		return *reinterpret_cast<const uint64_t *>(&(v.content[0]));
	}

	static void get_kvpair(string str, kv_pair & kvpair){
		vector<string> words = split(str,":");

		//only possible case is a kv-pair
		assert(words.size() == 2);

		string s_key = trim(words[0]," "); //delete all spaces
		kvpair.key = atoi(s_key.c_str());

		string s_value = trim(words[1]," "); //delete all spaces
		int type = checktype(s_value);
		switch(type){
			case 4: //string
				s_value = trim(s_value,"\"");
				str2str(s_value, kvpair.value);
				break;
			case 3: //char
				s_value = trim(s_value,"\'");
				str2char(s_value, kvpair.value);
				break;
			case 2: //double
				str2double(s_value, kvpair.value);
				break;
			case 1: //int
				str2int(s_value, kvpair.value);
				break;
			default:
				cout << "Error when parse the KV pair from sting!" << endl;
		}
	}

	static int checktype(string s){
		string quote = "\"";
		string squote = "\'";
		string dot = ".";
		if((s.find(quote) == 0) && (s.rfind(quote) == s.length()-quote.length()))
			return 4;//string
		if((s.find(squote) == 0) && (s.rfind(squote) == s.length()-squote.length()) && (s.length() == 3))
			return 3;//char
		if(s.find(dot) != string::npos)
			return 2;//double
		if (std::regex_match(s, std::regex("[(-|+)]?[0-9]+"))){
			uint64_t num = stoull(s);
			if(num >> 32 == 0){
				return 1; // int
			}else{
				return 5; // uint64_t;
			}
		}
		return -1;
	}

	static void str2uint64_t(string s, value_t & v){
		uint64_t u = stoull(s);
		size_t sz = sizeof(uint64_t);
		char f[sz];
		memcpy(f,(const char*)&u, sz);

		for(int k = 0 ; k < sz; k++)
			v.content.push_back(f[k]);
		v.type = 5;
	}

	static void str2str(string s, value_t & v){
		v.content.insert(v.content.end(), s.begin(), s.end());
		v.type = 4;
	}

	static void str2char(string s, value_t & v){
		v.content.push_back(s[0]);
		v.type = 3;
	}

	static void str2double(string s, value_t & v){
		double d = atof(s.c_str());
		size_t sz = sizeof(double);
		char f[sz];
		memcpy(f,(const char*)&d, sz);

		for(int k = 0 ; k < sz; k++)
			v.content.push_back(f[k]);
		v.type = 2;
	}

	static void str2int(string s, value_t & v){
		int i = atoi(s.c_str());
		size_t sz = sizeof(int);
		char f[sz];
		memcpy(f,(const char*)&i, sz);

		for(int k = 0 ; k < sz; k++)
			v.content.push_back(f[k]);
		v.type = 1;
	}

	static bool str2value_t(string s, value_t & v){
		int type = Tool::checktype(s);
		switch (type){
		case 5:
			Tool::str2uint64_t(s, v);
			break;
		case 4: //string
			s = Tool::trim(s, "\"");
			Tool::str2str(s, v);
			break;
		case 3: //char
			s = Tool::trim(s, "\'");
			Tool::str2char(s, v);
			break;
		case 2: //double
			Tool::str2double(s, v);
			break;
		case 1: //int
			Tool::str2int(s, v);
			break;
		default:
			return false;
		}
		return true;
	}

	static bool vec2value_t(const vector<string>& vec, value_t & v, int type)
	{
		if (vec.size() == 0)
		{
			return false;
		}
		for (string s : vec){
			string s_value = trim(s, " ");
			if (checktype(s_value) != type){
				return false;
			}
			str2value_t(s_value, v);
			v.content.push_back('\t');
		}

		// remove last '\t'
		v.content.pop_back();
		v.type = 16 | type;
		return true;
	}

	static void kvmap2value_t(const map<string, string>& m, value_t & v){
		v.type = 20; // vector of string

		for(auto& item : m){
			str2value_t(item.first + ":" + item.second ,v);
			v.content.push_back('\t');
		}
		v.content.pop_back();
	}

	static void value_t2vec(value_t & v, vector<value_t>& vec)
	{
		string value = value_t2string(v);
		vector<string> values = split(value, "\t");
		int type = v.type & 0xF;

		// scalar
		if (v.type < 0xF){
			vec.push_back(v);
		}
		else{
			for (string s : values){
				value_t v;
				str2str(s, v);
				v.type = type;
				vec.push_back(v);
			}
		}
	}

	static void elem_t2value_t(elem_t & e, value_t & v){
		v.content.resize(e.sz);
		v.content.insert(v.content.end(), e.content, e.content+e.sz);
		v.type = e.type;
	}

	static eid_t value_t2eid_t(value_t & v) {
		uint64_t eid_value = value_t2uint64_t(v);
		uint64_t in_v = eid_value >> VID_BITS;
		uint64_t out_v = eid_value - (in_v << VID_BITS);

		return eid_t(in_v, out_v);
	}

	static string DebugString(value_t & v){
		double d;
		int i;
		uint64_t u;
		vector<value_t> vec;
		string temp;
		switch (v.type)
		{
		case 5:
			u = Tool::value_t2uint64_t(v);
			return to_string(u);
		case 4:
		case 3:
			return string(v.content.begin(), v.content.end());
		case 2:
			d = Tool::value_t2double(v);
			return to_string(d);
		case 1:
			i = Tool::value_t2int(v);
			return to_string(i);
		case -1:
			return "";
		default:
			value_t2vec(v, vec);
			temp = "[";
			for(auto& val : vec){
				temp += DebugString(val) + ", ";
			}
			temp = trim(temp, ", ");
			temp += "]";
			return temp;
		}
	}
};


#endif /* TOOL_HPP_ */
