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

#include "utils/type.hpp"

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

	static int value_t2int(value_t & v){
		return *reinterpret_cast<int *>(&(v.content[0]));
	}

	static double value_t2double(value_t & v){
		return *reinterpret_cast<double *>(&(v.content[0]));
	}

	static char value_t2char(value_t & v){
		return v.content[0];
	}

	string static value_t2string(value_t & v){
		return string(v.content.begin(), v.content.end());
	}

	void static get_kvpair(string str, kv_pair & kvpair){
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

	//TODO 4 type convert functions
	vid_t static uint2vid_t(uint64_t id){
		return id;
	}

	eid_t static uint2eid_t(uint64_t id){
		return id;
	}

	vpid_t static uint2vpid_t(uint64_t id){
		return id;
	}

	epid_t static uint2epid_t(uint64_t id){
		return id;
	}

private:
	int checktype(string s){
		string quote = "\"";
		string squote = "\'";
		string dot = ".";
		if((s.find(quote) == 0) && (s.rfind(quote) == s.length()-quote.length()))
			return 4;//string
		if((s.find(squote) == 0) && (s.rfind(squote) == s.length()-squote.length()) && (s.length() == 3))
			return 3;//char
		if(s.find(dot) != string::npos)
			return 2;//double
		if(s.find_first_not_of("0123456789") == string::npos)
			return 1;//int
		return -1;
	}

	void str2str(string s, value_t & v){
		v.content.insert(v.content.end(), s.begin(), s.end());
		v.type = 4;
	}

	void str2char(string s, value_t & v){
		v.content.push_back(s[0]);
		v.type = 3;
	}

	void str2double(string s, value_t & v){
		double d = atof(s.c_str());
		size_t sz = sizeof(double);
		char f[sz];
		memcpy(f,(const char*)&d, sz);

		for(int k = 0 ; k < sz; k++)
			v.content.push_back(f[k]);
		v.type = 2;
	}

	void str2int(string s, value_t & v){
		int i = atoi(s.c_str());
		size_t sz = sizeof(int);
		char f[sz];
		memcpy(f,(const char*)&i, sz);

		for(int k = 0 ; k < sz; k++)
			v.content.push_back(f[k]);
		v.type = 1;
	}
};



#endif /* TOOL_HPP_ */
