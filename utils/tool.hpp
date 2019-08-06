/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Hongzhi Chen (hzchen@cse.cuhk.edu.hk)
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
#include <map>
#include <utility>

#include "base/type.hpp"

class Tool{
 public:
    static void split(const string &s, const string &seperator, vector<string>& result) {
        typedef string::size_type string_size;
        string_size i = 0;

        while (i != s.size()) {
            int flag = 0;
            while (i != s.size() && flag == 0) {
                flag = 1;
                for (string_size x = 0; x < seperator.size(); ++x)
                    if (s[i] == seperator[x]) {
                        ++i;
                        flag = 0;
                        break;
                    }
            }

            flag = 0;
            string_size j = i;
            while (j != s.size() && flag == 0) {
                for (string_size x = 0; x < seperator.size(); ++x)
                    if (s[j] == seperator[x]) {
                        flag = 1;
                        break;
                    }
                if (flag == 0) ++j;
            }
            if (i != j) {
                result.push_back(s.substr(i, j-i));
                i = j;
            }
        }
    }

    // split string with seperator, escaping double quote
    // Make sure no double quote in double quote before using this function
    static void splitWithEscape(const string &s, const string &seperator, vector<string>& result) {
        // seperator should not contains "
        // assert(seperator.find('"') == string::npos);
        typedef string::size_type string_size;
        string_size i = 0;
        bool in_quote = false;
        while (i != s.size()) {
            int flag = 0;

            while (i != s.size() && flag == 0) {
                flag = 1;
                for (string_size x = 0; x < seperator.size(); ++x)
                    if (s[i] == seperator[x]) {
                        ++i;
                        flag = 0;
                        break;
                    }
            }

            flag = 0;
            string_size j = i;
            while (j != s.size() && flag == 0) {
                if (s[j] == '"') {
                    in_quote = !in_quote;
                }
                if (!in_quote) {
                    for (string_size x = 0; x < seperator.size(); ++x)
                        if (s[j] == seperator[x]) {
                            flag = 1;
                            break;
                        }
                }
                if (flag == 0) ++j;
            }
            if (i != j) {
                result.push_back(s.substr(i, j-i));
                i = j;
            }
        }
    }

    static string& trim(string &s, string sub) {
        if (s.empty())
            return s;
        s.erase(0, s.find_first_not_of(sub));
        s.erase(s.find_last_not_of(sub) + 1);
        return s;
    }

    static int value_t2int(const value_t & v) {
        return *reinterpret_cast<const int *>(&(v.content[0]));
    }

    static double value_t2double(const value_t & v) {
        return *reinterpret_cast<const double *>(&(v.content[0]));
    }

    static char value_t2char(const value_t & v) {
        return v.content[0];
    }

    static string value_t2string(const value_t & v) {
        return string(v.content.begin(), v.content.end());
    }

    static uint64_t value_t2uint64_t(const value_t & v) {
        return *reinterpret_cast<const uint64_t *>(&(v.content[0]));
    }

    static void get_kvpair(string & key, string & value, int type_, kv_pair & kvpair) {
        string s_key = trim(key, " ");  // delete all spaces
        kvpair.key = atoi(s_key.c_str());

        string s_value = trim(value, " ");  // delete all spaces
        switch (type_) {
          case 4:  // string
            s_value = trim(s_value, "\"");
            str2str(s_value, kvpair.value);
            break;
          case 3:  // char
            s_value = trim(s_value, "\'");
            str2char(s_value, kvpair.value);
            break;
          case 2:  // double
            str2double(s_value, kvpair.value);
            break;
          case 1:  // int
            str2int(s_value, kvpair.value);
            break;
          default:
            cout << "Error when parse the KV pair from sting!" << endl;
        }
    }

    static int checktype(string s) {
        string quote = "\"";
        string squote = "\'";
        string dot = ".";
        if ((s.find(quote) == 0) && (s.rfind(quote) == s.length()-quote.length()))
            return 4;  // string
        if ((s.find(squote) == 0) && (s.rfind(squote) == s.length()-squote.length()) && (s.length() == 3))
            return 3;  // char
        if (s.find(dot) != string::npos)
            return 2;  // double
        if (std::regex_match(s, std::regex("[(-|+)]?[0-9]+"))) {
            uint64_t num = stoull(s);
            if (num >> 32 == 0) {
                return 1;  // int
            } else {
                return 5;  // uint64_t;
            }
        }
        return -1;
    }

    static void str2uint64_t(string s, value_t & v) {
        uint64_t u = stoull(s);
        uint64_t2value_t(u, v);
    }

    static void str2str(string s, value_t & v) {
        v.content.insert(v.content.end(), s.begin(), s.end());
        v.type = 4;
    }

    static void str2char(string s, value_t & v) {
        v.content.push_back(s[0]);
        v.type = 3;
    }

    static void str2double(string s, value_t & v) {
        double d = atof(s.c_str());
        double2value_t(d, v);
    }

    static void str2int(string s, value_t & v) {
        int i = atoi(s.c_str());
        int2value_t(i, v);
    }

    static bool str2value_t(string s, value_t & v) {
        int type = Tool::checktype(s);
        switch (type) {
          case 5:
            Tool::str2uint64_t(s, v);
            break;
          case 4:  // string
            s = Tool::trim(s, "\"");
            Tool::str2str(s, v);
            break;
          case 3:  // char
            s = Tool::trim(s, "\'");
            Tool::str2char(s, v);
            break;
          case 2:  // double
            Tool::str2double(s, v);
            break;
          case 1:  // int
            Tool::str2int(s, v);
            break;
          default:
            return false;
        }
        return true;
    }

    static void double2value_t(double d, value_t & v) {
        size_t sz = sizeof(double);
        char f[sz];
        memcpy(f, (const char*)&d, sz);

        for (int k = 0 ; k < sz; k++)
            v.content.push_back(f[k]);
        v.type = 2;
    }

    static void int2value_t(int i, value_t & v) {
        size_t sz = sizeof(int);
        char f[sz];
        memcpy(f, (const char*)&i, sz);

        for (int k = 0 ; k < sz; k++)
            v.content.push_back(f[k]);
        v.type = 1;
    }

    static bool uint64_t2value_t(uint64_t u64, value_t & v) {
        size_t sz = sizeof(uint64_t);
        char f[sz];
        memcpy(f, (const char*)&u64, sz);

        for (int k = 0 ; k < sz; k++)
            v.content.push_back(f[k]);
        v.type = 5;
    }

    static bool vec2value_t(const vector<string>& vec, value_t & v, int type) {
        if (vec.size() == 0) {
            return false;
        }
        for (string s : vec) {
            string s_value = trim(s, " ");
            if (checktype(s_value) != type) {
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

    static bool vec2value_t(const vector<value_t>& vec, value_t & v) {
        if (vec.size() == 0) {
            return false;
        }
        int type = vec[0].type;
        for (auto& val : vec) {
            if (val.type != type) {
                return false;
            }
            v.content.insert(v.content.begin(), val.content.begin(), val.content.end());
            v.content.push_back('\t');
        }

        // remove last '\t'
        v.content.pop_back();
        v.type = 16 | type;
        return true;
    }

    static void kvmap2value_t(const map<string, string>& m, vector<value_t> & vec) {
        for (auto& item : m) {
            value_t v;
            str2str("{" + item.first + ":" + item.second + "}", v);
            vec.push_back(v);
        }
    }

    static void vec_pair2value_t(const vector<pair<uint64_t, string>> & v_p, vector<value_t> & vec) {
        // type 6 : v/epid(uint64_t) + {pkey : pvalue} (string)
        //  for supporting drop.
        for (auto& pair : v_p) {
            value_t v;
            uint64_t2value_t(pair.first, v);
            str2str(pair.second, v);
            v.type = 6;
            vec.emplace_back(v);
        }
    }

    static void value_t2vec(const value_t & v, vector<value_t>& vec) {
        string value = value_t2string(v);
        vector<string> values;
        int type = v.type & 0xF;

        if (type == 4) {
            // string
            splitWithEscape(value, "\t", values);
        } else {
            split(value, "\t", values);
        }

        // scalar
        if (v.type < 0xF) {
            vec.push_back(v);
        } else {
            for (string s : values) {
                value_t v;
                str2str(s, v);
                v.type = type;
                vec.push_back(v);
            }
        }
    }

    static void elem_t2value_t(elem_t & e, value_t & v) {
        v.content.resize(e.sz);
        v.content.insert(v.content.end(), e.content, e.content+e.sz);
        v.type = e.type;
    }

    static string DebugString(const value_t & v) {
        double d;
        int i;
        uint64_t u;
        size_t uint64_sz = sizeof(uint64_t);
        vector<value_t> vec;
        string temp;
        switch (v.type) {
          case 6:  // v/epid(uint64_t) + {pkey : pvalue} (string)
            u = Tool::value_t2uint64_t(v);
            // return to_string(u) + " : " + string(v.content.begin() + uint64_sz, v.content.end()); 
            return string(v.content.begin() + uint64_sz, v.content.end()); 
          case 5:
            u = Tool::value_t2uint64_t(v);
            return to_string(u);
          case 4:
          case 3:
            return string(v.content.begin(), v.content.end());
          case 2:
            d = Tool::value_t2double(v);
            temp = to_string(d);
            temp.resize(temp.find_last_not_of("0") + 1);
            if (temp.back() == '.')
                temp += "0";
            return temp;
          case 1:
            i = Tool::value_t2int(v);
            return to_string(i);
          case -1:
            return "";
          default:
            value_t2vec(v, vec);
            temp = "[";
            for (auto& val : vec) {
                temp += DebugString(val) + ", ";
            }
            temp = trim(temp, ", ");
            temp += "]";
            return temp;
        }
    }

    static string int64_to_2int32_str(uint64_t val) {
        int a, b;

        a = val & 0xFFFFFFFF;
        b = val >> 32;

        char tmp[64];

        int pos = 0;

        pos += snprintf(tmp + pos, sizeof(tmp), "%d ", b);  // higher
        pos += snprintf(tmp + pos, sizeof(tmp), "%d", a);  // higher

        return string(tmp);
    }

    static string my_regex_replace(string & original, regex match, vector<string> replace_target) {
        for (auto & t : replace_target) {
            original = regex_replace(original, match, t, regex_constants::format_first_only);
        }
        return original;
    }
};


#endif /* TOOL_HPP_ */
