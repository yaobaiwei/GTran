///------------- too be removed

#include<vector>
#include <regex>
#include <string>
#include "base/type.hpp"
using namespace std;

// to tool.hpp
class Tool{
public:
	static vector<string> split(const string &s, const string &seperator){
		vector<string> result;
		typedef string::size_type string_size;
		string_size i = 0;

		while (i != s.size()){
			int flag = 0;
			while (i != s.size() && flag == 0){
				flag = 1;
				for (string_size x = 0; x < seperator.size(); ++x)
				if (s[i] == seperator[x]){
					++i;
					flag = 0;
					break;
				}
			}

			flag = 0;
			string_size j = i;
			while (j != s.size() && flag == 0){
				for (string_size x = 0; x < seperator.size(); ++x)
				if (s[j] == seperator[x]){
					flag = 1;
					break;
				}
				if (flag == 0) ++j;
			}
			if (i != j){
				result.push_back(s.substr(i, j - i));
				i = j;
			}
		}
		return result;
	}

	static string& trim(string &s, string sub)
	{
		if (s.empty())
			return s;
		s.erase(0, s.find_first_not_of(sub));
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

	static string value_t2string(value_t & v){
		return string(v.content.begin(), v.content.end());
	}

	static int checktype(string s){
		string quote = "\"";
		string squote = "\'";
		string dot = ".";
		if ((s.find(quote) == 0) && (s.rfind(quote) == s.length() - quote.length()))
			return 4;//string
		if ((s.find(squote) == 0) && (s.rfind(squote) == s.length() - squote.length()) && (s.length() == 3))
			return 3;//char
		if (s.find(dot) != string::npos)
			return 2;//double
		if (std::regex_match(s, std::regex("[(-|+)]?[0-9]+")))
			return 1;//int
		return -1;
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
		char f[8];
		memcpy(f, (const char*)&d, sz);

		for (int k = 0; k < sz; k++)
			v.content.push_back(f[k]);
		v.type = 2;
	}


	static void str2int(string s, value_t & v){
		int i = atoi(s.c_str());
		size_t sz = sizeof(int);
		char f[4];
		memcpy(f, (const char*)&i, sz);

		for (int k = 0; k < sz; k++)
			v.content.push_back(f[k]);
		v.type = 1;
	}

	//// new functions
	static int value_t2uint64_t(value_t & v){
		return *reinterpret_cast<uint64_t *>(&(v.content[0]));
	}

	static bool str2value_t(string s, value_t & v){
		int type = Tool::checktype(s);
		switch (type){
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

	static vector<value_t> value_t2vec(value_t & v)
	{
		string value = value_t2string(v);
		vector<string> values = split(value, "\t");
		vector<value_t> vec;
		int type = v.type & 0xE;

		// scalar
		if (v.type < 0xE){
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
		return vec;
	}
};
//-----------------------------------------------------------


// to type.hpp
enum class ACTOR_T : char {
	ADD, PROXY, HW, AGGREGATE, AS, BRANCH, BRANCHFILTER, CAP, COIN, COUNT, DEDUP, GROUP, HAS,
	HASLABEL, IS, KEY, LABEL, LOOPS, MATH, ORDER, PROJECTION, PROPERTY, RANGE, REPEAT, SELECT, TRAVERSAL, VALUES, WHERE
};
static const char *ActorType[] = { "ADD", "PROXY", "HW", "AGGREGATE", "AS", "BRANCH", "BRANCHFILTER", "CAP", "COIN", "COUNT", "DEDUP", "GROUP", "HAS",
"HASLABEL", "IS", "KEY", "LABEL", "LOOPS", "MATH", "ORDER", "PROJECTION", "PROPERTY", "RANGE", "REPEAT", "SELECT", "TRAVERSAL", "VALUES", "WHERE" };
ibinstream& operator<<(ibinstream& m, const ACTOR_T& type);
obinstream& operator>>(obinstream& m, ACTOR_T& type);

enum Relation{ TRUE, FALSE, UNDEFINED };

Relation operator==(value_t& lhs, value_t& rhs);

Relation operator!=(value_t& lhs, value_t& rhs);
Relation operator>(value_t& lhs, value_t& rhs);

Relation operator<(value_t& lhs, value_t& rhs);

Relation operator>=(value_t& lhs, value_t& rhs);

Relation operator<=(value_t& lhs, value_t& rhs);

// Enums for actors
enum Branch_T { UNION, COALESCE, CHOOSE };
enum Filter_T{ AND, OR, NOT };
enum Math_T { SUM, MAX, MIN, MEAN };
enum Element_T{ VERTEX, EDGE };
enum Direction_T{ IN, OUT, BOTH };
enum Predicate_T{ ANY, NONE, EQ, NEQ, LT, LTE, GT, GTE, INSDIE, OUTSIDE, BETWEEN, WITHIN, WITHOUT };
