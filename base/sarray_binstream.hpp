/*
 * sarray_binstream.hpp
 *
 *  Created on: Jun 5, 2018
 *      Author: Yuzhen Huang
 */

#ifndef SARRAY_BINSTREAM_HPP_
#define SARRAY_BINSTREAM_HPP_

#include <string>
#include <vector>
#include <map>

#include "base/sarray.hpp"
#include "core/message.hpp"

#include "glog/logging.h"


class SArrayBinStream {
public:
	SArrayBinStream() = default;
	~SArrayBinStream() = default;

	size_t Size() const;

	void AddBin(const char* bin, size_t sz);

	void* PopBin(size_t sz);

	Message ToMsg() const;

	void FromMsg(const Message& msg);

private:
	SArray<char> buffer_;
	size_t front_ = 0;
};

template <typename T>
SArrayBinStream& operator<<(SArrayBinStream& bin, const T& t) {
	bin.AddBin((char*)&t, sizeof(T));
	return bin;
}

template <typename T>
SArrayBinStream& operator>>(SArrayBinStream& bin, T& t) {
	t = *(T*)(bin.PopBin(sizeof(T)));
	return bin;
}

template <typename InputT>
SArrayBinStream& operator<<(SArrayBinStream& stream, const std::basic_string<InputT>& v) {
    size_t len = v.size();
    stream << len;
    for (auto& elem : v)
        stream << elem;
    return stream;
}

template <typename OutputT>
SArrayBinStream& operator>>(SArrayBinStream& stream, std::basic_string<OutputT>& v) {
    size_t len;
    stream >> len;
    v.clear();
    try {
        v.resize(len);
    } catch (std::exception e) {
        assert(false);
    }
    for (auto& elem : v)
        stream >> elem;
    return stream;
}


template <typename K, typename V>
SArrayBinStream& operator<<(SArrayBinStream& stream, const std::map<K, V>& map) {
    size_t len = map.size();
    stream << len;
    for (auto& elem : map)
        stream << elem;
    return stream;
}

template <typename K, typename V>
SArrayBinStream& operator>>(SArrayBinStream& stream, std::map<K, V>& map) {
    size_t len;
    stream >> len;
    map.clear();
    for (int i = 0; i < len; i++) {
        std::pair<K, V> elem;
        stream >> elem;
        map.insert(elem);
    }
    return stream;
}

template <typename InputT>
SArrayBinStream& operator<<(SArrayBinStream& stream, const std::vector<InputT>& v) {
    size_t len = v.size();
    stream << len;
    for (int i = 0; i < v.size(); ++i)
        stream << v[i];
    return stream;
}

template <typename OutputT>
SArrayBinStream& operator>>(SArrayBinStream& stream, std::vector<OutputT>& v) {
    size_t len;
    stream >> len;
    v.clear();
    v.resize(len);
    for (int i = 0; i < v.size(); ++i)
        stream >> v[i];
    return stream;
}



#endif /* SARRAY_BINSTREAM_HPP_ */
