/*
 * sarray_binstream.cpp
 *
 *  Created on: Jun 5, 2018
 *      Author: Yuzhen Huang
 */

#include "base/sarray_binstream.hpp"


size_t SArrayBinStream::Size() const { return buffer_.size() - front_; }

void SArrayBinStream::AddBin(const char* bin, size_t sz) {
	buffer_.append_bytes(bin, sz);
}

void* SArrayBinStream::PopBin(size_t sz) {
	CHECK_LE(front_ + sz, buffer_.size());
	void* ret = &buffer_[front_];
	front_ += sz;
	return ret;
}

Message SArrayBinStream::ToMsg() const {
	Message msg;
	msg.AddData(buffer_);
	return msg;
}

void SArrayBinStream::FromMsg(const Message& msg) {
	CHECK_EQ(msg.data.size(), 1);
	buffer_ = msg.data[0];
	front_ = 0;
}

export template <typename T>
SArrayBinStream& operator<<(SArrayBinStream& bin, const T& t) {
	bin.AddBin((char*)&t, sizeof(T));
	return bin;
}

export template <typename T>
SArrayBinStream& operator>>(SArrayBinStream& bin, T& t) {
	t = *(T*)(bin.PopBin(sizeof(T)));
	return bin;
}

export template <typename InputT>
SArrayBinStream& operator<<(SArrayBinStream& stream, const std::basic_string<InputT>& v) {
    size_t len = v.size();
    stream << len;
    for (auto& elem : v)
        stream << elem;
    return stream;
}

export template <typename OutputT>
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


export template <typename K, typename V>
SArrayBinStream& operator<<(SArrayBinStream& stream, const std::map<K, V>& map) {
    size_t len = map.size();
    stream << len;
    for (auto& elem : map)
        stream << elem;
    return stream;
}

export template <typename K, typename V>
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

export template <typename InputT>
SArrayBinStream& operator<<(SArrayBinStream& stream, const std::vector<InputT>& v) {
    size_t len = v.size();
    stream << len;
    for (int i = 0; i < v.size(); ++i)
        stream << v[i];
    return stream;
}

export template <typename OutputT>
SArrayBinStream& operator>>(SArrayBinStream& stream, std::vector<OutputT>& v) {
    size_t len;
    stream >> len;
    v.clear();
    v.resize(len);
    for (int i = 0; i < v.size(); ++i)
        stream >> v[i];
    return stream;
}


