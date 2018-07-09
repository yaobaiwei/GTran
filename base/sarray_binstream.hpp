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

private:
	SArray<char> buffer_;
	size_t front_ = 0;
};

template <typename T>
SArrayBinStream& operator<<(SArrayBinStream& bin, const T& t);

template <typename T>
SArrayBinStream& operator>>(SArrayBinStream& bin, T& t) ;

template <typename InputT>
SArrayBinStream& operator<<(SArrayBinStream& stream, const std::basic_string<InputT>& v);

template <typename OutputT>
SArrayBinStream& operator>>(SArrayBinStream& stream, std::basic_string<OutputT>& v);

template <typename K, typename V>
SArrayBinStream& operator<<(SArrayBinStream& stream, const std::map<K, V>& map);

template <typename K, typename V>
SArrayBinStream& operator>>(SArrayBinStream& stream, std::map<K, V>& map) ;

template <typename InputT>
SArrayBinStream& operator<<(SArrayBinStream& stream, const std::vector<InputT>& v);

template <typename OutputT>
SArrayBinStream& operator>>(SArrayBinStream& stream, std::vector<OutputT>& v);



#endif /* SARRAY_BINSTREAM_HPP_ */
