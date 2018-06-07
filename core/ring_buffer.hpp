/*
 * ring_buffer.hpp
 *
 *  Created on: May 12, 2018
 *      Author: Hongzhi Chen
 */

#pragma once

#include <stdint.h>
#include "base/serialization.hpp"

class RingBuffer {
public:
	RingBuffer(){}
    RingBuffer(char* buffer, int size) : buffer_(buffer), size_(size), header_(0) {}

    void Init();
    // The first byte of msg should not be 0
    bool Pop(obinstream & um);
    bool Check();

private:
    // memory
    char* buffer_;
    uint64_t size_;

    // begin for the next msg
    uint64_t header_;
    uint64_t CheckHeader();
    void ClearHeader();
    uint64_t CheckFooter(uint64_t msg_size);
    void ClearFooter(uint64_t msg_size);
};
