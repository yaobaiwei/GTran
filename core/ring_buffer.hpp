#pragma once

#include <stdio.h>
#include <string.h>

#include "core/message.hpp"
#include "base/serialization.hpp"
#include "utils/unit.hpp"

template <class MSG>
class RingBuffer {
public:
    RingBuffer(char* buffer, int size) : buffer_(buffer), size_(size), header_(0) {}

    void Init() {
    	memset(buffer_, 0, size_);
    }

    // The first byte of msg should not be 0
    bool Pop(MSG & pop_msg) {
        // check header
    	uint64_t pop_msg_size = CheckHeader();
        if (pop_msg_size) {
            // Make sure RDMA trans is done
            while (CheckFooter(pop_msg_size) != pop_msg_size) {
                _mm_pause();
                assert(CheckFooter(pop_msg_size) == 0 || CheckFooter(pop_msg_size) == pop_msg_size);
            }

            // IF it is a ring(rare situation)
            uint64_t start = (header_ + sizeof(uint64_t)) % size_;
            uint64_t end = (header_ + sizeof(uint64_t) + pop_msg_size) % size_;
            if (start > end) {
                char* tmp_buf = new char[pop_msg_size];
                memcpy(tmp_buf, buffer_ + start, pop_msg_size - end);
                memcpy(tmp_buf + pop_msg_size - end, buffer_, end);

                //register tmp_buf into obinstream,
                //the obinstream will charge the memory of buf, including memory release
                obinstream um = obinstream(tmp_buf, pop_msg_size);
                um >> pop_msg;
                
                // clean
                memset(buffer_ + start, 0, pop_msg_size - end);
                memset(buffer_, 0, ceil(end, sizeof(uint64_t)));
            }
            else {
                obinstream um = obinstream(buffer_ + start, pop_msg_size);
                um >> pop_msg;

                // clean the data
                memset(buffer_ + start, 0, ceil(pop_msg_size, sizeof(uint64_t)));
            }

            //clear header and footer
            ClearHeader();
            ClearFooter(pop_msg_size);

            // advance the pointer
            header_ += 2 * sizeof(uint64_t) + ceil(pop_msg_size, sizeof(uint64_t));
            return true;
        }
        return false;
    }

    bool Check() {
        return CheckHeader();
    }

private:
    // memory
    char* buffer_;
    uint64_t size_;

    // begin for the next msg
    uint64_t header_;

    uint64_t CheckHeader() {
    	volatile uint64_t msg_size = *(volatile uint64_t *)(buffer_ + header_ % size_);  // header
        return msg_size;
    }
    
    void ClearHeader(){
    	*(uint64_t *)(buffer_ + header_ % size_) = 0;
    }

    uint64_t CheckFooter(uint64_t msg_size) {
    	uint64_t to_footer = sizeof(uint64_t) + ceil(msg_size, sizeof(uint64_t));
    	volatile uint64_t * footer = (volatile uint64_t *)(buffer_ + (header_ + to_footer) % size_); // footer
        return *footer;
    }

    void ClearFooter(uint64_t msg_size){
    	uint64_t to_footer = sizeof(uint64_t) + ceil(msg_size, sizeof(uint64_t));
    	*(uint64_t *)(buffer_ + (header_ + to_footer) % size_) = 0;
    }
};
