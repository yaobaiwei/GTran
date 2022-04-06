// Copyright 2020 BigGraph Team @ Husky Data Lab, CUHK
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef PROGRESS_MONITOR_HPP_
#define PROGRESS_MONITOR_HPP_

#include <mutex>
#include <vector>
#include "base/node.hpp"
#include "base/communication.hpp"

class Monitor {
 public:
    explicit Monitor(Node & node):node_(node), num_task_(0), report_end_(false) {}

    void Init() {
        progress_.resize(2);
        progress_[0] = node_.get_world_rank();
    }

    void IncreaseCounter(int num) {
        std::lock_guard<std::mutex> lck(mtx_);
        num_task_ += num;
    }

    void DecreaseCounter(int num) {
        std::lock_guard<std::mutex> lck(mtx_);
        num_task_ -= num;
    }

    void ProgressReport() {
        while (!report_end_) {
            progress_[1] = num_task_;
            send_data(node_, progress_, MASTER_RANK, true, MONITOR_CHANNEL);
            // DEBUG
            // cout << "RANK " << node_.get_world_rank() << "=> SEND PROG " << num_task_ << endl;
            sleep(COMMUN_TIME);
        }
        progress_[1] = -1;
        send_data(node_, progress_, MASTER_RANK, true, MONITOR_CHANNEL);
    }

    void Start() {
        Init();
        thread_ = thread(&Monitor::ProgressReport, this);
    }

    void Stop() {
        report_end_ = true;
        send_data(node_, DONE, MASTER_RANK, true, MSCOMMUN_CHANNEL);
        thread_.join();
    }

 private:
    Node & node_;
    uint32_t num_task_;
    thread thread_;
    vector<uint32_t> progress_;
    bool report_end_;
    std::mutex mtx_;
};



#endif /* PROGRESS_MONITOR_HPP_ */
