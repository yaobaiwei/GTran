/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Nick Fang (jcfang6@cse.cuhk.edu.hk)
*/

#ifndef BASE_THROUGHPUT_MONITOR_HPP_
#define BASE_THROUGHPUT_MONITOR_HPP_

#include <stdio.h>

#include <algorithm>
#include <map>
#include <vector>
#include <string>
#include <utility>
#include <unordered_map>

#include "utils/timer.hpp"

#include "layout/data_storage.hpp"

class ThroughputMonitor {
 public:
    ThroughputMonitor() : is_emu_(false) {
        data_storage_ = DataStorage::GetInstance();
    }

    void StartEmu() {
        {
            unique_lock<mutex> lock(thread_mutex_);
            stats_.clear();
        }

        num_completed_ = 0;
        num_recorded_ = 0;
        num_aborted_ = 0;
        last_cnt_ = 0;
        start_time_ = last_time_ = timer::get_usec();
        is_emu_ = true;

        thpt_info = "";
    }

    void StopEmu() {
        last_time_ = timer::get_usec();
        last_cnt_ = num_completed_;
        is_emu_ = false;
    }

    // Set the start time of emu command
    void SetEmuStartTime(uint64_t qid) {
        num_recorded_++;
        unique_lock<mutex> lock(thread_mutex_);
        stats_[qid].second = start_time_;
    }

    // Set the start time of normal query
    void RecordStart(uint64_t qid, int query_type = -1) {
        unique_lock<mutex> lock(thread_mutex_);
        num_recorded_++;
        stats_[qid] = make_pair(query_type, timer::get_usec());
    }

    // Record latency of query
    uint64_t RecordEnd(uint64_t qid, bool isAbort) {
        num_completed_++;
        if (isAbort) { num_aborted_++; }
        unique_lock<mutex> lock(thread_mutex_);
        uint64_t latency = timer::get_usec() - stats_[qid].second;
        if (!is_emu_) {
            stats_.erase(qid);
        } else {
            stats_.at(qid).second = latency;
        }
        return latency;
    }

    int GetCompletedTrx() { return num_completed_; }
    int GetAbortedTrx() { return num_aborted_; }

    double GetThroughput() {
        double thpt = 1000.0 * last_cnt_;
        thpt /= (last_time_ - start_time_);
        return thpt;
    }

    void GetLatencyMap(map<int, vector<uint64_t>>& m) {
        for (auto & latency : stats_) {
            m[latency.second.first].push_back(latency.second.second);
        }
        unique_lock<mutex> lock(thread_mutex_);
        stats_.clear();
    }

    void PrintCDF(vector<map<int, vector<uint64_t>>>& vec) {
        map<int, vector<uint64_t>> m;
        for (auto& node_map : vec) {
            for (auto& item : node_map) {
                m[item.first].insert(m[item.first].end(), item.second.begin(), item.second.end());
            }
        }

        vector<double> cdf_rates = {0.01};

        // output cdf
        // 5% >> 95%
        for (int i = 1; i < 20; i++)
            cdf_rates.push_back(0.05 * i);

        // 96% >> 100%
        for (int i = 1; i <= 5; i++)
            cdf_rates.push_back(0.95 + i * 0.01);

        map<int, vector<uint64_t>> cdf_res;
        for (auto& item : m) {
            if (!item.second.empty()) {
                sort(item.second.begin(), item.second.end());

                int query_type = item.first;
                int cnt = 0;
                // select 25 points from total_latency_map
                // select points from lats corresponding to cdf_rates
                for (auto const &rate : cdf_rates) {
                    int idx = item.second.size() * rate;
                    if (idx >= item.second.size()) idx = item.second.size() - 1;
                    cdf_res[query_type].push_back(item.second[idx]);
                    cnt++;
                }
                assert(cdf_res[query_type].size() == 25);
            }
        }
        string ofname = "CDF.txt";
        ofstream ofs(ofname, ofstream::out);
        ofs << "CDF Res: " << endl;
        ofs << "P";

        for (auto& item : m) {
            ofs << "\t" << "Q" << item.first;
        }
        ofs << endl;

        // print cdf data
        for (int row = 1; row <= 25; ++row) {
            if (row == 1)
                ofs << row << "\t";
            else if (row <= 20)
                ofs << 5 * (row - 1) << "\t";
            else
                ofs << 95 + (row - 20) << "\t";

            for (auto& item : m) {
                ofs << cdf_res[item.first][row - 1] << "\t";
            }

            ofs << endl;
        }
    }

    uint64_t WorksRemaining() {
        return num_recorded_ - num_completed_;
    }

    double GetDataStorageUsage() {
        vector<pair<uint64_t, uint64_t>> vec_s;
        vec_s.emplace_back(data_storage_->vp_row_pool_->UsageStatistic(128));
        vec_s.emplace_back(data_storage_->ve_row_pool_->UsageStatistic(128));
        vec_s.emplace_back(data_storage_->ep_row_pool_->UsageStatistic(64));
        vec_s.emplace_back(data_storage_->vp_mvcc_pool_->UsageStatistic(32));
        vec_s.emplace_back(data_storage_->ep_mvcc_pool_->UsageStatistic(32));
        vec_s.emplace_back(data_storage_->vertex_mvcc_pool_->UsageStatistic(32));
        vec_s.emplace_back(data_storage_->edge_mvcc_pool_->UsageStatistic(40));
        // vec_s.emplace_back(data_storage_->vp_store_->UsageStatistic(12));
        // vec_s.emplace_back(data_storage_->ep_store_->UsageStatistic(12));

        uint64_t total = 0, avail = 0;

        for (auto & pair : vec_s) {
            avail += pair.first;
            total += pair.second;
        }

        return (double)avail / total;
    }

    // print the throughput of a fixed interval
    void PrintThroughput(int node_id) {
        uint64_t now = timer::get_usec();
        // periodically print timely throughput
        if ((now - last_time_) > interval) {
            double cur_thpt = 1000.0 * (num_completed_ - last_cnt_) / (now - last_time_);
            double usage = (1 - GetDataStorageUsage()) * 100;
            string info = "Throughput: " + to_string(cur_thpt) + "K queries/sec with abort rate: " + to_string((double) num_aborted_ * 100 / num_completed_) + "% with mem " + to_string(usage) + "%\n";
            if(node_id == 0) { cout << info; }

            thpt_info += info;

            last_time_ = now;
            last_cnt_ = num_completed_;
        }
    }

    void PrintThptToFile(int node_id) {
        if (thpt_info != "") {
            string ofname = "Thpt_Record_" + to_string(node_id) + ".txt";
            ofstream ofs(ofname, ofstream::out);
            ofs << thpt_info;
        }
    }

 private:
    uint64_t num_completed_;
    uint64_t num_recorded_;
    uint64_t num_aborted_;
    bool is_emu_;
    mutex thread_mutex_;

    string thpt_info;
    DataStorage * data_storage_;

    // timer related
    uint64_t last_cnt_;
    uint64_t last_time_;
    uint64_t start_time_;
    static const uint64_t interval = 500000;

    unordered_map<uint64_t, pair<int, uint64_t>> stats_;
};

#endif  // BASE_THROUGHPUT_MONITOR_HPP_
