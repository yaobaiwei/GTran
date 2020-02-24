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
#include <sstream>
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
            trx_stats_.clear();
        }

        num_completed_ = 0;
        num_recorded_ = 0;
        num_aborted_ = 0;
        num_committed_ = 0;
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

    // Set the start time of normal query
    void RecordPushed() {
        unique_lock<mutex> lock(thread_mutex_);
        num_recorded_++;
    }

    // Set the start time of normal query
    void RecordStart(uint64_t qid, int query_type = -1, string trx_string = "") {
        unique_lock<mutex> lock(thread_mutex_);
        trx_stats_[qid] = make_tuple(query_type, timer::get_usec(), trx_string);
    }

    // Record latency of query
    uint64_t RecordEnd(uint64_t qid, bool isAbort, string& trx_string, int& trx_type) {
        unique_lock<mutex> lock(thread_mutex_);
        num_completed_++;
        if (isAbort) {
            num_aborted_++;

            // get trx string and type for rerunning
            trx_type = get<1>(trx_stats_.at(qid));
            trx_string = get<2>(trx_stats_.at(qid));
        } else {
            num_committed_++;
        }
        uint64_t latency = timer::get_usec() - get<1>(trx_stats_[qid]);
        if (!is_emu_) {
            trx_stats_.erase(qid);
        } else {
            get<1>(trx_stats_.at(qid)) = latency;
        }
        return latency;
    }

    int GetCommittedTrx() const { return num_committed_;}
    int GetCompletedTrx() const { return num_completed_; }
    int GetAbortedTrx() const { return num_aborted_; }

    double GetThroughput() {
        double thpt = 1000.0 * num_committed_;
        thpt /= (last_time_ - start_time_);
        return thpt;
    }

    void GetLatencyMap(map<int, vector<uint64_t>>& m) {
        unique_lock<mutex> lock(thread_mutex_);
        for (pair<uint64_t, tuple<int, uint64_t, string>> p : trx_stats_) {
            m[get<0>(p.second)].push_back(get<1>(p.second));
        }
        trx_stats_.clear();
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
        return data_storage_->GetContainerUsage();
    }

    // print the throughput of a fixed interval
    void PrintThroughput(int node_id) {
        uint64_t now = timer::get_usec();
        // periodically print timely throughput
        if ((now - last_time_) > interval) {
            double cur_thpt = 1000.0 * (num_committed_ - last_cnt_) / (now - last_time_);
            double usage = GetDataStorageUsage() * 100;
            string info = "Throughput: " + to_string(cur_thpt) + "K queries/sec with abort rate: " + to_string((double) num_aborted_ * 100 / num_completed_) + "% with mem " + to_string(usage) + "%\n";
            if(node_id == 0) { cout << info; }

            thpt_info += info;

            last_time_ = now;
            last_cnt_ = num_committed_;
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
    uint64_t num_committed_;
    bool is_emu_;
    mutex thread_mutex_;

    string thpt_info;
    DataStorage * data_storage_;

    // timer related
    uint64_t last_cnt_;
    uint64_t last_time_;
    uint64_t start_time_;
    static const uint64_t interval = 500000;

    // key: trx_id, value: <trx_type, latency, trx_string>
    unordered_map<uint64_t, tuple<int, uint64_t, string>> trx_stats_;
};

#endif  // BASE_THROUGHPUT_MONITOR_HPP_
