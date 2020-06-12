// Copyright 2019 BigGraph Team @ Husky Data Lab, CUHK
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

#pragma once

#include <bits/stdc++.h>
#include <mpi.h>
#include <time.h>

#include <atomic>
#include <set>

#define TIMESTAMP_MACHINE_ID_BITS 8

// Only one thread should be used to get the timestamp
class DistributedClock {
 public:
    static DistributedClock* GetInstance() {
        static DistributedClock instance;
        return &instance;
    }

    void Init(MPI_Comm comm, int local_calibrate_delay = 3, int global_calibrate_loop = 1000, double sample_rate = 0.05);

    // Get the "tsc => nanosec" formular
    void LocalCalibrate(int local_calibrate_delay);

    // The wall time on each node is different. Thus, we need calibrate them.
    void GlobalCalibrateMeasure(int global_calibrate_loop, double sample_rate, bool with_sys_error_fix = false);
    void GlobalCalibrateApply();

    // The clock speed is different on each node is not the same.
    // So, we need to calculate the system error.
    void GlobalSystemErrorCalculate(int global_calibrate_loop, int sleep_duration, double sample_rate);

    // not thread safe
    uint64_t GetTimestamp() const;
    uint64_t GetTimestampWithSystemErrorFix() const;

    // for GetTimestamp
    inline uint64_t GetRefinedNS() const {
        return GetGlobalPhysicalNS() - global_min_ns_;
    }

    inline uint64_t GetRefinedNSWithSystemErrorFix() const {
        return GetGlobalPhysicalNSWithSystemErrorFix() - global_min_ns_;
    }

    inline uint64_t GetGlobalPhysicalNSWithSystemErrorFix() const {
        uint64_t ret = GetGlobalPhysicalNS();
        return (ret - global_calibrate_applied_ns_) * system_error_fix_rate_ + global_calibrate_applied_ns_ + post_calibrate_offset_;
    }

    inline uint64_t GetGlobalPhysicalNS() const {
        // system error occurs
        return GetLocalPhysicalNS() + global_ns_offset_ + post_calibrate_offset_;
    }

    inline uint64_t GetGlobalSysNS() const {
        return rdsysns() + global_ns_offset_;
    }

    inline uint64_t GetLocalPhysicalNS() const {
        uint64_t tsc = rdtsc();
        return local_ns_offset_ + (int64_t)((int64_t)tsc * tsc_ghz_inv_);
    }

    inline void IncreaseGlobalNSOffset(int64_t delta) {
        assert(delta > 0);
        post_calibrate_offset_ += delta;
    }

    inline int64_t GetMeasuredGlobalNSOffset() const {
        return measured_global_ns_offset_;
    }

 private:
    DistributedClock();

    MPI_Comm comm_, ori_comm_;
    int my_rank_, comm_sz_;
    double tsc_ghz_inv_;
    uint64_t local_ns_offset_;  // set in LocalCalibrate
    std::atomic<int64_t> post_calibrate_offset_;
    int64_t global_ns_offset_ = 0;
    int64_t measured_global_ns_offset_;
    uint64_t global_calibrate_applied_ns_ = 0;
    double system_error_fix_rate_ = 1.0;
    uint64_t global_min_ns_ = 0;  // set in GlobalSystemErrorCalculate
    uint64_t init_ns_;

    uint64_t last_ts_ = 0;

    struct DiffPosPair {
        DiffPosPair(int64_t _diff, int _pos) : diff(_diff), pos(_pos) {}
        int64_t diff;
        int pos;
        bool operator< (const DiffPosPair& _r) const {
            if (diff < _r.diff)
                return true;
            return pos < _r.pos;
        }
    };

 public:
    static inline uint64_t rdtsc() {
        return __builtin_ia32_rdtsc();
    }

    static inline uint64_t rdsysns() {
        timespec ts;
        ::clock_gettime(CLOCK_REALTIME, &ts);
        return ts.tv_sec * 1000000000 + ts.tv_nsec;
    }

    static inline void sync_time(uint64_t& tsc, uint64_t& ns) {
        static constexpr int N = 500;

        uint64_t tscs[N + 1];
        uint64_t nses[N + 1];

        tscs[0] = rdtsc();
        for (int i = 1; i <= N; i++) {
            nses[i] = rdsysns();
            tscs[i] = rdtsc();
        }

        int best = 1;
        for (int i = 2; i <= N; i++)
            if (tscs[i] - tscs[i - 1] < tscs[best] - tscs[best - 1])
                best = i;

        tsc = (tscs[best] + tscs[best - 1]) >> 1;  // get avg tsc
        ns = nses[best];
    }

    static inline bool BindToLogicalCore(int logical_core) {
      cpu_set_t my_set;
      CPU_ZERO(&my_set);
      CPU_SET(logical_core, &my_set);
      if (sched_setaffinity(0, sizeof(cpu_set_t), &my_set)) {
            std::cerr << "sched_setaffinity error: " << strerror(errno) << std::endl;
            assert(false);
            return false;
      }
      return true;
    }
};
