/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Chenghuan Huang (entityless@gmail.com)
*/

#pragma once

#include <bits/stdc++.h>
#include <mpi.h>
#include <time.h>

#include <set>

#define TIMESTAMP_MACHINE_ID_BITS 8

// Use only one thread should be used to get the timestamp
class MPITimestamper {
 public:
    static MPITimestamper* GetInstance() {
        static MPITimestamper instance;
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

    inline uint64_t GetGlobalPhysicalNSWithSystemErrorFix() const {
        uint64_t ret = GetGlobalPhysicalNS();
        return (ret - global_calibrate_applied_ns_) * system_error_fix_rate_ + global_calibrate_applied_ns_;
    }

    inline uint64_t GetGlobalPhysicalNS() const {
        // system error occurs
        return GetLocalPhysicalNS() - global_ns_offset_;
    }

    inline uint64_t GetGlobalSysNS() const {
        return rdsysns() - global_ns_offset_;
    }

    inline uint64_t GetLocalPhysicalNS() const {
        uint64_t tsc = rdtsc();
        return local_ns_offset_ + (int64_t)((int64_t)tsc * tsc_ghz_inv_);
    }

 private:
    MPITimestamper();

    MPI_Comm comm_, ori_comm_;
    int my_rank_, comm_sz_;
    double tsc_ghz_inv_;
    uint64_t local_ns_offset_;  // set in LocalCalibrate
    int64_t global_ns_offset_ = 0, measured_global_ns_offset_;
    uint64_t global_calibrate_applied_ns_ = 0;
    double system_error_fix_rate_ = 1.0;
    uint64_t global_min_ns_ = 0;  // set in GlobalSystemErrorCalculate
    uint64_t init_ns_;  // for debug usage

    mutable uint64_t last_nanosec_ = 0;
    mutable int nanosec_duplicate_count_ = 0;

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
