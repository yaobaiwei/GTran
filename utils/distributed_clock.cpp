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

#include "distributed_clock.hpp"

DistributedClock::DistributedClock() {
}

void DistributedClock::Init(MPI_Comm comm, int local_calibrate_delay, int global_calibrate_loop, double sample_rate) {
    ori_comm_ = comm;
    MPI_Comm_dup(comm, &comm_);
    MPI_Comm_size(comm_, &comm_sz_);
    MPI_Comm_rank(comm_, &my_rank_);

    assert((comm_sz_ - 1 >> TIMESTAMP_MACHINE_ID_BITS) == 0);
    post_calibrate_offset_ = 0;
    
    LocalCalibrate(local_calibrate_delay);
    GlobalCalibrateMeasure(global_calibrate_loop, sample_rate);
    GlobalCalibrateApply();

    init_ns_ = GetLocalPhysicalNS();
}

uint64_t DistributedClock::GetTimestamp() const {
    uint64_t nano_sec = GetRefinedNS();

    assert((nano_sec >> (64 - 1 - TIMESTAMP_MACHINE_ID_BITS)) == 0);

    uint64_t ret = (nano_sec << TIMESTAMP_MACHINE_ID_BITS) + my_rank_;
    assert(last_ts_ < ret);

    // printf("[Worker%d], allocated timestamp: %lu\n", my_rank_, nano_sec);
    return ret;
}

uint64_t DistributedClock::GetTimestampWithSystemErrorFix() const {
    uint64_t nano_sec = GetRefinedNSWithSystemErrorFix();

    assert((nano_sec >> (64 - 1 - TIMESTAMP_MACHINE_ID_BITS)) == 0);

    uint64_t ret = (nano_sec << TIMESTAMP_MACHINE_ID_BITS) + my_rank_;
    assert(last_ts_ < ret);

    // printf("[Worker%d], allocated timestamp: %lu\n", my_rank_, nano_sec);
    return ret;
}

void DistributedClock::LocalCalibrate(int local_calibrate_delay) {
    // check if constant_tsc feature exists
    uint64_t init_tsc, init_ns;
    uint64_t delayed_tsc, delayed_ns;
    sync_time(init_tsc, init_ns);

    std::this_thread::sleep_for(std::chrono::seconds(local_calibrate_delay));

    sync_time(delayed_tsc, delayed_ns);

    tsc_ghz_inv_ = static_cast<double>((int64_t)(delayed_ns - init_ns)) / (int64_t)(delayed_tsc - init_tsc);
    local_ns_offset_ = init_ns - (int64_t)((int64_t)init_tsc * tsc_ghz_inv_);
}

void DistributedClock::GlobalCalibrateMeasure(int global_calibrate_loop, double sample_rate, bool with_sys_error_fix) {
    int64_t ns;
    int64_t remote_ns;
    MPI_Status mpi_status;

    int64_t ns_offsets[comm_sz_];
    ns_offsets[0] = 0;

    if (my_rank_ == 0) {
        int64_t nss[global_calibrate_loop + 1];
        int64_t remote_nss[global_calibrate_loop + 1];

        // send and recv with other processes
        for (int partner = 1; partner < comm_sz_; partner++) {
            for (int i = 0; i < global_calibrate_loop + 1; i++) {
                if (!with_sys_error_fix)
                    ns = GetGlobalPhysicalNS();
                else
                    ns = GetGlobalPhysicalNSWithSystemErrorFix();

                nss[i] = ns;
                MPI_Send(&ns, 1, MPI_DOUBLE, partner, 0, comm_);
                MPI_Recv(&remote_ns, 1, MPI_DOUBLE, partner, 0, comm_, &mpi_status);
                remote_nss[i] = remote_ns;
                // printf("ns = %ld, remote_ns = %ld, diff = %ld\n", ns, remote_ns, ns - remote_ns);
            }

            std::set<DiffPosPair> tmp_pair_set;
            for (int i = 0; i < global_calibrate_loop; i++) {
                tmp_pair_set.emplace(nss[i + 1] - nss[i], i);
            }

            int iter_len = sample_rate * global_calibrate_loop;
            if (iter_len < 1)
                iter_len = 1;

            int64_t send_recv_latency = 0;
            ns_offsets[partner] = 0;

            int iter_cnt = 0;

            // only sample those send-recv pair with lowest latencies
            for (auto v : tmp_pair_set) {
                if (iter_cnt == iter_len)
                    break;
                iter_cnt++;

                int64_t tmp_send_recv_latency = (nss[v.pos + 1] - nss[v.pos]) >> 1;
                int64_t tmp_ns_offset = remote_nss[v.pos] - nss[v.pos] - tmp_send_recv_latency;
                send_recv_latency += tmp_send_recv_latency;
                ns_offsets[partner] -= tmp_ns_offset;
                // printf("ns_offsets[partner] += %ld\n", tmp_ns_offset);
            }

            send_recv_latency /= iter_cnt;
            ns_offsets[partner] /= iter_cnt;

            // printf("partner = %d, latency = %d, offset = %d\n",
            //        partner, send_recv_latency, ns_offsets[partner]);

            MPI_Barrier(comm_);
        }
        // the ns offset should not be negative
        int64_t min_ns_offset = 0;

        for (int partner = 1; partner < comm_sz_; partner++)
            if (min_ns_offset > ns_offsets[partner])
                min_ns_offset = ns_offsets[partner];

        // if (!with_sys_error_fix)
        //     printf("%lu sec, without sys fix: \t", (GetLocalPhysicalNS() - init_ns_) / 1000000000);
        // else
        //     printf("%lu sec,    with sys fix: \t", (GetLocalPhysicalNS() - init_ns_) / 1000000000);

        // for (int partner = 1; partner < comm_sz_; partner++)
        //     printf("%ld\t\t", ns_offsets[partner]);
        // printf("\n");

        for (int i = 0; i < comm_sz_; i++) {
            ns_offsets[i] -= min_ns_offset;
        }
    } else {
        // send and recv with rank 0
        for (int partner = 1; partner < comm_sz_; partner++) {
            if (my_rank_ == partner) {
                for (int i = 0; i < global_calibrate_loop + 1; i++) {
                    MPI_Recv(&remote_ns, 1, MPI_DOUBLE, 0, 0, comm_, &mpi_status);
                    if (!with_sys_error_fix)
                        ns = GetGlobalPhysicalNS();
                    else
                        ns = GetGlobalPhysicalNSWithSystemErrorFix();
                    MPI_Send(&ns, 1, MPI_DOUBLE, 0, 0, comm_);
                }
            }
            MPI_Barrier(comm_);
        }
    }

    MPI_Scatter(ns_offsets, 1, MPI_DOUBLE, &measured_global_ns_offset_, 1, MPI_DOUBLE, 0, comm_);
    assert(measured_global_ns_offset_ >= 0);
}

void DistributedClock::GlobalCalibrateApply() {
    global_ns_offset_ += measured_global_ns_offset_;
    assert(global_ns_offset_ >= 0);
    global_calibrate_applied_ns_ = GetGlobalPhysicalNS();

    // get the global min physical timestamp
    MPI_Allreduce(&global_calibrate_applied_ns_, &global_min_ns_, 1, MPI_UINT64_T, MPI_MIN, comm_);
    // printf("[Worker%d] DistributedClock::GlobalCalibrateApply, off = %ld, %ld, global_min_ns_ = %lu\n", my_rank_, global_ns_offset_, measured_global_ns_offset_, global_min_ns_);
    MPI_Barrier(comm_);
}

void DistributedClock::GlobalSystemErrorCalculate(int global_calibrate_loop, int sleep_duration, double sample_rate) {
    if (my_rank_ == 0)
        printf("[DistributedClock] Measuring system error for, please wait for %d sec.\n", sleep_duration);

    GlobalCalibrateMeasure(global_calibrate_loop, sample_rate);
    uint64_t start_ns = GetGlobalPhysicalNS();
    int64_t start_tmp_global_ns_offset = measured_global_ns_offset_;
    std::this_thread::sleep_for(std::chrono::seconds(sleep_duration));
    GlobalCalibrateMeasure(global_calibrate_loop, sample_rate);
    uint64_t end_ns = GetGlobalPhysicalNS();
    int64_t end_tmp_global_ns_offset = measured_global_ns_offset_;

    // calculate system_error_fix_rate_
    // not so precise when sleep_duration is not large enough.
    system_error_fix_rate_ = 1.0 + (double)(end_tmp_global_ns_offset - start_tmp_global_ns_offset) / (end_ns - start_ns);
}
