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

#ifndef TIMER_HPP_
#define TIMER_HPP_

#include <emmintrin.h>
#include <sys/time.h>
#include <unistd.h>
#include <stdint.h>
#include <string>
#include <vector>

class timer {
 public:
    static uint64_t get_usec() {
        struct timespec tp;
        /* POSIX.1-2008: Applications should use the clock_gettime() function
           instead of the obsolescent gettimeofday() function. */
        /* NOTE: The clock_gettime() function is only available on Linux.
           The mach_absolute_time() function is an alternative on OSX. */
        clock_gettime(CLOCK_MONOTONIC, &tp);
        return ((tp.tv_sec * 1000 * 1000) + (tp.tv_nsec / 1000));
    }

    static void cpu_relax(int u) {
        int t = 166 * u;
        while ((t--) > 0)
            _mm_pause();  // a busy-wait loop
    }

    static void init_timers(int size);

    static void reset_timers();

    static double get_current_time();

    static void start_timer(int i);

    static void reset_timer(int i);

    static void stop_timer(int i);

    static double get_timer(int i);

    static void print_timer(std::string str, int i);

    static int N_Timers;

    static std::vector<double> _timers;    // timers
    static std::vector<double> _acc_time;  // accumulated time
};

#endif /* TIMER_HPP_ */
