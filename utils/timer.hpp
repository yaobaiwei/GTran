/*
 * timer.hpp
 *
 *  Created on: May 13, 2018
 *      Author: Hongzhi Chen
 */

#ifndef TIMER_HPP_
#define TIMER_HPP_

#include <emmintrin.h>
#include <sys/time.h>
#include <unistd.h>
#include <stdint.h>
#include <string>

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
            _mm_pause(); // a busy-wait loop
    }

	enum TIMERS
	{
		MASTER_TIMER = 0,
		WORKER_TIMER = 1,
		SERIALIZATION_TIMER = 2,
		TRANSFER_TIMER = 3,
		COMMUNICATION_TIMER = 4
	};

	static const int N_Timers = 20; //currently, 10 timers are available
	static double _timers[N_Timers]; // timers
	static double _acc_time[N_Timers]; // accumulated time

	static void init_timers();

	static double get_current_time();

	//currently, only 4 timers are used, others can be defined by users

	static void start_timer(int i);

	static void reset_timer(int i);

	static void stop_timer(int i);

	static double get_timer(int i);

	static void print_timer(std::string str, int i);
};

#endif /* TIMER_HPP_ */
