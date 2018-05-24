/*
 * mtime.h
 *
 *  Created on: Dec 17, 2012
 *      Author: ms
 */
//
//#ifndef MTIME_H_
//#define MTIME_H_
//
//#include <time.h>
//
//#ifdef __MACH__
//#include<mach/clock.h>
//#include<mach/mach.h>
//#endif
//
//static int get_realtime(struct timespec *ts) {
//	#ifdef __MACH__
//	clock_serv_t cclock;
//	mach_timespec_t mts;
//	host_get_clock_service(mach_host_self(), CALENDAR_CLOCK,&cclock);
//	clock_get_time(cclock,&mts);
//	mach_port_deallocate(mach_task_self(), cclock);
//	ts->tv_sec = mts.tv_sec;
//	ts->tv_nsec = mts.tv_nsec;
//	#else
//	clock_gettime(CLOCK_REALTIME, ts);
//	#endif
//	return 0;
//}
//
//#endif /* MTIME_H_ */
