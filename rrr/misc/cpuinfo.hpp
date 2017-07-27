#pragma once

#include "stdlib.h"
#include "stdio.h"
#include "string.h"
#include "sys/times.h"

#ifndef __APPLE__
#include "sys/vtimes.h"
#endif

namespace rrr {

class CPUInfo {
private:
    clock_t last_ticks_, last_user_ticks_, last_kernel_ticks_;
    //uint32_t num_processors_;
    CPUInfo() {
        //FILE* proc_cpuinfo;
        struct tms tms_buf;
        //char line[1024];

        last_ticks_ = times(&tms_buf);
        last_user_ticks_ = tms_buf.tms_utime;
        last_kernel_ticks_ = tms_buf.tms_stime;

        //proc_cpuinfo = fopen("/proc/cpuinfo", "r");
        //num_processors_ = 0;
        //while (fgets(line, 1024, proc_cpuinfo) != NULL)
        //    if (strncmp(line, "processor", 9) == 0)
        //        num_processors_++;
        //fclose(proc_cpuinfo);
    }

    /**
     * get current cpu utilization for the calling process
     * result is calculated as (sys_time + user_time) / total_time, since the
     * last recent call to this function
     *
     * return:  upon success, this function will return the utilization roughly
     *          between 0.0 to 1.0; otherwise, it will return a negative value
     */
    double get_cpu_stat() {
        struct tms tms_buf;
        clock_t ticks;
        double ret;

        ticks = times(&tms_buf);

        if (ticks <= last_ticks_/* || num_processors_ <= 0*/)
            return -1.0;

        ret = (tms_buf.tms_stime - last_kernel_ticks_) +
            (tms_buf.tms_utime - last_user_ticks_);
        ret /= (ticks - last_ticks_);
        //ret /= num_processors_;

        last_ticks_ = ticks;
        last_user_ticks_ = tms_buf.tms_utime;
        last_kernel_ticks_ = tms_buf.tms_stime;

        return ret;
    }

public:
    static double cpu_stat() {
        static CPUInfo cpu_info;
        return cpu_info.get_cpu_stat();
    }
};

}
