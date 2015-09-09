//#include <string>
//#include <unistd.h>
//#include <thread>
//#include <fcntl.h>
//#include "rrr.hpp"
//
//TEST(io_wait, simple) {
//
//    const int num_th = 1;
//    std::thread *th[num_th];
//    for (int j = 0; j < num_th; j++) {
//        th[j] = new std::thread([j] () {
//            char filename[100];
//            sprintf(filename, "/scratch1/ycui/test_%d", j);
//            int fd = open(filename, O_RDWR | O_CREAT, 00644);
//            int i = 0;
//            char buf[100];
//            uint64_t t0 = rrr::Time::now();
//            while (i < 100000) {
//                if (i % 2000 == 0) {
//                    uint64_t t1 = rrr::Time::now();
//                    rrr::Log::info("time: %lu", t1 - t0);
//                    t0 = t1;
//                    rrr::Log::info("cpu utilization: %lf", rrr::CPUInfo::cpu_stat());
//                }
//                int ret = write(fd, buf, 100);
//                verify(ret == 100);
//#ifndef __APPLE__
//                fdatasync(fd);
//#endif
//                i++;
//                sleep(0);
//            }
//        });
//    }
//
//    const int num_th1 = 1;
//    std::thread *comp_th[num_th1];
//    for (int jj = 0; jj < num_th1; jj++) {
//        comp_th[jj] = new std::thread([] () {
//                volatile int i = 0;
//                while (true) {
//                    volatile int j = 1;
//                    i = i + j;
//                    //if (i % 10000000 == 0) {
//                    //    rrr::Log::info("cpu utilization: %lf", rrr::CPUInfo::cpu_stat());
//                    //}
//                }
//                });
//    }
//
//    for (int j = 0; j < num_th; j++) {
//        th[j]->join();
//        delete th[j];
//    }
//    for (int jj = 0; jj < num_th1; jj++) {
//        comp_th[jj]->join();
//        delete comp_th[jj];
//    }
//}
