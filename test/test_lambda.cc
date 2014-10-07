#include <stdio.h>
#include <time.h>
#include "rrr.hpp"

#include <functional>
#define CALL_TIMES 10000

uint64_t copy_func(const std::function<uint64_t(void)> &foo) {
    std::function<uint64_t(void)> boo = foo;
    return boo();
}

struct para_t {
    uint64_t a[1024];
};

struct para_t rand_para() {
    static uint64_t c = 0;
    struct para_t ret;
    for (uint64_t i = 0; i < 1024; i++)
        ret.a[i] = c++;

    return ret;
}

TEST(lambda, simple) {
    uint64_t i = 0;
    uint64_t t0 = rrr::Time::now();
    uint64_t ret;
    while (i++ < CALL_TIMES) {
        struct para_t para = rand_para();
        uint64_t k = copy_func([para] () -> uint64_t {
                return (uint64_t)para.a[1000];
                });
        ret = k;
    }
    uint64_t t1 = rrr::Time::now();

    Log_info("ret: %lu, time interval: %lf", ret, CALL_TIMES / ((double)(t1 - t0) / 1000 / 1000));

}

TEST(lambda, no_copy) {
    uint64_t i = 0;
    uint64_t t0 = rrr::Time::now();
    uint64_t ret;
    while (i++ < CALL_TIMES) {
        struct para_t para = rand_para();
        uint64_t k = [para] () -> uint64_t {
            return (uint64_t)para.a[1000];
        }();
        ret = k;
    }
    uint64_t t1 = rrr::Time::now();

    Log_info("ret: %lu, time interval: %lf", ret, 
	     CALL_TIMES / ((double)(t1 - t0) / 1000 / 1000));

}
