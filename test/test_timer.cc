#include "rrr.hpp"

#include "deptran/all.h"
#include "time.h"

using namespace rococo;
using namespace rrr;

TEST(Time, perf) {

    int n = 1000000;

    Timer t;

    uint64_t tmp = 0;
    t.start();

    for (int i = 0; i < n; i++) {
	tmp += rrr::Time::now();
    }

    t.stop();
    Log_info("time now rate: %f per sec, tmp: %lu", n / t.elapsed(), tmp);
}

TEST(Timer, loop) {
    int k = 0;
    unsigned long t = time(NULL);
    TIMER_LOOP(3) {
        k++;
    }
    t = time(NULL) - t;
    EXPECT_EQ(t, 3);
}

TEST(Timer, IF) {
    int k = 0;
    unsigned long t = time(NULL);
    TIMER_SET(3);
    while (1) {
        TIMER_IF_NOT_END {
        }
        else
            break;
        k++;
    }

    t = time(NULL) - t;
    EXPECT_EQ(t, 3);
}
