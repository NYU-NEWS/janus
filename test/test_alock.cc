
//#include <thread>
//#include "rrr.hpp"
//#include "deptran/all.h"
//
//using namespace rococo;
//
//TEST(alock, timeout_one) {
//    TimeoutALock al;
//    int i = 0;
//
//    auto id1 = al.lock([&i] () {
//        i = 1;
//    },
//    [&i] () {
//        i = 2;
//    }
//    );
//    EXPECT_EQ(id1, 1);
//
//    EXPECT_EQ(i, 1);
//    al.abort(id1);
//}
//
//TEST(alock, timeout_two) {
//    TimeoutALock al;
//    int i = 0;
//
//    auto id1 = al.lock([&i] (uint64_t id) {
//        i = 1;
//    },
//    [&i] () {
//        i = 2;
//    }
//    );
//    EXPECT_EQ(i, 1);
//
//    auto id2 = al.lock([&i] (uint64_t id) {
//        i = 3;
//    },
//    [&i] () {
//        i = 4;
//    }
//    );
//    EXPECT_EQ(i, 1);
//    al.abort(id1);
//    EXPECT_EQ(i, 3);
//    al.abort(id2);
//}
//
//TEST(alock, timeout) {
//    TimeoutALock al;
//    int i = 0;
//
//    auto id1 = al.lock([&i] () {
//        i = 1;
//    },
//    [&i] () {
//        i = 2;
//    }
//    );
//    EXPECT_EQ(i, 1);
//
//    auto id2 = al.lock([&i] () {
//        i = 3;
//    },
//    [&i] () {
//        i = 4;
//    }
//    );
//    EXPECT_EQ(i, 1);
//
//    rrr::Time::sleep(2 * ALOCK_TIMEOUT);
//    Log::info("lalala");
//
//    EXPECT_EQ(id1, 1);
//    EXPECT_EQ(id2, 2);
//
//    EXPECT_EQ(i, 4);
//    al.abort(id1);
//    //    EXPECT_EQ(i, 3);
//
//    EXPECT_EQ(i, 4); // request 2 should timeout.
//
//    al.abort(id2); // double abort is ok.
//}
//
//TEST(alock, timeout_perf) {
//    int max = 100000;
//    base::Timer timer;
//    timer.start();
//    for (int i = 0; i < max; i++) {
//    TimeoutALock al;
//    auto id = al.lock([](){}, [](){});
//    al.abort(id);
//    }
//    timer.stop();
//    Log::info("lock rates: %f", max * 1.0 / timer.elapsed());
//}
//
//
//TEST(alockgroup, timeout_simple) {
//    TimeoutALock al1, al2, al3;
//    ALockGroup alg1, alg2, alg3;
//
//    int i=0/*, j=0, k=0*/;
//
//    alg1.add(&al1);
//    alg1.lock_all([&i](){i=1;}, [&i](){i=2;});
//    EXPECT_EQ(i, 1);
//
//    alg2.add(&al1);
//    alg2.add(&al2);
//    alg2.lock_all([&i](){i=3;}, [&i](){i=4;});
//    rrr::Time::sleep(2 * ALOCK_TIMEOUT);
//    EXPECT_EQ(i, 4);
//
//    alg3.add(&al1);
//    alg3.add(&al2);
//    alg3.add(&al3);
//
//    alg1.unlock_all();
//
//    alg3.lock_all([&i](){i=5;}, [&i](){i=6;});
//    EXPECT_EQ(i, 5);
//    alg3.unlock_all();
//}
//
//
//TEST(alockgroup, timeout_complex) {
//    TimeoutALock al1, al2, al3;
//    ALockGroup alg1;
//    int i = 0;
//    alg1.add(&al1);
//    alg1.add(&al2);
//    alg1.lock_all([&i](){i=1;}, [&i](){i=2;});
//    alg1.add(&al3);
//
//    alg1.lock_all([&i](){i=3;}, [&i](){i=4;});
//    EXPECT_EQ(i, 3);
//}
//
//TEST(alock, wait_die_one) {
//    WaitDieALock wdal;
//
//    int w1 = 0;
//    uint64_t w1_id = wdal.lock([&w1] () {
//            w1 = 1;
//            },
//            [&w1] () {
//            w1 = -1;
//            },
//            ALock::WLOCK,
//            10);
//    EXPECT_EQ(w1, 1); // lock: w1
//
//    int r_locks[5];
//    for (int i = 0; i < 5; i++)
//        r_locks[i] = 0;
//    uint64_t r_locks_id[5];
//    for (int i = 0; i < 5; i++) {
//        r_locks_id[i] = wdal.lock([&r_locks, i] () {
//                r_locks[i] = 1;
//                },
//                [&r_locks, i] () {
//                r_locks[i] = -1;
//                },
//                ALock::RLOCK,
//                i + 7);
//    }
//    EXPECT_EQ(w1, 1);
//    EXPECT_EQ(r_locks[0], 0); // lock: w1, rlocks[0-2]
//    EXPECT_EQ(r_locks[1], 0);
//    EXPECT_EQ(r_locks[2], 0);
//    EXPECT_EQ(r_locks[3], -1);
//    EXPECT_EQ(r_locks[4], -1);
//
//    int w_locks[5];
//    for (int i = 0; i < 5; i++)
//        w_locks[i] = 0;
//    uint64_t w_locks_id[5];
//    for (int i = 0; i < 2; i++) {
//        w_locks_id[i] = wdal.lock([&w_locks, i] () {
//                w_locks[i] = 1;
//                },
//                [&w_locks, i] () {
//                w_locks[i] = -1;
//                },
//                ALock::WLOCK,
//                i + 6);
//    }
//    EXPECT_EQ(w1, 1);
//    EXPECT_EQ(r_locks[0], 0);
//    EXPECT_EQ(r_locks[1], 0);
//    EXPECT_EQ(r_locks[2], 0);
//    EXPECT_EQ(r_locks[3], -1);
//    EXPECT_EQ(r_locks[4], -1);
//    EXPECT_EQ(w_locks[0], 0); // lock: w1, rlocks[0-2], wlocks[0]
//    EXPECT_EQ(w_locks[1], -1);
//
//    int r1 = 0;
//    uint64_t r1_id = wdal.lock([&r1] () {
//            r1 = 1;
//            },
//            [&r1] () {
//            r1 = -1;
//            },
//            ALock::RLOCK,
//            1);
//    EXPECT_EQ(w1, 1);
//    EXPECT_EQ(r_locks[0], 0);
//    EXPECT_EQ(r_locks[1], 0);
//    EXPECT_EQ(r_locks[2], 0);
//    EXPECT_EQ(r_locks[3], -1);
//    EXPECT_EQ(r_locks[4], -1);
//    EXPECT_EQ(w_locks[0], 0); // lock: w1, rlocks[0-2], wlocks[0], r1
//    EXPECT_EQ(w_locks[1], -1);
//    EXPECT_EQ(r1, 0);
//
//    wdal.abort(w1_id);
//    EXPECT_EQ(w1, 1);
//    EXPECT_EQ(r_locks[0], 1);
//    EXPECT_EQ(r_locks[1], 1);
//    EXPECT_EQ(r_locks[2], 1);
//    EXPECT_EQ(r_locks[3], -1);
//    EXPECT_EQ(r_locks[4], -1);
//    EXPECT_EQ(w_locks[0], 0); // lock: rlocks[0-2], wlocks_id[0], r1
//    EXPECT_EQ(w_locks[1], -1);
//    EXPECT_EQ(r1, 0);
//
//    wdal.abort(w_locks_id[0]);
//    EXPECT_EQ(w1, 1);
//    EXPECT_EQ(r_locks[0], 1);
//    EXPECT_EQ(r_locks[1], 1);
//    EXPECT_EQ(r_locks[2], 1);
//    EXPECT_EQ(r_locks[3], -1);
//    EXPECT_EQ(r_locks[4], -1);
//    EXPECT_EQ(w_locks[0], -1); // lock: rlocks[0-2], r1
//    EXPECT_EQ(w_locks[1], -1);
//    EXPECT_EQ(r1, 1);
//
//    wdal.abort(r_locks_id[0]);
//    wdal.abort(r_locks_id[1]);
//    wdal.abort(r_locks_id[2]);
//    wdal.abort(r1_id);
//}
//
//TEST(alock, wound_one) {
//    WoundDieALock wdal;
//
//    int w1 = 0;
//    uint64_t w1_id = wdal.lock([&w1] () {
//            w1 = 1;
//            },
//            [&w1] () {
//            w1 = -1;
//            },
//            ALock::WLOCK,
//            10,
//            [&w1] () -> int {
//            w1 = -2; // wound
//            return 1; // can't wound
//            });
//    EXPECT_EQ(w1, 1); // lock: w1
//
//    int r_locks[5];
//    for (int i = 0; i < 5; i++)
//        r_locks[i] = 0;
//    //uint64_t r_locks_id[5];
//    for (int i = 4; i >= 0; i--) {
//        /*r_locks_id[i] = */wdal.lock([&r_locks, i] () {
//                r_locks[i] = 1;
//                },
//                [&r_locks, i] () {
//                r_locks[i] = -1;
//                },
//                ALock::RLOCK,
//                i + 7,
//                [&r_locks, i] () -> int {
//                r_locks[i] = -2; // wound
//                return 0;
//                });
//        if (i >= 4) {
//            EXPECT_EQ(w1, 1);
//        }
//        else {
//            EXPECT_EQ(w1, -2);
//            w1 = 1;
//        }
//    }
//    EXPECT_EQ(r_locks[0], 0); // lock: w1, rlocks[0-4]
//    EXPECT_EQ(r_locks[1], 0);
//    EXPECT_EQ(r_locks[2], 0);
//    EXPECT_EQ(r_locks[3], 0);
//    EXPECT_EQ(r_locks[4], 0);
//
//    int w_locks[5];
//    for (int i = 0; i < 5; i++)
//        w_locks[i] = 0;
//    //uint64_t w_locks_id[5];
//    for (int i = 0; i < 2; i++) {
//        /*w_locks_id[i] = */wdal.lock([&w_locks, i] () {
//                w_locks[i] = 1;
//                },
//                [&w_locks, i] () {
//                w_locks[i] = -1;
//                },
//                ALock::WLOCK,
//                i + 10,
//                [&w_locks, i] () -> int {
//                w_locks[i] = -2; // wound
//                return 0;
//                });
//    }
//    EXPECT_EQ(w1, -2);
//    EXPECT_EQ(r_locks[0], 0);
//    EXPECT_EQ(r_locks[1], 0);
//    EXPECT_EQ(r_locks[2], 0);
//    EXPECT_EQ(r_locks[3], -1);
//    EXPECT_EQ(r_locks[4], -1);
//    EXPECT_EQ(w_locks[0], 0); // lock: w1, rlocks[0-2], wlocks[0,1]
//    EXPECT_EQ(w_locks[1], 0);
//
//    int r1 = 0;
//    /*uint64_t r1_id = */wdal.lock([&r1] () {
//            r1 = 1;
//            },
//            [&r1] () {
//            r1 = -1;
//            },
//            ALock::RLOCK,
//            11,
//            [&r1] () -> int {
//            r1 = -2;
//            return 0;
//            });
//    EXPECT_EQ(w1, -2);
//    EXPECT_EQ(r_locks[0], 0);
//    EXPECT_EQ(r_locks[1], 0);
//    EXPECT_EQ(r_locks[2], 0);
//    EXPECT_EQ(r_locks[3], -1);
//    EXPECT_EQ(r_locks[4], -1);
//    EXPECT_EQ(w_locks[0], 0); // lock: w1, rlocks[0-2], wlocks[0], r1
//    EXPECT_EQ(w_locks[1], -1);
//    EXPECT_EQ(r1, 0);
//
//    wdal.abort(w1_id);
//    EXPECT_EQ(w1, -2);
//    EXPECT_EQ(r_locks[0], 1);
//    EXPECT_EQ(r_locks[1], 1);
//    EXPECT_EQ(r_locks[2], 1);
//    EXPECT_EQ(r_locks[3], -1);
//    EXPECT_EQ(r_locks[4], -1);
//    EXPECT_EQ(w_locks[0], 0); // lock: rlocks[0-2], wlocks[0], r1
//    EXPECT_EQ(w_locks[1], -1);
//    EXPECT_EQ(r1, 0);
//
//    int r2 = 0;
//    uint64_t r2_id = wdal.lock([&r2] () {
//            r2 = 1;
//            },
//            [&r2] () {
//            r2 = -1;
//            },
//            ALock::RLOCK,
//            5,
//            [&r2] () -> int {
//            r2 = -2;
//            return 1;
//            });
//
//    EXPECT_EQ(w1, -2);
//    EXPECT_EQ(r_locks[0], 1);
//    EXPECT_EQ(r_locks[1], 1);
//    EXPECT_EQ(r_locks[2], 1);
//    EXPECT_EQ(r_locks[3], -1);
//    EXPECT_EQ(r_locks[4], -1);
//    EXPECT_EQ(w_locks[0], -1); // lock: rlocks[0-2], r1, r2
//    EXPECT_EQ(w_locks[1], -1);
//    EXPECT_EQ(r1, 1);
//    EXPECT_EQ(r2, 1);
//
//    int w2 = 0;
//    /*uint64_t w2_id = */wdal.lock([&w2] () {
//            w2 = 1;
//            },
//            [&w2] () {
//            w2 = -1;
//            },
//            ALock::WLOCK,
//            2,
//            [&w2] () -> int {
//            w2 = -2;
//            return 0;
//            });
//
//    EXPECT_EQ(w1, -2);
//    EXPECT_EQ(r_locks[0], -2);
//    EXPECT_EQ(r_locks[1], -2);
//    EXPECT_EQ(r_locks[2], -2);
//    EXPECT_EQ(r_locks[3], -1);
//    EXPECT_EQ(r_locks[4], -1);
//    EXPECT_EQ(w_locks[0], -1); // lock: r2, w2
//    EXPECT_EQ(w_locks[1], -1);
//    EXPECT_EQ(r1, -2);
//    EXPECT_EQ(r2, -2);
//    EXPECT_EQ(w2, 0);
//
//    wdal.abort(r2_id);
//
//    EXPECT_EQ(w1, -2);
//    EXPECT_EQ(r_locks[0], -2);
//    EXPECT_EQ(r_locks[1], -2);
//    EXPECT_EQ(r_locks[2], -2);
//    EXPECT_EQ(r_locks[3], -1);
//    EXPECT_EQ(r_locks[4], -1);
//    EXPECT_EQ(w_locks[0], -1); // lock: w2
//    EXPECT_EQ(w_locks[1], -1);
//    EXPECT_EQ(r1, -2);
//    EXPECT_EQ(r2, -2);
//    EXPECT_EQ(w2, 1);
//
//}
