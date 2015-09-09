
//#include <random>
//#include <thread>
//
//#include "base/all.hpp"
//#include "deptran/all.h"
//
//using namespace rococo;
//
//#define N_RAND_THREAD (100)
//#define N_RAND_RAND (100000000)
//
//TEST(RandomGenerator, std_rand_sep_perf) {
//    /*unsigned seed = */std::chrono::system_clock::now().time_since_epoch().count();
//
//
//    base::Timer timer;
//    timer.start();
//    // test for 10 thread, each thread
//    std::vector<std::thread> threads;
//    for (int i = 0; i < N_RAND_THREAD; i++) {
//	threads.push_back(std::thread([] () {
//		    std::mt19937 ra;
//		    for (int j = 0; j < N_RAND_RAND; j++) {
//			ra();
//		    }
//		}));
//    }
//
//    for (auto &t: threads) {
//	t.join();
//    }
//
//    timer.stop();
//
//    Log::info("std random performance: %f", timer.elapsed());
//}
//
//TEST(RandomGenerator, std_rand_perf) {
//    /*unsigned seed = */std::chrono::system_clock::now().time_since_epoch().count();
//    std::mt19937 ra;
//
//    base::Timer timer;
//    timer.start();
//    // test for 10 thread, each thread
//    std::vector<std::thread> threads;
//    for (int i = 0; i < N_RAND_THREAD; i++) {
//	threads.push_back(std::thread([&ra] () {
//		    for (int j = 0; j < N_RAND_RAND; j++) {
//			ra();
//		    }
//		}));
//    }
//
//    for (auto &t: threads) {
//	t.join();
//    }
//
//    timer.stop();
//
//    Log::info("std random performance: %f", timer.elapsed());
//}
//
//TEST(RandomGenerator, rand_performance) {
//    /*unsigned seed = */std::chrono::system_clock::now().time_since_epoch().count();
//    std::mt19937 ra;
//
//    base::Timer timer;
//    timer.start();
//    // test for 10 thread, each thread
//    std::vector<std::thread> threads;
//    for (int i = 0; i < N_RAND_THREAD; i++) {
//	threads.push_back(std::thread([&ra] () {
//		    for (int j = 0; j < N_RAND_RAND; j++) {
//			RandomGenerator::rand(0, 1000);
//		    }
//		}));
//    }
//
//    for (auto &t: threads) {
//	t.join();
//    }
//
//    timer.stop();
//
//    Log::info("random generator performance: %f", timer.elapsed());
//}
//
//
//TEST(RandomGenerator, rand) {
//    int r, pr;
//    EXPECT_EQ(0, RandomGenerator::rand(0, 0));
//    EXPECT_EQ(0, RandomGenerator::rand(0, 0));
//    EXPECT_EQ(0, RandomGenerator::rand(0, 0));
//    EXPECT_EQ(0, RandomGenerator::rand(0, 0));
//    r = RandomGenerator::rand(0, 1);
//    EXPECT_LE(0, r);
//    EXPECT_LE(r, 1);
//    r = RandomGenerator::rand(0, 1);
//    EXPECT_LE(0, r);
//    EXPECT_LE(r, 1);
//    r = RandomGenerator::rand(0, 1);
//    EXPECT_LE(0, r);
//    EXPECT_LE(r, 1);
//    r = RandomGenerator::rand(0, 1);
//    EXPECT_LE(0, r);
//    EXPECT_LE(r, 1);
//    r = RandomGenerator::rand(0, 1);
//    EXPECT_LE(0, r);
//    EXPECT_LE(r, 1);
//    r = RandomGenerator::rand(1, 3);
//    EXPECT_LE(1, r);
//    EXPECT_LE(r, 3);
//    r = RandomGenerator::rand(1, 3);
//    EXPECT_LE(1, r);
//    EXPECT_LE(r, 3);
//    r = RandomGenerator::rand(1, 3);
//    EXPECT_LE(1, r);
//    EXPECT_LE(r, 3);
//    r = RandomGenerator::rand(1, 3);
//    EXPECT_LE(1, r);
//    EXPECT_LE(r, 3);
//    r = RandomGenerator::rand(1, 3);
//    EXPECT_LE(1, r);
//    EXPECT_LE(r, 3);
//    r = RandomGenerator::rand(1, 3);
//    EXPECT_LE(1, r);
//    EXPECT_LE(r, 3);
//    r = RandomGenerator::rand(1, 3);
//    EXPECT_LE(1, r);
//    EXPECT_LE(r, 3);
//    r = RandomGenerator::rand(1, 3);
//    EXPECT_LE(1, r);
//    EXPECT_LE(r, 3);
//    r = RandomGenerator::rand(1, 3);
//    EXPECT_LE(1, r);
//    EXPECT_LE(r, 3);
//    r = RandomGenerator::rand(1, 3);
//    EXPECT_LE(1, r);
//    EXPECT_LE(r, 3);
//    r = RandomGenerator::rand(100, 10000);
//    EXPECT_LE(100, r);
//    EXPECT_LE(r, 10000);
//    pr = r;
//    r = RandomGenerator::rand(100, 10000);
//    EXPECT_LE(100, r);
//    EXPECT_LE(r, 10000);
//    EXPECT_NEQ(r, pr);
//    pr = r;
//    r = RandomGenerator::rand(100, 10000);
//    EXPECT_LE(100, r);
//    EXPECT_LE(r, 10000);
//    EXPECT_NEQ(r, pr);
//    pr = r;
//    r = RandomGenerator::rand(100, 10000);
//    EXPECT_LE(100, r);
//    EXPECT_LE(r, 10000);
//    EXPECT_NEQ(r, pr);
//    pr = r;
//    r = RandomGenerator::rand(100, 10000);
//    EXPECT_LE(100, r);
//    EXPECT_LE(r, 10000);
//    EXPECT_NEQ(r, pr);
//    pr = r;
//    r = RandomGenerator::rand(100, 10000);
//    EXPECT_LE(100, r);
//    EXPECT_LE(r, 10000);
//    EXPECT_NEQ(r, pr);
//    pr = r;
//    r = RandomGenerator::rand(100, 10000);
//    EXPECT_LE(100, r);
//    EXPECT_LE(r, 10000);
//    EXPECT_NEQ(r, pr);
//}
//
//TEST(RandomGenerator, rand_double) {
//    double r;
//    r = RandomGenerator::rand(100.0, 10000.0);
//    EXPECT_LE(100.0, r);
//    EXPECT_LE(r, 10000.0);
//    r = RandomGenerator::rand(100.0, 10000.0);
//    EXPECT_LE(100.0, r);
//    EXPECT_LE(r, 10000.0);
//    r = RandomGenerator::rand(100.0, 10000.0);
//    EXPECT_LE(100.0, r);
//    EXPECT_LE(r, 10000.0);
//    r = RandomGenerator::rand(100.0, 10000.0);
//    EXPECT_LE(100.0, r);
//    EXPECT_LE(r, 10000.0);
//    r = RandomGenerator::rand(100.0, 10000.0);
//    EXPECT_LE(100.0, r);
//    EXPECT_LE(r, 10000.0);
//}
//
//#define N_THREAD 10
//int num_generate = 1;
//int min = 0;
//int max = 100000;
//std::set<int> rand_set;
//pthread_mutex_t mutex;
//
//void *rand_helper(void *) {
//    for (int i = 0; i < num_generate; i++) {
//        int r = RandomGenerator::rand(min, max);
//        Log::info("rand: %d", r);
//        pthread_mutex_lock(&mutex);
//        rand_set.insert(r);
//        pthread_mutex_unlock(&mutex);
//    }
//    return NULL;
//}
//
//TEST(RandomGenerator, rand_multhread) {
//    int i = 0;
//    pthread_mutex_init(&mutex, NULL);
//    pthread_t th[N_THREAD];
//    for (i = 0; i < N_THREAD; i++)
//        pthread_create(th + i, NULL, &rand_helper, NULL);
//
//    for (i = 0; i < N_THREAD; i++)
//        pthread_join(th[i], NULL);
//    pthread_mutex_destroy(&mutex);
//    EXPECT_EQ(rand_set.size(), num_generate * N_THREAD);
//}
//
//TEST(RandomGenerator, percentage_true) {
//    int num_true = 0;
//    int i = 0;
//    for (; i < 1000; i++) {
//        if (RandomGenerator::percentage_true(70))
//            num_true++;
//    }
//    EXPECT_LE(num_true, 750);
//    EXPECT_LE(650, num_true);
//
//    num_true = 0;
//    for (i = 0; i < 1000000; i++) {
//        if (RandomGenerator::percentage_true(1))
//            num_true++;
//    }
//    EXPECT_LE(num_true, 11000);
//    EXPECT_LE(9000, num_true);
//
//    num_true = 0;
//    for (i = 0; i < 1000; i++) {
//        if (RandomGenerator::percentage_true(70.0))
//            num_true++;
//    }
//    EXPECT_LE(num_true, 750);
//    EXPECT_LE(650, num_true);
//
//    num_true = 0;
//    for (i = 0; i < 1000000; i++) {
//        if (RandomGenerator::percentage_true(1.0))
//            num_true++;
//    }
//    EXPECT_LE(num_true, 11000);
//    EXPECT_LE(9000, num_true);
//
//    num_true = 0;
//    for (i = 0; i < 10000000; i++) {
//        if (RandomGenerator::percentage_true(100))
//            num_true++;
//    }
//    EXPECT_EQ(num_true, 10000000);
//
//    num_true = 0;
//    for (i = 0; i < 10000000; i++) {
//        if (RandomGenerator::percentage_true(100.0))
//            num_true++;
//    }
//    EXPECT_EQ(num_true, 10000000);
//
//    num_true = 0;
//    for (i = 0; i < 10000000; i++) {
//        if (RandomGenerator::percentage_true(0))
//            num_true++;
//    }
//    EXPECT_EQ(num_true, 0);
//
//    num_true = 0;
//    for (i = 0; i < 10000000; i++) {
//        if (RandomGenerator::percentage_true(0.0))
//            num_true++;
//    }
//    EXPECT_EQ(num_true, 0);
//
//}
//
//TEST(RandomGenerator, weighted_select) {
//    unsigned long int times = 5000000;
//    std::vector<double> vec;
//    vec.resize(3);
//    vec[0] = 1.3;
//    vec[1] = 2.5;
//    vec[2] = 3.9;
//    double sum = vec[0] + vec[1] + vec[2];
//    std::vector<unsigned long int> ret_vec;
//    ret_vec.resize(3);
//    unsigned long int i = 0;
//    while (i++ < times)
//        ret_vec[RandomGenerator::weighted_select(vec)]++;
//
//    EXPECT_LT(ret_vec[0], vec[0] / sum * times * 1.1);
//    EXPECT_GT(ret_vec[0], vec[0] / sum * times * 0.9);
//
//    EXPECT_LT(ret_vec[1], vec[1] / sum * times * 1.1);
//    EXPECT_GT(ret_vec[1], vec[1] / sum * times * 0.9);
//
//    EXPECT_LT(ret_vec[2], vec[2] / sum * times * 1.1);
//    EXPECT_GT(ret_vec[2], vec[2] / sum * times * 0.9);
//}
