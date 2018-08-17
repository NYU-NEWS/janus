#include <gtest/gtest.h>

#include <boost/coroutine/all.hpp>
#include <stdexcept>
#include <iostream>

#include "rrr/rrr.hpp"

using namespace std;
using namespace rrr;

//using boost::coroutines::coroutine;
//typedef coroutine<void>::pull_type boost_coro_task_t;
//typedef coroutine<void>::push_type boost_coro_yield_t;

//TEST(coroutine, hello) {
//  ASSERT_EQ(1, 1);
//  Coroutine::Create([] () {ASSERT_EQ(1, 1);});
////  Coroutine::Create([] () {ASSERT_NE(1, 1);});
//}
//
//void cooperative(boost_coro_yield_t &yield)
//{
//  yield();
//}

//class Cooperative {
// public:
//  void work(boost_coro_yield_t &yield) {
//    yield();
//  }
//};
//
//class CoroutineWrapper {
// public:
//  boost_coro_task_t *boost_coro_task_{nullptr};
//
//  void Work(boost_coro_yield_t &yield) {
//    yield();
//  }
//
//  CoroutineWrapper() {
//    boost_coro_task_ = new boost_coro_task_t(
//        std::bind(&CoroutineWrapper::Work, this, std::placeholders::_1));
//  }
//
//  void Run() {
//    (*boost_coro_task_)();
//  }
//
//  ~CoroutineWrapper() {
//    delete boost_coro_task_;
//  }
//};

//TEST(coroutine, boost) {
//  boost_coro_task_t source{cooperative};
//  source();
//  Cooperative o;
//  boost_coro_task_t* task = new boost_coro_task_t(
//      std::bind(&Cooperative::work, o, std::placeholders::_1));
//  (*task)();
//  delete task;
//  CoroutineWrapper* cw = new CoroutineWrapper();
//  cw->Run();
//  delete cw;
//
//}

#include "gtest/gtest.h"

TEST(CoroutineTest, helloworld) {
  Coroutine::CreateRun([] () {ASSERT_EQ(1, 1);});
  Coroutine::CreateRun([] () {ASSERT_NE(1, 2);});
}

TEST(CoroutineTest, yield) {
  int x = 0;
  auto coro1 = Coroutine::CreateRun([&x] () {
    x = 1;
    Coroutine::CurrentCoroutine()->Yield();
    x = 2;
  });
  ASSERT_EQ(x, 1);
  coro1->Continue();
  ASSERT_EQ(x, 2);
}

TEST(CoroutineTest, wait_die_lock) {
  WaitDieALock a;
  auto coro1 = Coroutine::CreateRun([&a] () {
    uint64_t req_id = a.Lock(0, ALock::WLOCK, 10);
    ASSERT_EQ(req_id, true);
    Coroutine::CurrentCoroutine()->Yield();
    Log_debug("aborting lock from coroutine 1.");
    a.abort(req_id);
  });

  int x = 0;
  auto coro2 = Coroutine::CreateRun([&] () {
    uint64_t req_id = a.Lock(0, ALock::WLOCK, 11);
    ASSERT_EQ(req_id, false);
    x = 1;
  });
  ASSERT_EQ(x, 1);

  int y = 0;
  auto coro3 = Coroutine::CreateRun([&] () {
    uint64_t req_id = a.Lock(0, ALock::WLOCK, 8);
    ASSERT_GT(req_id, 0);
    Log_debug("acquired lock from coroutine 3.");
    y = 1;
  });
  ASSERT_EQ(y, 0);
  coro1->Continue();
  AppEngine::CurrentScheduler()->Loop();
  ASSERT_EQ(y, 1);
}

TEST(SquareRootTest, PositiveNos) {
//  EXPECT_EQ (18.0, square-root (324.0));
//  EXPECT_EQ (25.4, square-root (645.16));
//  EXPECT_EQ (50.3321, square-root (2533.310224));
}

TEST (SquareRootTest, ZeroAndNegativeNos) {
//  ASSERT_EQ (0.0, square-root (0.0));
//  ASSERT_EQ (-1, square-root (-22.0));
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}