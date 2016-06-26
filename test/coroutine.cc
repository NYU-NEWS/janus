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

TEST(coroutine, hello) {
  ASSERT_EQ(1, 1);
  Coroutine::Create([] () {ASSERT_EQ(1, 1);});
//  Coroutine::Create([] () {ASSERT_NE(1, 1);});
}

void cooperative(boost_coro_yield_t &yield)
{
  yield();
}

class Cooperative {
 public:
  void work(boost_coro_yield_t &yield) {
    yield();
  }
};

class CoroutineWrapper {
 public:
  boost_coro_task_t *boost_coro_task_{nullptr};

  void Work(boost_coro_yield_t &yield) {
    yield();
  }

  CoroutineWrapper() {
    boost_coro_task_ = new boost_coro_task_t(
        std::bind(&CoroutineWrapper::Work, this, std::placeholders::_1));
  }

  void Run() {
    (*boost_coro_task_)();
  }

  ~CoroutineWrapper() {
    delete boost_coro_task_;
  }
};

TEST(coroutine, boost) {
  boost_coro_task_t source{cooperative};
  source();
  Cooperative o;
  boost_coro_task_t* task = new boost_coro_task_t(
      std::bind(&Cooperative::work, o, std::placeholders::_1));
  (*task)();
  delete task;
  CoroutineWrapper* cw = new CoroutineWrapper();
  cw->Run();
  delete cw;

}
