#include <functional>
#include "../base/all.hpp"
#include "coroutine.h"
#include "scheduler.h"

namespace rrr {

Coroutine::Coroutine(const std::function<void()>& func) : func_(func) {
  sched_ = Scheduler::CurrentScheduler();
//  auto func = [] (boost_coro_yield_t& yield) -> void {yield();};
//  boost_coro_task_t task(func);
//  task();
//  boost_coro_task_t task2(
//      std::bind(&Coroutine::BoostRunWrapper, this, std::placeholders::_1));
//  task2();
//  boost_coro_task_t* task3 = new boost_coro_task_t(
//      std::bind(&Coroutine::BoostRunWrapper, this, std::placeholders::_1));
//  (*task3)();
  boost_coro_task_ = new boost_coro_task_t(
      std::bind(&Coroutine::BoostRunWrapper, this, std::placeholders::_1));
}

Coroutine::~Coroutine() {
  verify(boost_coro_task_ != nullptr);
  delete boost_coro_task_;
}

void Coroutine::BoostRunWrapper(boost_coro_yield_t &yield) {
  boost_coro_yield_ = &yield;
  verify(func_);
  func_();
  yield();
}

void Coroutine::Run(const std::function<void()> &func, bool defer) {
  // TODO
  verify(0);
  func_ = func;
  func_();
}

void Coroutine::Yield() {
  verify(boost_coro_yield_ != nullptr);
  (*boost_coro_yield_)();
}

void Coroutine::Continue() {
  verify(boost_coro_task_ != nullptr);
  (*boost_coro_task_)();
}

} // namespace rrr
