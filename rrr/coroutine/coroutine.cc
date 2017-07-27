#include <functional>
#include <iostream>
#include "../base/all.hpp"
#include "coroutine.h"
#include "scheduler.h"

namespace rrr {

Coroutine::Coroutine(const std::function<void()>& func) : func_(func) {
  finished_ = false;
  verify(!finished_);
  sched_ = CoroScheduler::CurrentScheduler();
}

Coroutine::~Coroutine() {
  verify(up_boost_coro_task_ != nullptr);
}

void Coroutine::BoostRunWrapper(boost_coro_yield_t &yield) {
  boost_coro_yield_ = yield;
//  while (true) {
    verify(func_);
    func_();
    finished_ = true;
    func_ = {};
  boost_coro_yield_.reset();
//    yield(); // for potential reuse in future.
//  }
}

void Coroutine::Run() {
  verify(!up_boost_coro_task_);
  up_boost_coro_task_ = std::make_unique<boost_coro_task_t>(
      std::bind(&Coroutine::BoostRunWrapper, this, std::placeholders::_1));
}

void Coroutine::Yield() {
  verify(boost_coro_yield_);
  boost_coro_yield_.value()();
}

void Coroutine::Continue() {
  verify(up_boost_coro_task_);
  (*up_boost_coro_task_)();
}

bool Coroutine::Finished() {
  return finished_;
}

} // namespace rrr
