#include <functional>
#include "../base/all.hpp"
#include "coroutine.h"
#include "scheduler.h"

namespace rrr {

Coroutine::Coroutine(const std::function<void()>& func) : func_(func) {
  finished_ = false;
  verify(!finished_);
  sched_ = CoroScheduler::CurrentScheduler();
  // This runs it.
  up_boost_coro_task_.reset(new boost_coro_task_t(
      std::bind(&Coroutine::BoostRunWrapper, this, std::placeholders::_1)));
}

Coroutine::~Coroutine() {
  verify(up_boost_coro_task_ != nullptr);
}

void Coroutine::BoostRunWrapper(boost_coro_yield_t &yield) {
  boost_coro_yield_ = yield;
  while (true) {
    verify(func_);
    func_();
    finished_ = true;
    func_ = {};
    yield(); // for potential reuse in future.
  }
}

void Coroutine::Run(const std::function<void()> &func, bool defer) {
  // TODO
  verify(0);
  func_ = func;
  func_();
}

void Coroutine::Yield() {
  verify(boost_coro_yield_);
  boost_coro_yield_.value()();
}

void Coroutine::Continue() {
  verify(up_boost_coro_task_ != nullptr);
  (*up_boost_coro_task_)();
}

bool Coroutine::Finished() {
  return finished_;
}

} // namespace rrr
