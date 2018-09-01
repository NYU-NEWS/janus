#include <functional>
#include <iostream>
#include "../base/all.hpp"
#include "coroutine.h"
#include "reactor.h"

namespace rrr {

Coroutine::Coroutine(const std::function<void()>& func) : func_(func) {
  finished_ = false;
  verify(!finished_);
}

Coroutine::~Coroutine() {
  verify(up_boost_coro_task_ != nullptr);
}

void Coroutine::BoostRunWrapper(boost_coro_yield_t& yield) {
  boost_coro_yield_ = yield;
  verify(func_);
  func_();
  finished_ = true;
  func_ = {};
  boost_coro_yield_.reset();
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
  // some events might have been triggered from last coroutine,
  // but you have to manually call the scheduler to loop.
}

bool Coroutine::Finished() {
  return finished_;
}

} // namespace rrr
