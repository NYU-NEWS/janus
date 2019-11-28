#include <utility>

#include <functional>
#include <iostream>
#include "../base/all.hpp"
#include "coroutine.h"
#include "reactor.h"

namespace rrr {

Coroutine::Coroutine(std::function<void()> func) : func_(std::move(func)), status_(INIT) {
}

Coroutine::~Coroutine() {
  verify(up_boost_coro_task_ != nullptr);
//  verify(0);
}

void Coroutine::BoostRunWrapper(boost_coro_yield_t& yield) {
  boost_coro_yield_ = yield;
  verify(func_);
  auto reactor = Reactor::GetReactor();
//  reactor->coros_;
  while (true) {
    auto sz = reactor->coros_.size();
    verify(sz > 0);
    func_();
    func_ = {};
    status_ = FINISHED;
    yield();
  }
}

void Coroutine::Run() {
  verify(!up_boost_coro_task_);
  verify(status_ == INIT);
  status_ = STARTED;
  auto reactor = Reactor::GetReactor();
//  reactor->coros_;
  auto sz = reactor->coros_.size();
  verify(sz > 0);
  up_boost_coro_task_ = make_unique<boost_coro_task_t>(
      std::bind(&Coroutine::BoostRunWrapper, this, std::placeholders::_1));
#ifdef USE_BOOST_COROUTINE1
  (*up_boost_coro_task_)();
#endif
}

void Coroutine::Yield() {
  verify(boost_coro_yield_);
  verify(status_ == STARTED || status_ == RESUMED);
  status_ = PAUSED;
  boost_coro_yield_.value()();
}

void Coroutine::Continue() {
  verify(status_ == PAUSED || status_ == RECYCLED);
  verify(up_boost_coro_task_);
  status_ = RESUMED;
  (*up_boost_coro_task_)();
  // some events might have been triggered from last coroutine,
  // but you have to manually call the scheduler to loop.
}

bool Coroutine::Finished() {
  return status_ == FINISHED;
}

} // namespace rrr
