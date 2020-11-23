#include <utility>

#include <functional>
#include <iostream>
#include <boost/coroutine2/protected_fixedsize_stack.hpp>
#include "../base/all.hpp"
#include "coroutine.h"
#include "reactor.h"


namespace rrr {

Coroutine::Coroutine(std::function<void()> func) : func_(func), status_(INIT) {
}

Coroutine::~Coroutine() {
  verify(up_boost_coro_task_);
  up_boost_coro_task_.reset();
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
    verify(func_);
    func_();
//    func_ = nullptr; // Can be swapped out here?
    status_ = FINISHED;
    Reactor::GetReactor()->n_active_coroutines_--;
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
//  up_boost_coro_task_ = make_shared<boost_coro_task_t>(

  const auto x = new boost_coro_task_t(
#ifdef USE_PROTECTED_STACK
      boost::coroutines2::protected_fixedsize_stack(),
#endif
      std::bind(&Coroutine::BoostRunWrapper, this, std::placeholders::_1)
//    [this] (boost_coro_yield_t& yield) {
//      this->BoostRunWrapper(yield);
//    }
      );
  verify(up_boost_coro_task_ == nullptr);
  up_boost_coro_task_.reset(x);
#ifdef USE_BOOST_COROUTINE1
  (*up_boost_coro_task_)();
#endif
}

void Coroutine::Yield() {
  verify(boost_coro_yield_);
  verify(status_ == STARTED || status_ == RESUMED);
  status_ = PAUSED;
  Reactor::GetReactor()->n_active_coroutines_--;
  boost_coro_yield_.value()();
}

void Coroutine::Continue() {
  verify(status_ == PAUSED || status_ == RECYCLED);
  verify(up_boost_coro_task_);
  status_ = RESUMED;
  auto& r = *up_boost_coro_task_;
  verify(r);
  r();
  // some events might have been triggered from last coroutine,
  // but you have to manually call the scheduler to loop.
}

bool Coroutine::Finished() {
  return status_ == FINISHED;
}

} // namespace rrr
