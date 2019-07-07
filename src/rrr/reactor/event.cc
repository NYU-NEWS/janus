
#include <functional>
#include "coroutine.h"
#include "event.h"
#include "reactor.h"
#include "epoll_wrapper.h"

namespace rrr {
using std::function;

void Event::Wait() {
  if (IsReady()) {
    status_ = DONE; // does not need to wait.
    return;
  } else {
    verify(status_ == INIT);
    status_= DEBUG;
    // the event may be created in a different coroutine.
    // this value is set when wait is called.
    // for now only one coroutine can wait on an event.
    auto sp_coro = Coroutine::CurrentCoroutine();
//    verify(_dbg_p_scheduler_ == nullptr);
//    _dbg_p_scheduler_ = Reactor::GetReactor().get();
    verify(sp_coro);
    wp_coro_ = sp_coro;
    status_ = WAIT;
    sp_coro->Yield();
  }
}

bool Event::Test() {
  if (IsReady()) {
    if (status_ == INIT) {
      // wait has not been called, do nothing until wait happens.
      status_ = DONE;
    } else if (status_ == WAIT) {
      auto sp_coro = wp_coro_.lock();
      verify(sp_coro);
      verify(status_ != DEBUG);
//      auto sched = Reactor::GetReactor();
//      verify(sched.get() == _dbg_p_scheduler_);
//      verify(sched->__debug_set_all_coro_.count(sp_coro.get()) > 0);
//      verify(sched->coros_.count(sp_coro) > 0);
      status_ = READY;
    } else {
      // TODO could be a multi condition event?
      Log_debug("event status not init or wait");
      verify(0);
    }
    return true;
  }
  return false;
}

Event::Event() {
  auto coro = Coroutine::CurrentCoroutine();
//  verify(coro);
  wp_coro_ = coro;
}

bool IntEvent::TestTrigger() {
  verify(status_ <= WAIT);
  if (value_ == target_) {
    if (status_ == INIT) {
      // do nothing until wait happens.
      status_ = DONE;
    } else if (status_ == WAIT) {
      status_ = READY;
    } else {
      verify(0);
    }
    return true;
  }
  return false;
}

void SharedIntEvent::Wait(function<bool(int v)> f) {
  auto sp_ev =  Reactor::CreateSpEvent<IntEvent>();
  sp_ev->test_ = f;
  events_.push_back(sp_ev);
  sp_ev->Wait();
}

} // namespace rrr
