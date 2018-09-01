
#include "coroutine.h"
#include "event.h"
#include "reactor.h"
#include "epoll_wrapper.h"

namespace rrr {

void Event::Wait() {
  if (IsReady()) {
    status_ = READY;
    return;
  } else {
    verify(status_ == INIT);
    status_= DEBUG;
    // the event may be created in a different coroutine.
    // this value is set when wait is called.
    // for now only one coroutine can wait on an event.
    auto sp_coro = Coroutine::CurrentCoroutine();
    verify(_dbg_p_scheduler_ == nullptr);
    _dbg_p_scheduler_ = Reactor::GetReactor().get();
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
    } else if (status_ == WAIT) {
//      AppEngine::CurrentScheduler()->AddReadyEvent(*this);
      auto sp_coro = wp_coro_.lock();
      verify(sp_coro);
      verify(status_ != DEBUG);
      auto sched = Reactor::GetReactor();
      verify(sched.get() == _dbg_p_scheduler_);
      verify(sched->__debug_set_all_coro_.count(sp_coro.get()) > 0);
      verify(sched->yielded_coros_.count(sp_coro) > 0);
      status_ = READY;
    } else {
      // could be a multi condition event
      Log_debug("event status not init or wait");
    }
    status_ = READY;
    return true;
  }
  return false;
}

Event::Event(std::shared_ptr<rrr::Coroutine> coro) {
  if (!coro) {
    coro = Coroutine::CurrentCoroutine();
  }
  verify(coro);
  wp_coro_ = coro;
}

bool IntEvent::TestTrigger() {
  verify(status_ <= WAIT);
  if (value_ == target_) {
    if (status_ == INIT) {
      // do nothing until wait happens.
    } else if (status_ == WAIT) {
      // AppEngine::CurrentScheduler()->AddReadyEvent(*this);
    } else {
      verify(0);
    }
    status_ = READY;
    return true;
  }
  return false;
}


} // namespace rrr
