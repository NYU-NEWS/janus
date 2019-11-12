
#include <functional>
#include <thread>
#include "coroutine.h"
#include "event.h"
#include "reactor.h"
#include "epoll_wrapper.h"

namespace rrr {
using std::function;

void Event::Wait() {
//  verify(__debug_creator); // if this fails, the event is not created by reactor.

  verify(Reactor::sp_reactor_th_);
  verify(Reactor::sp_reactor_th_->thread_id_ == std::this_thread::get_id());
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
//    verify(sp_coro);
//    verify(_dbg_p_scheduler_ == nullptr);
//    _dbg_p_scheduler_ = Reactor::GetReactor().get();
    auto& events = Reactor::GetReactor()->waiting_events_;
    events.push_back(shared_from_this());
    wp_coro_ = sp_coro;
    status_ = WAIT;
    sp_coro->Yield();
  }
}

bool Event::Test() {
  verify(__debug_creator); // if this fails, the event is not created by reactor.
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
    } else if (status_ == READY) {
      // This could happen for a quorum event.
      Log_debug("event status ready, triggered?");
    } else if (status_ == DONE) {
      // do nothing
    } else {
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
