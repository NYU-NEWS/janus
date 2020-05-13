
#include <functional>
#include <thread>
#include "coroutine.h"
#include "event.h"
#include "reactor.h"
#include "epoll_wrapper.h"

namespace rrr {
using std::function;

void Event::Wait(uint64_t timeout) {
//  verify(__debug_creator); // if this fails, the event is not created by reactor.
  verify(Reactor::sp_reactor_th_);
  verify(Reactor::sp_reactor_th_->thread_id_ == std::this_thread::get_id());
  verify(status_ == INIT);
  if (IsReady()) {
    status_ = DONE; // no need to wait.
    return;
  } else {
//    if (status_ == WAIT) {
//      // this does not look right, fix later
//      Log_fatal("multiple waits on the same event; no support at the moment");
//    }
//    verify(status_ == INIT); // does not support multiple wait so far. maybe we can support it in the future.
//    status_= DEBUG;
    // the event may be created in a different coroutine.
    // this value is set when wait is called.
    // for now only one coroutine can wait on an event.
    auto sp_coro = Coroutine::CurrentCoroutine();
    verify(sp_coro);
//    verify(_dbg_p_scheduler_ == nullptr);
//    _dbg_p_scheduler_ = Reactor::GetReactor().get();
//    auto& waiting_events = Reactor::GetReactor()->waiting_events_; // Timeout???
//    waiting_events.push_back(shared_from_this());
#ifdef EVENT_TIMEOUT_CHECK
    if (timeout == 0) {
      timeout = 300 * 1000 * 1000;
//#ifdef SIMULATE_WAN
//      timeout = 600 * 1000 * 1000;
//#endif
    }
#endif
    if (timeout > 0) {
      wakeup_time_ = Time::now() + timeout;
      auto& timeout_events = Reactor::GetReactor()->timeout_events_;
      timeout_events.push_back(shared_from_this());
    }
    // TODO optimize timeout_events, sort by wakeup time.
//      auto it = timeout_events.end();
//      timeout_events.push_back(shared_from_this());
//      while (it != events.begin()) {
//        it--;
//        auto& it_event = *it;
//        if (it_event->wakeup_time_ < wakeup_time_) {
//          it++; // list insert happens before position.
//          break;
//        }
//      }
//      events.insert(it, shared_from_this());
    wp_coro_ = sp_coro;
    status_ = WAIT;
    verify(sp_coro->status_ != Coroutine::FINISHED && sp_coro->status_ != Coroutine::RECYCLED);
    sp_coro->Yield();
#ifdef EVENT_TIMEOUT_CHECK
    if (status_ == TIMEOUT) {
      verify(0);
    }
#endif
  }
}

bool Event::Test() {
  verify(__debug_creator); // if this fails, the event is not created by reactor.
  if (IsReady()) {
    if (status_ == INIT) {
      // wait has not been called, do nothing until wait happens.
    } else if (status_ == WAIT) {
      auto sp_coro = wp_coro_.lock();
      verify(sp_coro);
      verify(status_ != DEBUG);
//      auto sched = Reactor::GetReactor();
//      verify(sched.get() == _dbg_p_scheduler_);
//      verify(sched->__debug_set_all_coro_.count(sp_coro.get()) > 0);
//      verify(sched->coros_.count(sp_coro) > 0);
      status_ = READY;
      Reactor::GetReactor()->ready_events_.push_back(shared_from_this());
    } else if (status_ == READY) {
      // This could happen for a quorum event.
//      Log_debug("event status ready, triggered?");
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
  if (status_ > WAIT) {
    Log_debug("Event already triggered!");
    return false;
  }
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

int SharedIntEvent::Set(const int& v) {
  auto ret = value_;
  value_ = v;
  for (auto& sp_ev : events_) {
    if (sp_ev->status_ <= Event::WAIT) {
      if (sp_ev->target_ <= v) {
        sp_ev->Set(v);
      }
    }
  }
  return ret;
}

void SharedIntEvent::WaitUntilGreaterOrEqualThan(int x) {
  if (value_ >= x) {
    return;
  }
  auto sp_ev =  Reactor::CreateSpEvent<IntEvent>();
  sp_ev->value_ = value_;
  sp_ev->target_ = x;
  events_.push_back(sp_ev);
//  sp_ev->Wait(1000*1000*1000);
//  verify(sp_ev->status_ != Event::TIMEOUT);
  sp_ev->Wait();
}

void SharedIntEvent::Wait(function<bool(int v)> f) {
  if (f(value_)) {
    return;
  }
  auto sp_ev =  Reactor::CreateSpEvent<IntEvent>();
  sp_ev->value_ = value_;
  sp_ev->test_ = f;
  events_.push_back(sp_ev);

  sp_ev->Wait(1000*1000*1000);
  verify(sp_ev->status_ != Event::TIMEOUT);
}

} // namespace rrr
