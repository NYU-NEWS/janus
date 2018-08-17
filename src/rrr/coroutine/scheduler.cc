
#include <functional>
#include "../base/all.hpp"
#include "scheduler.h"
#include "coroutine.h"
#include "event.h"

namespace rrr {

thread_local std::shared_ptr<AppEngine> sp_coro_sched_{};
thread_local std::shared_ptr<Coroutine> curr_coro_{};

std::shared_ptr<Coroutine> Coroutine::CurrentCoroutine() {
  verify(curr_coro_);
  return curr_coro_;
}

std::shared_ptr<Coroutine>
Coroutine::CreateRun(const std::function<void()> &func) {
  auto sched = AppEngine::CurrentScheduler();
  auto coro = sched->CreateRunCoroutine(func);
  // some events might be triggered in the last coroutine.
  sched->Loop();
  return coro;
}

std::shared_ptr<AppEngine>
AppEngine::CurrentScheduler() {
  if (!sp_coro_sched_) {
    Log_debug("create a coroutine scheduler");
    sp_coro_sched_.reset(new AppEngine);
  }
  return sp_coro_sched_;
}

// void AppEngine::AddReadyEvent(Event& ev) {
//   boost::optional<Event&> e = ev;
//   ready_events_.push_back(e);
// }

std::shared_ptr<Coroutine>
AppEngine::CreateRunCoroutine(const std::function<void()> &func) {
  std::shared_ptr<Coroutine> sp_coro(new Coroutine(func));
  __debug_set_all_coro_.insert(sp_coro.get());
//  verify(!curr_coro_); // Create a coroutine from another?
  auto sp_old_coro = curr_coro_;
  curr_coro_ = sp_coro;
  sp_coro->Run();
  if (!sp_coro->Finished()) {
    // got yielded.
    yielded_coros_.insert(sp_coro);
  }
  // yielded or finished, reset to old coro.
  curr_coro_ = sp_old_coro;
  return sp_coro;
}


void AppEngine::Loop(bool infinite) {
  do {
    std::vector<std::unique_ptr<Event>> ready_events;
    for (auto it = events_.begin(); it != events_.end();) {
      Event& event = **it;
      if (event.status_ == Event::READY) {
        ready_events.push_back(std::move(*it));
        it = events_.erase(it);
      } else {
        it ++;
      }
    }
    for (auto& up_ev: ready_events) {
      auto& event = *up_ev;
      auto sp_coro = event.wp_coro_.lock();
      verify(sp_coro);
      verify(yielded_coros_.find(sp_coro) != yielded_coros_.end());
      RunCoro(sp_coro);
    }
  } while (infinite);
}

void AppEngine::RunCoro(std::shared_ptr<Coroutine> sp_coro) {
  verify(!curr_coro_);
  auto sp_old_coro = curr_coro_;
  curr_coro_ = sp_coro;
  verify(!curr_coro_->Finished());
  curr_coro_->Continue();
  if (curr_coro_->Finished()) {
    yielded_coros_.erase(curr_coro_);
  }
  curr_coro_ = sp_old_coro;
}

} // namespace rrr
