
#include <functional>
#include "../base/all.hpp"
#include "engine.h"
#include "coroutine.h"
#include "event.h"

namespace rrr {

thread_local std::shared_ptr<AppEngine> sp_app_engine_th_{};
thread_local std::shared_ptr<Coroutine> sp_running_coro_th_{};

std::shared_ptr<Coroutine> Coroutine::CurrentCoroutine() {
  verify(sp_running_coro_th_);
  return sp_running_coro_th_;
}

std::shared_ptr<Coroutine>
Coroutine::CreateRun(const std::function<void()> &func) {
  auto sched = AppEngine::GetEngine();
  auto coro = sched->CreateRunCoroutine(func);
  // some events might be triggered in the last coroutine.
  sched->Loop();
  return coro;
}

std::shared_ptr<AppEngine>
AppEngine::GetEngine() {
  if (!sp_app_engine_th_) {
    Log_debug("create a coroutine scheduler");
    sp_app_engine_th_.reset(new AppEngine);
  }
  return sp_app_engine_th_;
}

std::shared_ptr<Coroutine>
AppEngine::CreateRunCoroutine(const std::function<void()> &func) {
  std::shared_ptr<Coroutine> sp_coro(new Coroutine(func));
  __debug_set_all_coro_.insert(sp_coro.get());
//  verify(!curr_coro_); // Create a coroutine from another?
  auto sp_old_coro = sp_running_coro_th_;
  sp_running_coro_th_ = sp_coro;
  sp_coro->Run();
  if (!sp_coro->Finished()) {
    // got yielded.
    yielded_coros_.insert(sp_coro);
  }
  // yielded or finished, reset to old coro.
  sp_running_coro_th_ = sp_old_coro;
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
  verify(!sp_running_coro_th_);
  auto sp_old_coro = sp_running_coro_th_;
  sp_running_coro_th_ = sp_coro;
  verify(!sp_running_coro_th_->Finished());
  sp_running_coro_th_->Continue();
  if (sp_running_coro_th_->Finished()) {
    yielded_coros_.erase(sp_running_coro_th_);
  }
  sp_running_coro_th_ = sp_old_coro;
}

} // namespace rrr
