
#include "../base/all.hpp"
#include "scheduler.h"
#include "coroutine.h"
#include "event.h"

namespace rrr {

thread_local Scheduler coro_sched_{};
thread_local Coroutine* curr_coro_{nullptr};

Coroutine* Coroutine::CurrentCoroutine() {
  verify(curr_coro_ != nullptr);
  return curr_coro_;
}

void Coroutine::Create(std::function<void()> func) {
  auto sched = Scheduler::CurrentScheduler();
  auto coro = sched->GetCoroutine();
  coro->func_ = func;
  // TODO
//  coro->Run(func);
  sched->Loop();
  sched->ReturnCoroutine(coro);
}

Scheduler* Scheduler::CurrentScheduler() {
  return &coro_sched_;
}

void Scheduler::AddReadyEvent(Event *ev) {
  ready_events_.push_back(ev);
}

Coroutine* Scheduler::GetCoroutine() {
  Coroutine* c = new Coroutine;
  return c;
}

void Scheduler::ReturnCoroutine(Coroutine* coro) {
  delete coro;
}

void Scheduler::Loop(bool infinite) {

  do {
    while (ready_events_.size() > 0) {
      Event* e = ready_events_.front();
      ready_events_.pop_front();
      curr_coro_ = e->coro_;
      e->coro_->Continue();
    }
    while (new_coros_.size() > 0) {
      Coroutine* c = new_coros_.front();
      new_coros_.pop_front();
      curr_coro_ = c;
      c->Continue();
    }
  } while (infinite);
}

} // namespace rrr
