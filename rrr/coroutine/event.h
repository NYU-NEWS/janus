
#pragma once

#include "../base/all.hpp"

namespace rrr {

class CoroScheduler;
class Coroutine;
class Event {
 public:
  enum EventStatus { INIT = 0, WAIT = 1, READY = 2, TRIGGERED = 3, DEBUG = 4 };
  EventStatus status_{INIT};
  void* _dbg_p_scheduler_{nullptr};

  // An event is usually allocated on a coroutine stack, thus it cannot own a
  //   shared_ptr to the coroutine it is.
  // In this case there is no shared pointer to the event.
  // When the stack that contains the event frees, the event frees.
  std::weak_ptr<Coroutine> wp_coro_{};

  Event(std::shared_ptr<Coroutine> coro = {}) {
    if (!coro) {
      coro = Coroutine::CurrentCoroutine();
    }
    verify(coro);
    wp_coro_ = coro;
  };

  virtual void Wait();
  virtual bool Test();
  virtual bool IsReady() { return false; }
};

class IntEvent : public Event {
 public:
  int value_{0};
  int target_{1};

  IntEvent() = default;

  IntEvent(int n, int t) : Event(), value_(n), target_(t) {

  }

  bool TestTrigger();

  int get() {
    return value_;
  }

  int Set(int n) {
    int t = value_;
    value_ = n;
    TestTrigger();
    return t;
  };

  int increment(int i = 1) {
    int t = value_;
    value_ += i;
    TestTrigger();
    return t;
  };

  int decrement(int i = 1) {
    int t = value_;
    value_ -= i;
    TestTrigger();
    return t;

  };

  bool IsReady() override {
    return (value_ == target_);
  }
};

}
