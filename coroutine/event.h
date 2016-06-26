
#pragma once

#include "../base/all.hpp"

namespace rrr {

class Scheduler;
class Coroutine;
class Event {
 public:
  enum EventStatus {INIT=0, WAIT=1, READY=2, TRIGGERED=3};
  EventStatus status_{INIT};
  Scheduler* sched_{nullptr};
  Coroutine* coro_{nullptr};
  Event(Coroutine* coro=nullptr) {
    if (coro == nullptr) {
      // TODO
//      coro_ = Coroutine::GetCurrentCoroutine();
    } else {
      coro_ = coro;
    }
    // TODO
//    sched_ = coro_->sched_;
  };
  virtual void Wait();
  virtual bool IsReady() {return false;}
};

class IntEvent : public Event {
 public:
  int value_{0};
  int target_{0};

//  IntEvent() = delete;
  IntEvent(int n, int t) : value_(n), target_(t) {

  }

  bool TestTrigger();

  int get() {
    return value_;
  }

  int set(int n) {
    int t = value_;
    value_ = n;
    TestTrigger();
    return t;
  };

  int increment(int i=1) {
    int t = value_;
    value_ += i;
    TestTrigger();
    return t;
  };

  int decrement(int i=1) {
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
