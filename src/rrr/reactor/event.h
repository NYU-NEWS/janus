
#pragma once

#include <memory>
#include "../base/all.hpp"

namespace rrr {
using std::shared_ptr;
using std::function;
using std::vector;

class Reactor;
class Coroutine;
class Event {
 public:
  enum EventStatus { INIT = 0, WAIT = 1, READY = 2, DONE = 3, DEBUG};
  EventStatus status_{INIT};
  void* _dbg_p_scheduler_{nullptr};
  uint64_t type_{0};
  function<bool(int)> test_{};

  // An event is usually allocated on a coroutine stack, thus it cannot own a
  //   shared_ptr to the coroutine it is.
  // In this case there is no shared pointer to the event.
  // When the stack that contains the event frees, the event frees.
  std::weak_ptr<Coroutine> wp_coro_{};

  virtual void Wait();
  virtual bool Test();
  virtual bool IsReady() {return false;}

  friend Reactor;
 protected:
  Event();
};

class IntEvent : public Event {

 public:
  IntEvent() {}
  int value_{0};
  int target_{1};


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

  bool IsReady() override {
    if (test_) {
      return test_(value_);
    } else {
      return (value_ == target_);
    }
  }
};

class SharedIntEvent {
 public:
  int value_{};
  vector<shared_ptr<IntEvent>> events_;
  int Set(int& v) {
    auto ret = value_;
    value_ = v;
    for (auto sp_ev : events_) {
      if (sp_ev->status_ <= Event::WAIT)
      sp_ev->Set(v);
    }
    return ret;
  }

  void Wait(function<bool(int)> f);
};


}
