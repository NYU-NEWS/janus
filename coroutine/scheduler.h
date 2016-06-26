#pragma once
#include <boost/coroutine/all.hpp>

namespace rrr {

typedef boost::coroutines::coroutine<void()> coro_t;

class Event;
class Coroutine;
class Scheduler {
 public:
  static Scheduler* CurrentScheduler();
  std::list<Event*> ready_events_{};
  std::list<Coroutine*> new_coros_{};

  void AddReadyEvent(Event *ev);
  Coroutine* GetCoroutine(const std::function<void()>& func);
  void ReturnCoroutine(Coroutine* coro);
  void Loop(bool infinite = false);
};

} // namespace rrr
