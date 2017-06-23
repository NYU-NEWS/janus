#pragma once
#include <set>
#include "coroutine.h"

namespace rrr {

class Event;
class Coroutine;
class CoroScheduler {
 public:
  static std::shared_ptr<CoroScheduler> CurrentScheduler();
  std::list<boost::optional<Event&>> ready_events_{};
  std::set<std::shared_ptr<Coroutine>> active_coros_{};
//  std::set<std::shared_ptr<Coroutine>> idle_coros_{};

  /**
   *
   * @param ev. is usually allocated on coroutine stack. memory managed by user.
   */
  void AddReadyEvent(Event& ev);
  void CreateRunCoroutine(const std::function<void()> &func);
  void Loop(bool infinite = false);
  void RunCoro(std::shared_ptr<Coroutine> sp_coro);
};

} // namespace rrr
