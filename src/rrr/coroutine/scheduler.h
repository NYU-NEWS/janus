#pragma once
#include <set>
#include <unordered_map>
#include <list>
#include "coroutine.h"
#include "event.h"

namespace rrr {

class Coroutine;
class AppEngine {
 public:
  static std::shared_ptr<AppEngine> CurrentScheduler();
//  std::list<boost::optional<Event&>> ready_events_{};
  std::list<std::unique_ptr<Event>> events_{};
  std::set<std::shared_ptr<Coroutine>> yielded_coros_{};
  std::set<Coroutine*> __debug_set_all_coro_{};
  std::unordered_map<uint64_t, std::function<void(Event&)>> processors_{};
//  std::set<std::shared_ptr<Coroutine>> idle_coros_{};

  /**
   * @param ev. is usually allocated on coroutine stack. memory managed by user.
   */
//  void AddReadyEvent(Event& ev);
  std::shared_ptr<Coroutine> CreateRunCoroutine(const std::function<void()> &func);
  void Loop(bool infinite = false);
  void RunCoro(std::shared_ptr<Coroutine> sp_coro);

  void AddProcessor(uint64_t type, std::function<void(Event&)>) {

  };
  // TODO for now we depend on the rpc services, fix in the future.
  void AddRpcService() {

  };
  void Run() {
    Loop(true);
  };
  template <class Ev>
  static Ev& CreateEvent() {
    auto& events = CurrentScheduler()->events_;
    auto up_ev = std::make_unique<Ev>();
    Ev& ret = *up_ev;
    events.push_back(std::move(up_ev));
    return ret;
  }
};

} // namespace rrr
