#pragma once
#include <set>
#include <unordered_map>
#include <list>
#include "base/misc.hpp"
#include "event.h"
#include "coroutine.h"
#include "epoll_wrapper.h"

namespace rrr {

class Coroutine;
class Reactor {
 public:
  static std::shared_ptr<Reactor> GetReactor();
  std::list<std::unique_ptr<Event>> events_{};
  std::set<std::shared_ptr<Coroutine>> yielded_coros_{};
  std::set<Coroutine*> __debug_set_all_coro_{};
  std::unordered_map<uint64_t, std::function<void(Event&)>> processors_{};

  /**
   * @param ev. is usually allocated on coroutine stack. memory managed by user.
   */
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
    auto& events = GetReactor()->events_;
    auto up_ev = std::make_unique<Ev>();
    Ev& ret = *up_ev;
    events.push_back(std::move(up_ev));
    return ret;
  }
};

class PollMgr: public rrr::RefCounted {
 public:
    class PollThread;

    PollThread* poll_threads_;
    const int n_threads_;

protected:

    // RefCounted object uses protected dtor to prevent accidental deletion
    ~PollMgr();

public:

    PollMgr(int n_threads = 1);
    PollMgr(const PollMgr&) = delete;
    PollMgr& operator=(const PollMgr&) = delete;

    void add(Pollable*);
    void remove(Pollable*);
    void update_mode(Pollable*, int new_mode);
    
    // Frequent Job
    void add(std::shared_ptr<Job> sp_job);
    void remove(std::shared_ptr<Job> sp_job);
};

} // namespace rrr
