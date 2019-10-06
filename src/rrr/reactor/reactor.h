#pragma once
#include <set>
#include <unordered_map>
#include <list>
#include "base/misc.hpp"
#include "event.h"
#include "quorum_event.h"
#include "coroutine.h"
#include "epoll_wrapper.h"

namespace rrr {

using std::make_unique;
using std::make_shared;

class Coroutine;
// TODO for now we depend on the rpc services, fix in the future.
class Reactor {
 public:
  static std::shared_ptr<Reactor> GetReactor();
  /**
   * A reactor needs to keep reference to all coroutines created,
   * in case it is freed by the caller after a yield.
   * TODO the lifetime of an event should be independent from a
   * coroutine? Or should an event belong to a coroutine?
   */
  std::list<std::shared_ptr<Event>> events_{};
  std::set<std::shared_ptr<Coroutine>> coros_{};
//  std::set<Coroutine*> __debug_set_all_coro_{};
  std::unordered_map<uint64_t, std::function<void(Event&)>> processors_{};
  bool looping_{false};

  /**
   * @param ev. is usually allocated on coroutine stack. memory managed by user.
   */
  std::shared_ptr<Coroutine> CreateRunCoroutine(std::function<void()> func);
  void Loop(bool infinite = false);
  void ContinueCoro(std::shared_ptr<Coroutine> sp_coro);

  ~Reactor() {
//    verify(0);
  }

  template <typename Ev, typename... Args>
  static shared_ptr<Ev> CreateSpEvent(Args&&... args) {
    auto& events = GetReactor()->events_;
//    auto sp_ev = make_shared<Ev>(args...);
    shared_ptr<Ev> sp_ev;
    sp_ev.reset(new Ev(args...));
    sp_ev->__debug_creator = 1;
    // TODO push them into a wait queue when they actually wait.
    events.push_back(sp_ev);
    return sp_ev;
  }

  template <typename Ev, typename... Args>
  static Ev& CreateEvent(Args&&... args) {
    return *CreateSpEvent<Ev>(args...);
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
