#pragma once

#include <boost/coroutine/all.hpp>

namespace rrr {

typedef boost::coroutines::coroutine<void>::pull_type boost_coro_task_t;
typedef boost::coroutines::coroutine<void>::push_type boost_coro_yield_t;

class Scheduler;
class Coroutine {
 public:
  static Coroutine* CurrentCoroutine();
  static void Create(const std::function<void()>& func);

  Scheduler* sched_{nullptr};
  std::function<void()> func_{};
//  boost_coro_t* boost_coro_{nullptr};
  boost_coro_task_t* boost_coro_task_{nullptr};
  boost_coro_yield_t* boost_coro_yield_{nullptr};

  Coroutine() = delete;
  Coroutine(const std::function<void()>& func);
  ~Coroutine();
  void BoostRunWrapper(boost_coro_yield_t& yield);
  void Run(const std::function<void()> &func, bool defer = false);
  void Yield();
  void Continue();
};
}
