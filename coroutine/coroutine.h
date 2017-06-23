#pragma once

#define BOOST_COROUTINE_NO_DEPRECATION_WARNING 1
#define BOOST_COROUTINES_NO_DEPRECATION_WARNING 1
#include <boost/coroutine/all.hpp>
#include <boost/optional.hpp>

namespace rrr {

typedef boost::coroutines::coroutine<void>::pull_type boost_coro_task_t;
typedef boost::coroutines::coroutine<void>::push_type boost_coro_yield_t;
typedef boost::coroutines::coroutine<void()> coro_t;

class CoroScheduler;
class Coroutine {
 public:
  static std::shared_ptr<Coroutine> CurrentCoroutine();
  static void CreateRun(const std::function<void()> &func);

  std::weak_ptr<CoroScheduler> sched_{};
  bool finished_{false}; //
  std::function<void()> func_{};

//  boost_coro_t* boost_coro_{nullptr};
  std::unique_ptr<boost_coro_task_t> up_boost_coro_task_{};
  boost::optional<boost_coro_yield_t&> boost_coro_yield_{};

  Coroutine() = delete;
  Coroutine(const std::function<void()>& func);
  ~Coroutine();
  void BoostRunWrapper(boost_coro_yield_t& yield);
  void Run();
  void Yield();
  void Continue();
  bool Finished();
};
}
