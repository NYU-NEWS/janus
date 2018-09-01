/**
 * This is an alarmer.
 */


#pragma once

#include <list>
#include <mutex>
#include <thread>
#include <functional>
#include <map>

#include "base/misc.hpp"
#include "reactor/reactor.h"

namespace rrr {

class Alarm: public FrequentJob {
 public:
  bool run_ = true;
  std::thread th_;
  //    std::mutex lock_;

  uint64_t next_id_ = 1;

  // either a thread_loop_holder or a epoll holder.
  rrr::PollMgr *holder = NULL;


  // id -> <alarm_time, func>;
  std::map<uint64_t,
           std::pair<uint64_t, std::function<void(void)>>> waiting_;

  // <time, id> -> func
  std::map<std::pair<uint64_t, uint64_t>,
           std::function<void(void)> > idx_time_;

  Alarm() : th_(), waiting_(), idx_time_()
//	th_([this] () {
//		this->alarm_loop();
//	    })
  {
    period_ = 50 * 1000; // 50ms;
  }

  Alarm(const Alarm&) = delete;
  Alarm& operator=(const Alarm&) = delete;

  ~Alarm() {
  }

  void set_holder(rrr::PollMgr *mgr) {
  }

  bool exe_next() {
    //	std::lock_guard<std::mutex> guard(lock_);
    bool ret = false;
    auto it = waiting_.begin();
    if (it != waiting_.end()) {
      uint64_t tm_now = rrr::Time::now();
      uint64_t tm_out = it->second.first;
      ret = (tm_now > tm_out);
      if (ret) {
        auto &func = it->second.second;
        func();
        waiting_.erase(it);
      }
    }
    return ret;
  }

  bool Done() override {
    verify(0);
    return true;
  }

  void Work() override {
    while (exe_next()) { }
  }

//    void alarm_loop() {
//	while (run_) {
//	    run();
//	    apr_sleep(10 * 1000); // triggered every 10 ms
//	}
//    }

  uint64_t add(uint64_t time, std::function<void(void)> func) {
    //	std::lock_guard<std::mutex> guard(lock_);
    //Log::debug("add timeout callback");
    auto id = next_id_++;
    waiting_[id] = std::make_pair(time, func);
    //	idx_time_[std::make_pair(time, id)] = func;
    return id;
  }

  /**
   * NEW: calling this will ensure that the timeout alarm
   * will not be executed. But care for DEADLOCKS !!!
   * If returns true, the callback is successfully removed
   * and will never be invoked. If not, it is not sure that
   * whether this callback will be invoked or not.
   */
  bool remove(uint64_t id) {
    //	std::lock_guard<std::mutex> guard(lock_);
    int n_erased = waiting_.erase(id);
    return (n_erased == 1);
  }

};

} // namespace rrr
