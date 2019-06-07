/**
 * DragonBall is an interesting abstraction of event driven
 * programming. Typically after you collect all dragon balls you can
 * call for the holy dragon and he will make your wish become true.
 *
 * DragonBall is not thread-safe for now.
 */

#pragma once


#include <mutex>
#include <functional>

namespace rrr {

class DragonBall {

 public:

  int64_t n_wait_ = -1;
  int64_t n_ready_ = 0;
  bool called_ = false;
  std::function<void(void)> wish_{}; // this is your wish!

  bool th_safe_ = false;
  bool auto_trigger = true;

//    std::mutex mtx_;

  DragonBall(bool th_safe = false)
      : th_safe_(th_safe), n_wait_() { }

  DragonBall(const int32_t n_wait,
             const std::function<void(void)> &wish,
             bool th_safe = false)
      : n_wait_(n_wait),
        wish_(wish),
        th_safe_(th_safe) { };

  DragonBall(const DragonBall&) = delete;
  DragonBall& operator=(const DragonBall&) = delete;

  void set_wait(int64_t n_wait) {
    //std::lock_guard<std::mutex> guard(mtx_);
    n_wait_ = n_wait;
  }

  //oid collect(int64_t n) {
  //    std::lock_guard<std::mutex> guard(mtx_);
  //    n_ready_ += n;
  //    if (auto_trigger) {
  //        trigger();
  //    }
  //}
  //


  /**
   * delete myself after triggered.
   */
  bool trigger() {
    //mtx_.lock();
    n_ready_++;
    verify(n_ready_ <= n_wait_);
    bool ready = (n_ready_ == n_wait_);
    //mtx_.unlock();

    if (ready) {
      verify(!called_);
      called_ = true;
      wish_();
      delete this;
    }
    return ready;
  }

  //only allows deconstructor called by itself.
 private:
  ~DragonBall() { }
};

typedef DragonBall ConcurrentDragonBall;

} // namespace deptran
