
#include <unistd.h>
#include <string.h>
#include <errno.h>
#include <unordered_map>
#include <unordered_set>
#include <functional>
#include "../base/all.hpp"
#include "reactor.h"
#include "coroutine.h"
#include "event.h"
#include "epoll_wrapper.h"

namespace rrr {

thread_local std::shared_ptr<Reactor> Reactor::sp_reactor_th_{};
thread_local std::shared_ptr<Coroutine> Reactor::sp_running_coro_th_{};

std::shared_ptr<Coroutine> Coroutine::CurrentCoroutine() {
  // TODO re-enable this verify
  verify(Reactor::sp_running_coro_th_);
  return Reactor::sp_running_coro_th_;
}

std::shared_ptr<Coroutine>
Coroutine::CreateRun(std::function<void()> func) {
  auto& reactor = *Reactor::GetReactor();
  auto coro = reactor.CreateRunCoroutine(func);
  // some events might be triggered in the last coroutine.
  return coro;
}

std::shared_ptr<Reactor>
Reactor::GetReactor() {
  if (!sp_reactor_th_) {
    Log_debug("create a coroutine scheduler");
    sp_reactor_th_ = std::make_shared<Reactor>();
    sp_reactor_th_->thread_id_ = std::this_thread::get_id();
  }
  return sp_reactor_th_;
}

/**
 * @param func
 * @return
 */
std::shared_ptr<Coroutine>
Reactor::CreateRunCoroutine(const std::function<void()> func) {
  std::shared_ptr<Coroutine> sp_coro;
  const bool reusing = REUSING_CORO && !available_coros_.empty();
  if (reusing) {
    n_idle_coroutines_--;
    sp_coro = available_coros_.back();
    available_coros_.pop_back();
    verify(!sp_coro->func_);
    sp_coro->func_ = func;
  } else {
    sp_coro = std::make_shared<Coroutine>(func);
    verify(sp_coro->status_ == Coroutine::INIT);
    n_created_coroutines_++;
    if (n_created_coroutines_ % 100 == 0) {
      Log_debug("created %d, busy %d, idle %d coroutines on this thread",
               (int)n_created_coroutines_,
               (int)n_busy_coroutines_,
               (int)n_idle_coroutines_);
    }

  }
  n_busy_coroutines_++;
  coros_.insert(sp_coro);
  ContinueCoro(sp_coro);
//  Loop();
  return sp_coro;
//  __debug_set_all_coro_.insert(sp_coro.get());
//  verify(!curr_coro_); // Create a coroutine from another?
//  verify(!sp_running_coro_th_); // disallows nested coroutines
//  auto sp_old_coro = sp_running_coro_th_;
//  sp_running_coro_th_ = sp_coro;
//  verify(sp_coro);
//  auto pair = coros_.insert(sp_coro);
//  verify(pair.second);
//  verify(coros_.size() > 0);
//  sp_coro->Run();
//  if (sp_coro->Finished()) {
//    coros_.erase(sp_coro);
//  }
//  Loop();
//  // yielded or finished, reset to old coro.
//  sp_running_coro_th_ = sp_old_coro;
//  return sp_coro;
}

void Reactor::CheckTimeout(std::vector<std::shared_ptr<Event>>& ready_events ) {
  auto time_now = Time::now();
  for (auto it = timeout_events_.begin(); it != timeout_events_.end();) {
    Event& event = **it;
    auto status = event.status_;
    switch (status) {
      case Event::INIT:
        verify(0);
      case Event::WAIT: {
        const auto &wakeup_time = event.wakeup_time_;
        verify(wakeup_time > 0);
        if (time_now > wakeup_time) {
          if (event.IsReady()) {
            // This is because our event mechanism is not perfect, some events
            // don't get triggered with arbitrary condition change.
            event.status_ = Event::READY;
          } else {
            event.status_ = Event::TIMEOUT;
          }
          ready_events.push_back(*it);
          it = timeout_events_.erase(it);
        } else {
          it++;
        }
        break;
      }
      case Event::READY:
      case Event::DONE:
        it = timeout_events_.erase(it);
        break;
      default:
        verify(0);
    }
  }

}

//  be careful this could be called from different coroutines.
void Reactor::Loop(bool infinite, bool check_timeout) {
  verify(std::this_thread::get_id() == thread_id_);
  looping_ = infinite;
  do {
    std::vector<shared_ptr<Event>> ready_events = std::move(ready_events_);
    verify(ready_events_.empty());
#ifdef DEBUG_CHECK
    for (auto ev : ready_events) {
      verify(ev->status_ == Event::READY);
    }
#endif
    if (check_timeout) {
      CheckTimeout(ready_events);
    }

    for (auto it = ready_events.begin(); it != ready_events.end(); it++) {
      Event& event = **it;
      verify(event.status_ != Event::DONE);
      auto sp_coro = event.wp_coro_.lock();
      verify(sp_coro);
      verify(sp_coro->status_ == Coroutine::PAUSED);
      verify(coros_.find(sp_coro) != coros_.end()); // TODO ?????????
      if (event.status_ == Event::READY) {
        event.status_ = Event::DONE;
      } else {
        verify(event.status_ == Event::TIMEOUT);
      }
      ContinueCoro(sp_coro);
    }

    // FOR debug purposes.
//    auto& events = waiting_events_;
////    Log_debug("event list size: %d", events.size());
//    for (auto it = events.begin(); it != events.end(); it++) {
//      Event& event = **it;
//      const auto& status = event.status_;
//      if (event.status_ == Event::WAIT) {
//        event.Test();
//        verify(event.status_ != Event::READY);
//      }
//    }
  } while (looping_ || !ready_events_.empty());
  verify(ready_events_.empty());
}

void Reactor::Recycle(std::shared_ptr<Coroutine>& sp_coro) {

  // This fixes the bug that coroutines are not recycling if they don't finish immediately.
  if (REUSING_CORO) {
    sp_coro->status_ = Coroutine::RECYCLED;
    sp_coro->func_ = {};
    n_idle_coroutines_++;
    available_coros_.push_back(sp_coro);
  }
  n_busy_coroutines_--;
  coros_.erase(sp_coro);
}

void Reactor::ContinueCoro(std::shared_ptr<Coroutine> sp_coro) {
//  verify(!sp_running_coro_th_); // disallow nested coros
  verify(sp_running_coro_th_ != sp_coro);
  auto sp_old_coro = sp_running_coro_th_;
  sp_running_coro_th_ = sp_coro;
  verify(!sp_running_coro_th_->Finished());
  n_active_coroutines_++;
  if (sp_coro->status_ == Coroutine::INIT) {
    sp_coro->Run();
  } else {
    // PAUSED or RECYCLED
    sp_coro->Continue();
  }
  verify(sp_running_coro_th_ == sp_coro);
  if (sp_running_coro_th_ -> Finished()) {
    Recycle(sp_coro);
  }
  sp_running_coro_th_ = sp_old_coro;
}

// TODO PollThread -> Reactor
// TODO PollMgr -> ReactorFactory
class PollMgr::PollThread {

  friend class PollMgr;

  Epoll poll_{};

  // guard mode_ and poll_set_
  SpinLock l_;
  std::unordered_map<int, int> mode_{}; // fd->mode
  std::set<shared_ptr<Pollable>> poll_set_{};
  std::set<std::shared_ptr<Job>> set_sp_jobs_{};
  std::unordered_set<shared_ptr<Pollable>> pending_remove_{};
  SpinLock pending_remove_l_;
  SpinLock lock_job_;

  pthread_t th_;
  bool stop_flag_;

  static void* start_poll_loop(void* arg) {
    PollThread* thiz = (PollThread*) arg;
    thiz->poll_loop();
    pthread_exit(nullptr);
    return nullptr;
  }

  void poll_loop();

  void start(PollMgr* poll_mgr) {
    Pthread_create(&th_, nullptr, PollMgr::PollThread::start_poll_loop, this);
  }

  void TriggerJob() {
    lock_job_.lock();
    auto it = set_sp_jobs_.begin();
    while (it != set_sp_jobs_.end()) {
      auto sp_job = *it;
      if (sp_job->Ready()) {
        Coroutine::CreateRun([sp_job]() {
          sp_job->Work();
        });
      }
      it = set_sp_jobs_.erase(it);
//      if (sp_job->Done()) {
//        it = set_sp_jobs_.erase(it);
//      } else {
//        it++;
//      }
    }
    lock_job_.unlock();
  }

 public:

  PollThread() : stop_flag_(false) {
  }

  ~PollThread() {
    stop_flag_ = true;
    Pthread_join(th_, nullptr);

    l_.lock();
    vector<shared_ptr<Pollable>> tmp(poll_set_.begin(), poll_set_.end());
    l_.unlock();
    // when stopping, release anything registered in pollmgr
    for (auto it: tmp) {
      verify(it);
      this->remove(it);
    }
  }

  void add(shared_ptr<Pollable>);
  void remove(shared_ptr<Pollable>);
  void update_mode(shared_ptr<Pollable>, int new_mode);

  void add(std::shared_ptr<Job>);
  void remove(std::shared_ptr<Job>);
};

PollMgr::PollMgr(int n_threads /* =... */)
    : n_threads_(n_threads), poll_threads_() {
  verify(n_threads_ > 0);
  poll_threads_ = new PollThread[n_threads_];
  for (int i = 0; i < n_threads_; i++) {
    poll_threads_[i].start(this);
  }
}

PollMgr::~PollMgr() {
  delete[] poll_threads_;
  poll_threads_ = nullptr;
  //Log_debug("rrr::PollMgr: destroyed");
}

void PollMgr::PollThread::poll_loop() {
  while (!stop_flag_) {
    TriggerJob();
    Reactor::GetReactor()->Loop(false, true);
    poll_.Wait();
    verify(Reactor::GetReactor()->ready_events_.empty());
    TriggerJob();
    // after each poll loop, remove uninterested pollables
    pending_remove_l_.lock();
    std::list<shared_ptr<Pollable>> remove_poll(pending_remove_.begin(), pending_remove_.end());
    pending_remove_.clear();
    pending_remove_l_.unlock();

    for (auto& poll: remove_poll) {
      int fd = poll->fd();

      l_.lock();
      if (mode_.find(fd) == mode_.end()) {
        // NOTE: only remove the fd when it is not immediately added again
        // if the same fd is used again, mode_ will contains its info
        poll_.Remove(poll);
      }
      l_.unlock();
    }
    TriggerJob();
    verify(Reactor::GetReactor()->ready_events_.empty());
    Reactor::GetReactor()->Loop();
    verify(Reactor::GetReactor()->ready_events_.empty());
  }
}

void PollMgr::PollThread::add(std::shared_ptr<Job> sp_job) {
  lock_job_.lock();
  set_sp_jobs_.insert(sp_job);
  lock_job_.unlock();
}

void PollMgr::PollThread::remove(std::shared_ptr<Job> sp_job) {
  lock_job_.lock();
  set_sp_jobs_.erase(sp_job);
  lock_job_.unlock();
}

void PollMgr::PollThread::add(shared_ptr<Pollable> poll) {
  int poll_mode = poll->poll_mode();
  int fd = poll->fd();
  verify(poll);

  l_.lock();

  // verify not exists
  verify(poll_set_.find(poll) == poll_set_.end());
  verify(mode_.find(fd) == mode_.end());

  // register pollable
  poll_set_.insert(poll);
  mode_[fd] = poll_mode;
  poll_.Add(poll);

  l_.unlock();
}

void PollMgr::PollThread::remove(shared_ptr<Pollable> poll) {
  bool found = false;
  l_.lock();
  auto it = poll_set_.find(poll);
  if (it != poll_set_.end()) {
    found = true;
    assert(mode_.find(poll->fd()) != mode_.end());
    poll_set_.erase(poll);
    mode_.erase(poll->fd());
  } else {
    assert(mode_.find(poll->fd()) == mode_.end());
  }
  l_.unlock();

  if (found) {
    pending_remove_l_.lock();
    pending_remove_.insert(poll);
    pending_remove_l_.unlock();
  }
}

void PollMgr::PollThread::update_mode(shared_ptr<Pollable> poll, int new_mode) {
  int fd = poll->fd();

  l_.lock();

  if (poll_set_.find(poll) == poll_set_.end()) {
    l_.unlock();
    return;
  }

  auto it = mode_.find(fd);
  verify(it != mode_.end());
  int old_mode = it->second;
  it->second = new_mode;

  if (new_mode != old_mode) {
    poll_.Update(poll, new_mode, old_mode);
  }

  l_.unlock();
}

static inline uint32_t hash_fd(uint32_t key) {
  uint32_t c2 = 0x27d4eb2d; // a prime or an odd constant
  key = (key ^ 61) ^ (key >> 16);
  key = key + (key << 3);
  key = key ^ (key >> 4);
  key = key * c2;
  key = key ^ (key >> 15);
  return key;
}

void PollMgr::add(shared_ptr<Pollable> poll) {
  int fd = poll->fd();
  if (fd >= 0) {
    int tid = hash_fd(fd) % n_threads_;
    poll_threads_[tid].add(poll);
  }
}

void PollMgr::remove(shared_ptr<Pollable> poll) {
  int fd = poll->fd();
  if (fd >= 0) {
    int tid = hash_fd(fd) % n_threads_;
    poll_threads_[tid].remove(poll);
  }
}

void PollMgr::update_mode(shared_ptr<Pollable> poll, int new_mode) {
  int fd = poll->fd();
  if (fd >= 0) {
    int tid = hash_fd(fd) % n_threads_;
    poll_threads_[tid].update_mode(poll, new_mode);
  }
}

void PollMgr::add(std::shared_ptr<Job> fjob) {
  int tid = 0;
  poll_threads_[tid].add(fjob);
}

void PollMgr::remove(std::shared_ptr<Job> fjob) {
  int tid = 0;
  poll_threads_[tid].remove(fjob);
}

} // namespace rrr
