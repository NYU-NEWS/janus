
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

thread_local std::shared_ptr<Reactor> sp_reactor_th_{};
thread_local std::shared_ptr<Coroutine> sp_running_coro_th_{};

std::shared_ptr<Coroutine> Coroutine::CurrentCoroutine() {
  // TODO re-enable this verify
//  verify(sp_running_coro_th_);
  return sp_running_coro_th_;
}

std::shared_ptr<Coroutine>
Coroutine::CreateRun(std::function<void()> func) {
  auto& reactor = *Reactor::GetReactor();
  auto coro = reactor.CreateRunCoroutine(func);
  // some events might be triggered in the last coroutine.
  verify(reactor.coros_.size() > 0);
  reactor.Loop();
  return coro;
}

std::shared_ptr<Reactor>
Reactor::GetReactor() {
  if (!sp_reactor_th_) {
    Log_debug("create a coroutine scheduler");
    sp_reactor_th_ = std::make_shared<Reactor>();
  }
  return sp_reactor_th_;
}

/**
 * @param func
 * @return
 */
std::shared_ptr<Coroutine>
Reactor::CreateRunCoroutine(const std::function<void()> func) {
  std::shared_ptr<Coroutine> sp_coro = std::make_shared<Coroutine>(func);
//  __debug_set_all_coro_.insert(sp_coro.get());
//  verify(!curr_coro_); // Create a coroutine from another?
//  verify(!sp_running_coro_th_); // disallows nested coroutines
  auto sp_old_coro = sp_running_coro_th_;
  sp_running_coro_th_ = sp_coro;
  verify(sp_coro);
  auto pair = coros_.insert(sp_coro);
  verify(pair.second);
  verify(coros_.size() > 0);
  sp_coro->Run();
  if (sp_coro->Finished()) {
    coros_.erase(sp_coro);
  }
  // yielded or finished, reset to old coro.
  sp_running_coro_th_ = sp_old_coro;
  return sp_coro;
}

//  be careful this could be called from different coroutines.
void Reactor::Loop(bool infinite) {
  looping_ = infinite;
  do {
    std::vector<shared_ptr<Event>> ready_events;
    for (auto it = events_.begin(); it != events_.end();) {
      Event& event = **it;
      event.Test();
      if (event.status_ == Event::READY) {
        ready_events.push_back(std::move(*it));
        it = events_.erase(it);
      } else if (event.status_ == Event::DONE) {
        it = events_.erase(it);
      } else {
        it ++;
      }
    }
    for (auto& up_ev: ready_events) {
      auto& event = *up_ev;
      auto sp_coro = event.wp_coro_.lock();
      verify(sp_coro);
      verify(coros_.find(sp_coro) != coros_.end());
      ContinueCoro(sp_coro);
    }
  } while (looping_);
}

void Reactor::ContinueCoro(std::shared_ptr<Coroutine> sp_coro) {
//  verify(!sp_running_coro_th_); // disallow nested coros
  auto sp_old_coro = sp_running_coro_th_;
  sp_running_coro_th_ = sp_coro;
  verify(!sp_running_coro_th_->Finished());
  sp_running_coro_th_->Continue();
  if (sp_running_coro_th_->Finished()) {
    coros_.erase(sp_running_coro_th_);
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
  std::unordered_map<int, int> mode_; // fd->mode
  std::unordered_set<Pollable*> poll_set_;

  std::set<std::shared_ptr<Job>> set_sp_jobs_;

  std::unordered_set<Pollable*> pending_remove_;
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
        Coroutine::CreateRun([sp_job]() {sp_job->Work();});
      }
      if (sp_job->Done()) {
        it = set_sp_jobs_.erase(it);
      } else {
        it++;
      }
    }
    lock_job_.unlock();
  }

 public:

  PollThread() : stop_flag_(false) {
  }

  ~PollThread() {
    stop_flag_ = true;
    Pthread_join(th_, nullptr);

    // when stopping, release anything registered in pollmgr
    for (auto& it: poll_set_) {
      this->remove(it);
    }
    for (auto& it: pending_remove_) {
      it->release();
    }
  }

  void add(Pollable*);
  void remove(Pollable*);
  void update_mode(Pollable*, int new_mode);

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
  //Log_debug("rrr::PollMgr: destroyed");
}

void PollMgr::PollThread::poll_loop() {
  while (!stop_flag_) {
    TriggerJob();
    poll_.Wait();
    TriggerJob();
    // after each poll loop, remove uninterested pollables
    pending_remove_l_.lock();
    std::list<Pollable*> remove_poll(pending_remove_.begin(), pending_remove_.end());
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

      poll->release();
    }
    TriggerJob();
    Reactor::GetReactor()->Loop();
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

void PollMgr::PollThread::add(Pollable* poll) {
  poll->ref_copy();   // increase ref count

  int poll_mode = poll->poll_mode();
  int fd = poll->fd();

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

void PollMgr::PollThread::remove(Pollable* poll) {
  bool found = false;
  l_.lock();
  std::unordered_set<Pollable*>::iterator it = poll_set_.find(poll);
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

void PollMgr::PollThread::update_mode(Pollable* poll, int new_mode) {
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

void PollMgr::add(Pollable* poll) {
  int fd = poll->fd();
  if (fd >= 0) {
    int tid = hash_fd(fd) % n_threads_;
    poll_threads_[tid].add(poll);
  }
}

void PollMgr::remove(Pollable* poll) {
  int fd = poll->fd();
  if (fd >= 0) {
    int tid = hash_fd(fd) % n_threads_;
    poll_threads_[tid].remove(poll);
  }
}

void PollMgr::update_mode(Pollable* poll, int new_mode) {
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
