/**
 * This is a asynchronous queued lock module with timeout support.
 * Written to support two-phase locking in rococo.
 *
 * This is NOT a thread lock!!!
 * Although it is thread-safe lock, you need to make sure your codes
 * are thread-safe too, because the the callback maybe called in another
 * thread.
 */

#pragma once

#include <list>
#include <mutex>
#include <thread>
#include <functional>
#include <cstdint>

#include "base/all.hpp"

#include "alarm.hpp"
#include "dball.hpp"

#define ALOCK_TIMEOUT (200000) // 0.2s;
//#define ALOCK_TIMEOUT (0) // no_timeout;


namespace rrr {

class ALock {
 public:
  enum type_t { RLOCK, WLOCK };
 private:
  uint64_t next_id_ = 1;

 protected:
  enum status_t { FREE, WLOCKED, RLOCKED };
  status_t status_ = FREE;
  // how many are holding read locks;
  int64_t n_rlock_ = 0; // -1 WLOCKED, 0 FREE, >0 RLOCKED numbers;
  uint64_t get_next_id() {
    return next_id_++;
  }

  virtual uint64_t vlock(const std::function<void(uint64_t)> &yes_callback,
                         const std::function<void(void)> &no_callback,
                         type_t type,
                         int64_t priority, // lower value has higher priority
                         const std::function<int(void)> &wound_callback) = 0;

  bool done_;

 public:
  ALock() :
      status_(FREE),
      n_rlock_(0),
      done_(false) {
  }

  virtual uint64_t lock(const std::function<void(uint64_t)> &yes_callback,
                        const std::function<void(void)> &no_callback,
                        type_t type = WLOCK,
                        int64_t priority = 0, // lower value has higher priority
                        const std::function<int(void)> &wound_callback
                        = std::function<int(void)>()) {
    return vlock(yes_callback,
                 no_callback,
                 type,
                 priority,
                 wound_callback);
  }

  virtual uint64_t lock(const std::function<void(void)> &yes_callback,
                        const std::function<void(void)> &no_callback,
                        type_t type = WLOCK,
                        int64_t priority = 0, // lower value has higher priority
                        const std::function<int(void)> &wound_callback
                        = std::function<int(void)>()) {
    std::function<void(uint64_t)> _yes_callback
        = [yes_callback](uint64_t id) {
          yes_callback();
        };
    return vlock(_yes_callback,
                 no_callback,
                 type,
                 priority,
                 wound_callback);
  }

  virtual void abort(uint64_t id) = 0;

  virtual ~ALock() { }
};

class WaitDieALock: public ALock {
 protected:
  struct lock_req_t {
    typedef enum {
      WAIT,
      LOCK
    } lock_req_status_t;
    uint64_t id;
    int64_t priority;
    type_t type;
    std::function<void(uint64_t)> yes_callback;
    std::function<void(void)> no_callback;
    lock_req_status_t status;

    lock_req_t() : id(0), priority(0), type(WLOCK), status(WAIT),
                   yes_callback(), no_callback() {
    }

    lock_req_t(uint64_t _id,
               int64_t _priority,
               type_t _type,
               const std::function<void(uint64_t)> &_yes_callback,
               const std::function<void(void)> &_no_callback,
               lock_req_status_t _status = WAIT) :
        id(_id),
        priority(_priority),
        type(_type),
        yes_callback(_yes_callback),
        no_callback(_no_callback),
        status(_status) {
    }

    lock_req_t(const lock_req_t &lock_req)
        : id(lock_req.id), priority(lock_req.priority), type(lock_req.type),
          yes_callback(lock_req.yes_callback),
          no_callback(lock_req.no_callback),
          status(lock_req.status) {
    }

    lock_req_t &operator=(const lock_req_t &lock_req) {
      id = lock_req.id;
      priority = lock_req.priority;
      type = lock_req.type;
      yes_callback = lock_req.yes_callback;
      no_callback = lock_req.no_callback;
      status = lock_req.status;
      return *this;
    }
  };

  typedef enum {
    WD_WAIT,
    WD_DIE
  } wd_status_t;
  std::list<lock_req_t> requests_;

  uint64_t n_r_in_queue_;
  uint64_t n_w_in_queue_;

  wd_status_t wait_die(type_t type, int64_t priority);

  void write_acquire(lock_req_t &lock_req) {
    verify(lock_req.type == WLOCK && lock_req.status == lock_req_t::WAIT);
    status_ = WLOCKED;
    lock_req.status = lock_req_t::LOCK;
    lock_req.yes_callback(lock_req.id);
  }
  void read_acquire(lock_req_t &lock_req) {
    verify(lock_req.type == RLOCK && lock_req.status == lock_req_t::WAIT);
    if (n_rlock_ == 0)
      status_ = RLOCKED;
    n_rlock_++;
    lock_req.status = lock_req_t::LOCK;
    lock_req.yes_callback(lock_req.id);
  }
  void read_acquire(const std::vector<lock_req_t *> &lock_reqs) {
    if (lock_reqs.size() == 0) {
      return;
    }
    if (n_rlock_ == 0)
      status_ = RLOCKED;
    n_rlock_ += lock_reqs.size();
    std::vector<std::pair<std::function<void(uint64_t)>, uint64_t> > to_calls;
    to_calls.reserve(lock_reqs.size());
    std::vector<lock_req_t *>::const_iterator it;
    for (it = lock_reqs.begin(); it != lock_reqs.end(); it++) {
      verify((*it)->type == RLOCK && (*it)->status == lock_req_t::WAIT);
      (*it)->status = lock_req_t::LOCK;
      to_calls.push_back(
          std::pair<std::function<void(uint64_t)>, uint64_t>(
              (*it)->yes_callback,
              (*it)->id));
    }
    std::vector<std::pair<std::function<void(uint64_t)>, uint64_t> >::iterator
        cit;
    for (cit = to_calls.begin(); cit != to_calls.end(); cit++) {
      cit->first(cit->second);
    }
  }

  virtual uint64_t vlock(const std::function<void(uint64_t)> &yes_callback,
                         const std::function<void(void)> &no_callback,
                         type_t type,
                         int64_t priority,
                         const std::function<int(void)> &);

  void sanity_check() {
    bool acquired_check = false;
    int64_t big = std::numeric_limits<int64_t>::max();
    int64_t tmp_big = -1;
    int64_t n_r_locked = 0;
    if (status_ == FREE)
      verify(requests_.size() == 0);
    int64_t num_w = 0, num_r = 0;
    std::list<lock_req_t>::iterator it = requests_.begin();
    for (; it != requests_.end(); it++) {
      if (!acquired_check) {
        if (status_ == WLOCKED) {
          acquired_check = true;
          verify(it == requests_.begin());
          verify(it->type == WLOCK);
          verify(it->status == lock_req_t::LOCK);
        }
        else {
          verify(status_ == RLOCKED);
          if (it->type == RLOCK) {
            verify(it->status == lock_req_t::LOCK);
            n_r_locked++;
          }
          else {
            verify(it->status == lock_req_t::WAIT);
            acquired_check = true;
            verify(n_r_locked == n_rlock_);
          }
        }
      }
      else {
        verify(it->status == lock_req_t::WAIT);
      }

      verify(big > it->priority);
      if (it->type == RLOCK) {
        num_r++;
        if (tmp_big == -1) {
          tmp_big = it->priority;
        }
        else {
          if (tmp_big > it->priority)
            tmp_big = it->priority;
        }
      }
      else {
        num_w++;
        if (tmp_big == -1) { // no R between two Ws
          big = it->priority;
        }
        else {
          big = tmp_big;
          verify(big > it->priority);
          tmp_big = -1;
        }
      }
    }
    verify(num_w == n_w_in_queue_);
    verify(num_r == n_r_in_queue_);
  }

 public:
  WaitDieALock() : ALock(), n_r_in_queue_(0), n_w_in_queue_(0),
                   requests_() {
  }

  virtual ~WaitDieALock();

  virtual void abort(uint64_t id);
};

class WoundDieALock: public ALock {
 protected:
  struct lock_req_t {
    typedef enum {
      WAIT,
      LOCK
    } lock_req_status_t;
    uint64_t id;
    int64_t priority;
    type_t type;
    std::function<void(uint64_t)> yes_callback;
    std::function<void(void)> no_callback;
    std::function<int(void)> wound_callback;
    lock_req_status_t status;

    lock_req_t() : id(0), priority(0), type(WLOCK), status(WAIT),
                   yes_callback(), no_callback(), wound_callback() {
    }

    lock_req_t(uint64_t _id,
               int64_t _priority,
               type_t _type,
               const std::function<void(uint64_t)> &_yes_callback,
               const std::function<void(void)> &_no_callback,
               const std::function<int(void)> &_wound_callback,
               lock_req_status_t _status = WAIT) :
        id(_id),
        priority(_priority),
        type(_type),
        yes_callback(_yes_callback),
        no_callback(_no_callback),
        wound_callback(_wound_callback),
        status(_status) {
    }

    lock_req_t(const lock_req_t &lock_req)
        : id(lock_req.id), priority(lock_req.priority), type(lock_req.type),
          yes_callback(lock_req.yes_callback),
          no_callback(lock_req.no_callback),
          wound_callback(lock_req.wound_callback), status(lock_req.status) {
    }

    lock_req_t &operator=(const lock_req_t &lock_req) {
      id = lock_req.id;
      priority = lock_req.priority;
      type = lock_req.type;
      yes_callback = lock_req.yes_callback;
      no_callback = lock_req.no_callback;
      wound_callback = lock_req.wound_callback;
      status = lock_req.status;
      return *this;
    }
  };

  std::list<lock_req_t> requests_;

  void wound_die(type_t type, int64_t priority);

  // return 0: wound succ
  // return 1: unwoundable
  int wound(lock_req_t &lock_req) {
    if (lock_req.status == lock_req_t::WAIT) { // waiting, use no callback
      lock_req.no_callback();
      return 0;
    }
    else { // locked, use wound callback
      int ret = lock_req.wound_callback();
      if (ret == 0) { // wound succ, reset alock status
        if (lock_req.type == WLOCK) {
          verify(status_ == WLOCKED);
          status_ = FREE;
        }
        else {
          verify(status_ == RLOCKED);
          n_rlock_--;
          if (n_rlock_ == 0)
            status_ = FREE;
        }
      }
      return ret;
    }
  }

  void write_acquire(lock_req_t &lock_req) {
    verify(lock_req.type == WLOCK && lock_req.status == lock_req_t::WAIT);
    status_ = WLOCKED;
    lock_req.status = lock_req_t::LOCK;
    lock_req.yes_callback(lock_req.id);
  }
  void read_acquire(lock_req_t &lock_req) {
    verify(lock_req.type == RLOCK && lock_req.status == lock_req_t::WAIT);
    if (n_rlock_ == 0)
      status_ = RLOCKED;
    n_rlock_++;
    lock_req.status = lock_req_t::LOCK;
    lock_req.yes_callback(lock_req.id);
  }
  void read_acquire(const std::vector<lock_req_t *> &lock_reqs) {
    if (lock_reqs.size() == 0) {
      return;
    }
    if (n_rlock_ == 0)
      status_ = RLOCKED;
    n_rlock_ += lock_reqs.size();
    std::vector<std::pair<std::function<void(uint64_t)>, uint64_t> > to_calls;
    to_calls.reserve(lock_reqs.size());
    std::vector<lock_req_t *>::const_iterator it;
    for (it = lock_reqs.begin(); it != lock_reqs.end(); it++) {
      verify((*it)->type == RLOCK && (*it)->status == lock_req_t::WAIT);
      (*it)->status = lock_req_t::LOCK;
      to_calls.push_back(
          std::pair<std::function<void(uint64_t)>, uint64_t>(
              (*it)->yes_callback,
              (*it)->id));
    }
    std::vector<std::pair<std::function<void(uint64_t)>, uint64_t> >::iterator
        cit;
    for (cit = to_calls.begin(); cit != to_calls.end(); cit++) {
      cit->first(cit->second);
    }
  }

  virtual uint64_t vlock(const std::function<void(uint64_t)> &yes_callback,
                         const std::function<void(void)> &no_callback,
                         type_t type,
                         int64_t priority,
                         const std::function<int(void)> &wound_callback);

  void sanity_check() {
    bool acquired_check = false;
    int64_t small = std::numeric_limits<int64_t>::min();
    int64_t tmp_small = -1;
    int64_t n_r_locked = 0;
    if (status_ == FREE)
      verify(requests_.size() == 0);
    std::list<lock_req_t>::iterator it = requests_.begin();
    for (; it != requests_.end(); it++) {
      if (!acquired_check) {
        if (status_ == WLOCKED) {
          acquired_check = true;
          verify(it == requests_.begin());
          verify(it->type == WLOCK);
          verify(it->status == lock_req_t::LOCK);
        }
        else {
          verify(status_ == RLOCKED);
          if (it->type == RLOCK) {
            verify(it->status == lock_req_t::LOCK);
            n_r_locked++;
          }
          else {
            verify(it->status == lock_req_t::WAIT);
            acquired_check = true;
            verify(n_r_locked == n_rlock_);
          }
        }
      }
      else {
        verify(it->status == lock_req_t::WAIT);

        // didn't check if wound callback is revoked or not
        verify(small < it->priority);
        if (it->type == RLOCK) {
          if (tmp_small < it->priority)
            tmp_small = it->priority;
        }
        else {
          if (tmp_small == -1) { // no R between two Ws
            small = it->priority;
          }
          else {
            small = tmp_small;
            verify(small < it->priority);
            tmp_small = -1;
          }
        }
      }
    }
  }

 public:
  WoundDieALock() :
      ALock(), requests_() {
  }

  virtual ~WoundDieALock();

  virtual void abort(uint64_t id);
};

class TimeoutALock: public ALock {
 protected:
  virtual uint64_t vlock(const std::function<void(uint64_t)> &yes_callback,
                         const std::function<void(void)> &no_callback,
                         type_t type,
                         int64_t,
                         const std::function<int(void)> &);

 public:
  enum mode_t { TIMEOUT, PROMPT };

  class ALockReq {
   public:
    /**
     * WAIT->RLOCK->UNLOCK
     * WAIT->WLOCK->UNLOCK
     * WAIT->ABORT
     * WAIT->TIMEOUT
     * TODO: RLOCK -> WLOCK
     */
    enum status_t { WAIT, LOCK, UNLOCK, ABORT, TIMEOUT };

    uint64_t id_;
    uint64_t alarm_id_ = 0;
    type_t type_;
    uint64_t timeout_ = 0;

    std::function<void(uint64_t)> yes_callback_;
    std::function<void()> no_callback_;

    uint64_t time_;
    status_t status_;

//        std::mutex mtx_;

    ALockReq(uint64_t id, type_t type)
        : id_(id), type_(type), time_(), timeout_(),
          status_(WAIT), yes_callback_(), no_callback_() { }


    ALockReq(uint64_t id,
             type_t type,
             const std::function<void(uint64_t)> &yes_cb,
             const std::function<void()> &no_cb,
             uint64_t tm_out) :
        id_(id),
        type_(type),
        yes_callback_(yes_cb),
        no_callback_(no_cb),
        time_(tm_out),
        status_(WAIT) {
    }

    status_t get_status() {
      //            std::lock_guard<std::mutex> guard(mtx_);
      return status_;
    }

    void set_status(status_t s) {
      //            std::lock_guard<std::mutex> guard(mtx_);
      status_ = s;
    }

    bool cas_status(status_t c, status_t s) {
      //            std::lock_guard<std::mutex> guard(mtx_);
      bool ret = (status_ == c);
      if (ret) {
        status_ = s;
      }
      return ret;
    }
  };

 public:

  static Alarm &get_alarm_s() {
    static Alarm alarm;
    return alarm;
  }

  uint64_t id_locked_ = 0;

  std::mutex lock_;
  std::list<ALockReq> requests_;
  //    uint64_t tm_last_ = 0;
  uint64_t tm_wait_;

  //    std::list<ALockReq> rreqs_;
  //    std::list<ALockReq> wreqs_;

  TimeoutALock(uint64_t time_wait = ALOCK_TIMEOUT) :
      ALock(),
      tm_wait_(time_wait) {
  }

  virtual ~TimeoutALock();

  //void safe_check() {
  //auto tm_now = rrr::Time::now();
  //if (tm_last_ != 0 && tm_now - tm_last_ > 5 * 1000 * 1000) {
  //    verify(0);
  //}
  //}

  void do_timeout(ALockReq &req) {
    // this should be try lock.

    auto func = req.no_callback_;
    bool call = req.cas_status(ALockReq::WAIT, ALockReq::TIMEOUT);
    if (call) {
      func();
    }
  }


  /**
   * return: how many i locked.
   */
  uint32_t lock_all(std::vector<ALockReq *> &lock_reqs);

  /**
   *
   */
  virtual void abort(uint64_t id);

};

class ALockGroup {
 public:

  enum status_t { INIT, WAIT, LOCK, TIMEOUT, UNLOCK };

  std::recursive_mutex mtx_locks_;

  std::map<ALock *, uint64_t> locked_;
  std::map<ALock *, ALock::type_t> tolock_;

  int64_t priority_;
  std::function<int(void)> wound_callback_;


  // INIT->WAIT->LOCK->UNLOCK
  // INIT->WAIT->TIMEOUT
  // TODO: LOCK->WAIT->LOCK->WAIT->LOCK
  // TODO: LOCK->WAIT->TIMEOUT
  status_t status_;
  std::mutex mtx_;

  std::function<void(void)> yes_callback_;
  std::function<void(void)> no_callback_;

  uint64_t n_locked_ = 0;
  uint64_t n_tolock_ = 0;

  DragonBall *db_;

  ALockGroup(int64_t priority = 0,
             const std::function<int(void)> &wound_callback
             = std::function<int(void)>()) :
      priority_(priority),
      wound_callback_(wound_callback),
      status_(INIT) {
  }

  bool cas_status(status_t c, status_t s) {
    //        std::lock_guard<std::mutex> guard(mtx_);
    if (status_ == c) {
      status_ = s;
      return true;
    }
    return false;
  }

  void set_status(status_t s) {
    //        std::lock_guard<std::mutex> guard(mtx_);
    status_ = s;
  }

  status_t get_status() {
    //        std::lock_guard<std::mutex> guard(mtx_);
    return status_;
  }

  void add(ALock *alock, ALock::type_t type = ALock::WLOCK) {


    auto status = get_status();
    if (status == INIT ||
        status == LOCK) {

      //	    mtx_locks_.lock();
      //	    tolock_.insert(std::pair<ALock*, uint64_t>(&alock, 0));
      //	    tolock_.insert(std::pair<ALock*, ALock::type_t>(&alock, type));
      tolock_[alock] = type;
      //	    mtx_locks_.unlock();
    } else {
      verify(0);
    }
  }

  void abort_all_locked() {
    //        mtx_locks_.lock();
    for (auto &pair: locked_) {
      auto &alock = pair.first;
      auto &areq_id = pair.second;
      if (areq_id != 0) {
        alock->abort(areq_id);
      }
    }
    //        mtx_locks_.unlock();
  }

  // After calling this, this group can be freed.
  void abort_all() {

    if (cas_status(LOCK, UNLOCK)) {
      abort_all_locked();
    } else {
      // TODO: what if this still waiting!!!???
      verify(0);
    }
  }

  void lock_all(const std::function<void(void)> &yes_cb,
                const std::function<void(void)> &no_cb);

  void unlock_all() {

    verify(cas_status(LOCK, UNLOCK));
    // abort all the locks.
    this->abort_all_locked();
  }

  ~ALockGroup() {

  }

};

} // namespace deptran
