#pragma once

#include "__dep__.h"
#include "coordinator.h"
#include "benchmark_control_rpc.h"
#include "frame.h"
#include "scheduler.h"
#include "communicator.h"
#include "config.h"

namespace janus {

class SubmitPool {
private:
  struct start_submit_pool_args {
    SubmitPool* subpool;
  };

  int n_;
  rrr::Queue<std::function<void()>*> q_;
  pthread_t th_;
  bool should_stop_{false};

  static void* start_thread_pool(void* args) {
    start_submit_pool_args* t_args = (start_submit_pool_args *) args;
    t_args->subpool->run_thread();
    delete t_args;
    pthread_exit(nullptr);
    return nullptr;
  }
  void run_thread() {
    for (;;) {
      function<void()>* job = nullptr;
      job = q_.pop();
      if (job == nullptr) { 
        break;
      }
      (*job)();
      delete job;
    }
  }

public:
  SubmitPool()
  : n_(1), th_(0) {
    verify(n_ >= 0);
    for (int i = 0; i < n_; i++) {
      start_submit_pool_args* args = new start_submit_pool_args();
      args->subpool = this;
      Pthread_create(&th_, nullptr, SubmitPool::start_thread_pool, args);
    }
  }
  SubmitPool(const SubmitPool&) = delete;
  SubmitPool& operator=(const SubmitPool&) = delete;
  ~SubmitPool() {
    should_stop_ = true;
    for (int i = 0; i < n_; i++) {
      q_.push(nullptr);  // death pill
    }
    for (int i = 0; i < n_; i++) {
      Pthread_join(th_, nullptr);
    }
    wait_for_all();
  }
  void wait_for_all() {
    Log_debug("%s: enter in", __FUNCTION__);
    for (int i = 0; i < n_; i++) {
      function<void()>* job;
      while (q_.try_pop(&job)) {
        if (job != nullptr) {
          (*job)();
          delete job;
        }
      }
    }
  }
  int add(const std::function<void()>& f) {
    if (should_stop_) {
      return -1;
    }
    q_.push(new function<void()>(f));
    return 0;
  }
};

class LogEntry : public Marshallable {
public:
  char* operation_ = nullptr;
  int length = 0;
  std::string log_entry;

  LogEntry() : Marshallable(MarshallDeputy::CONTAINER_CMD) {}
  virtual ~LogEntry() {
    if (operation_ != nullptr) delete operation_;
    operation_ = nullptr;
  }
  virtual Marshal& ToMarshal(Marshal&) const override;
  virtual Marshal& FromMarshal(Marshal&) override;
};

class PaxosWorker {
private:
  inline void _Submit(shared_ptr<Marshallable>);

  rrr::Mutex finish_mutex{};
  rrr::CondVar finish_cond{};
  std::atomic<int> n_current{0};
  std::function<void(const char*, int)> callback_ = nullptr;
  vector<Coordinator*> created_coordinators_{};
  struct timeval t1;
  struct timeval t2;

public:
  SubmitPool* submit_pool = nullptr;
  rrr::PollMgr* svr_poll_mgr_ = nullptr;
  vector<rrr::Service*> services_ = {};
  rrr::Server* rpc_server_ = nullptr;
  base::ThreadPool* thread_pool_g = nullptr;
  // for microbench
  std::atomic<int> submit_num{0};
  int tot_num = 0;
  int submit_tot_sec_ = 0;
  int submit_tot_usec_ = 0;

  rrr::PollMgr* svr_hb_poll_mgr_g = nullptr;
  ServerControlServiceImpl* scsi_ = nullptr;
  rrr::Server* hb_rpc_server_ = nullptr;
  base::ThreadPool* hb_thread_pool_g = nullptr;

  Config::SiteInfo* site_info_ = nullptr;
  Frame* rep_frame_ = nullptr;
  Scheduler* rep_sched_ = nullptr;
  Communicator* rep_commo_ = nullptr;

  void SetupHeartbeat();
  void SetupBase();
  void SetupService();
  void SetupCommo();
  void ShutDown();
  void Next(Marshallable&);
  void WaitForSubmit();

  static const uint32_t CtrlPortDelta = 10000;
  void WaitForShutdown();
  bool IsLeader();

  void SubmitExample();
  void Submit(const char*, int);
  void register_apply_callback(std::function<void(const char*, int)>);
};

} // namespace janus
