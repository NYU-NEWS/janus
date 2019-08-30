#pragma once

#include "__dep__.h"
#include "coordinator.h"
#include "benchmark_control_rpc.h"
#include "frame.h"
#include "scheduler.h"
#include "communicator.h"
#include "config.h"

namespace janus {

class LogEntry : public Marshallable {
public:
  char* operation_ = nullptr;
  int length = 0;

  LogEntry() : Marshallable(MarshallDeputy::CONTAINER_CMD) {}
  virtual ~LogEntry() {
    if (operation_ != nullptr) delete operation_;
  }
  virtual Marshal& ToMarshal(Marshal&) const override;
  virtual Marshal& FromMarshal(Marshal&) override;
};

class PaxosWorker {
private:
  void _Submit(shared_ptr<Marshallable>);

  rrr::Mutex finish_mutex{};
  rrr::CondVar finish_cond{};
  uint32_t n_current = 0;
  std::function<void(char*, int)> callback_ = nullptr;
  vector<Coordinator*> created_coordinators_{};
  struct timeval t1;
  struct timeval t2;

public:
  rrr::PollMgr* svr_poll_mgr_ = nullptr;
  vector<rrr::Service*> services_ = {};
  rrr::Server* rpc_server_ = nullptr;
  base::ThreadPool* thread_pool_g = nullptr;
  // for microbench
  std::atomic<int> submit_num{0};
  int tot_num = 0;
  int submit_tot_sec_ = 0;
  int submit_tot_usec_ = 0;
  int commit_tot_sec_ = 0;
  int commit_tot_usec_ = 0;
  struct timeval commit_time_;
  struct timeval leader_commit_time_;

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
  void register_apply_callback(std::function<void(char*, int)>);
};

} // namespace janus
