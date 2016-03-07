#pragma once

#include "__dep__.h"
#include "config.h"

namespace rococo {

class ClientControlServiceImpl;
class TxnGenerator;
class CoordinatorBase;
class Frame;
class Coordinator;
class TxnRegistry;
class TxnReply;

class ClientWorker {
 public:
  Frame* frame_;
  uint32_t coo_id;
  int32_t benchmark;
  int32_t mode;
  bool batch_start;
  uint32_t id;
  uint32_t duration;
  ClientControlServiceImpl *ccsi;
  uint32_t n_outstanding_;
  rrr::Mutex finish_mutex;
  rrr::CondVar finish_cond;
  CoordinatorBase* coo_ = nullptr;
  Coordinator* rep_coord_ = nullptr;
  std::atomic<uint32_t> num_txn, success, num_try;
  TxnGenerator * txn_req_factory_;
  Timer *timer_;
  TxnRegistry* txn_reg_ = nullptr;
  Config* config;
  Config::SiteInfo& my_site_;
  vector<string> servers_;
 public:
  ClientWorker(uint32_t id,
               Config::SiteInfo &site_info,
               Config *config,
               ClientControlServiceImpl *ccsi);
  ClientWorker() = delete;
  ~ClientWorker();

  void RequestDone(TxnReply &txn_reply);

  void work();
};
} // namespace rococo


