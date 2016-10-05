#pragma once

#include "__dep__.h"
#include "config.h"
#include "communicator.h"
#include "txn_chopper.h"

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
  Communicator* commo_;
  cliid_t cli_id_;
  int32_t benchmark;
  int32_t mode;
  bool batch_start;
  uint32_t id;
  uint32_t duration;
  ClientControlServiceImpl *ccsi;
  uint32_t n_concurrent_;
  rrr::Mutex finish_mutex;
  rrr::CondVar finish_cond;
  bool forward_requests_to_leader_ = false;

  // coordinators_{mutex, cond} synchronization currently only used for open clients
  std::mutex request_gen_mutex;
  std::mutex coordinator_mutex;
  vector<CoordinatorBase*> free_coordinators_ = {};
  vector<Coordinator*> created_coordinators_ = {};
  rrr::ThreadPool* dispatch_pool_ = new rrr::ThreadPool();

  std::atomic<uint32_t> num_txn, success, num_try;
  TxnGenerator * txn_generator_;
  Timer *timer_;
  TxnRegistry* txn_reg_ = nullptr;
  Config* config_;
  Config::SiteInfo& my_site_;
  vector<string> servers_;
 public:
  ClientWorker(uint32_t id,
               Config::SiteInfo &site_info,
               Config *config,
               ClientControlServiceImpl *ccsi);
  ClientWorker() = delete;
  ~ClientWorker();
  void work();
  Coordinator* FindOrCreateCoordinator();
  void DispatchRequest(Coordinator *coo);
  void AcceptForwardedRequest(TxnRequest &request, TxnReply* txn_reply, rrr::DeferredReply* defer);

 protected:
  Coordinator* CreateCoordinator(uint16_t offset_id);
  void RequestDone(Coordinator* coo, TxnReply &txn_reply);
  void ForwardRequestDone(Coordinator* coo, TxnReply* output, rrr::DeferredReply* defer, TxnReply &txn_reply);
};
} // namespace rococo


