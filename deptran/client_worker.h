#pragma once

#include "all.h"


namespace rococo {

class ClientControlServiceImpl;
class TxnRequestFactory;

class ClientWorker {
 public:
  uint32_t coo_id;
  std::vector<std::string> *servers;
  int32_t benchmark;
  int32_t mode;
  bool batch_start;
  uint32_t id;
  uint32_t duration;
  ClientControlServiceImpl *ccsi;
  uint32_t n_outstanding_;
  rrr::Mutex finish_mutex;
  rrr::CondVar finish_cond;
  Coordinator *coo_;
  std::atomic<uint32_t> num_txn, success, num_try;
  TxnRequestFactory* txn_req_factory_;
 public:
  ClientWorker() = default;

  Coordinator* GetCoord();

  void callback2(TxnReply &txn_reply);

  void work();
};
} // namespace rococo


