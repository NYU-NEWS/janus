#pragma once

#include "__dep__.h"
#include "marshal-value.h"
#include "rcc/graph.h"
#include "rcc/graph_marshaler.h"
#include "command.h"
#include "command_marshaler.h"
#include "rcc_rpc.h"
#include "rcc_service.h"
#include "sharding.h"
#include "dtxn.h"
#include "piece.h"

namespace rococo {

class ServerWorker {
 public:
  rrr::PollMgr *svr_poll_mgr_g = nullptr;
  vector<rrr::Service*> services_ = {};
  rrr::Server *svr_server_g = nullptr;
  base::ThreadPool *thread_pool_g = nullptr;


  rrr::PollMgr *svr_hb_poll_mgr_g = nullptr;
  ServerControlServiceImpl *scsi_g = nullptr;
  rrr::Server *svr_hb_server_g = nullptr;
  base::ThreadPool *hb_thread_pool_g = nullptr;

  Config::SiteInfo *site_info_ = nullptr;
  Sharding *sharding_ = nullptr;
  Scheduler * txn_mgr_ = nullptr;
  TxnRegistry *txn_reg_ = nullptr;

  void SetupHeartbeat();
  void PopTable();
  void SetupService();
  void RegPiece();
  void ShutDown();
};


} // namespace rococo
