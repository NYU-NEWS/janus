#pragma once

#include "__dep__.h"
#include "marshal-value.h"
#include "rcc/graph.h"
#include "rcc/graph_marshaler.h"
#include "rcc_rpc.h"
#include "rcc_service.h"
#include "sharding.h"
#include "dtxn.h"
#include "piece.h"

namespace rococo {

class ServerWorker {
 public:
  rrr::PollMgr *svr_poll_mgr_g;
  RococoServiceImpl *rsi_g;
  rrr::Server *svr_server_g;
  base::ThreadPool *thread_pool_g;


  rrr::PollMgr *svr_hb_poll_mgr_g;
  ServerControlServiceImpl *scsi_g;
  rrr::Server *svr_hb_server_g;
  base::ThreadPool *hb_thread_pool_g;

  Config::SiteInfo *site_info_;
  Sharding *sharding_;
  Scheduler * txn_mgr_;
  TxnRegistry *txn_reg_;

  void SetupHeartbeat();
  void PopTable();
  void SetupService();
  void RegPiece();
  void ShutDown();
};


} // namespace rococo
