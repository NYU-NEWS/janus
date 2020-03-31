#pragma once

#include "__dep__.h"
#include "marshal-value.h"
#include "rcc/graph.h"
#include "rcc/tx.h"
#include "rcc/graph_marshaler.h"
#include "command.h"
#include "procedure.h"
#include "command_marshaler.h"
#include "rcc_rpc.h"
#include "service.h"
#include "sharding.h"
#include "tx.h"
#include "workload.h"
#include "config.h"

namespace janus {

class Communicator;
class Frame;
class ServerWorker {
 public:
  rrr::PollMgr *svr_poll_mgr_ = nullptr;
  base::ThreadPool *svr_thread_pool_ = nullptr;
  vector<rrr::Service*> services_ = {};
  rrr::Server *rpc_server_ = nullptr;
  base::ThreadPool *thread_pool_g = nullptr;

  rrr::PollMgr *svr_hb_poll_mgr_g = nullptr;
  ServerControlServiceImpl *scsi_ = nullptr;
  rrr::Server *hb_rpc_server_ = nullptr;
  base::ThreadPool *hb_thread_pool_g = nullptr;

  Frame* tx_frame_ = nullptr;
  Frame* rep_frame_ = nullptr;
  Config::SiteInfo *site_info_ = nullptr;
  Sharding *sharding_ = nullptr;
  TxLogServer *tx_sched_ = nullptr;
  TxLogServer *rep_sched_ = nullptr;
  shared_ptr<TxnRegistry> tx_reg_{nullptr};

  Communicator *tx_commo_ = nullptr;
  Communicator *rep_commo_ = nullptr;

  bool launched_{false};

  int DbChecksum();
  void SetupHeartbeat();
  void PopTable();
  void SetupBase();
  void SetupService();
  void SetupCommo();
  void RegisterWorkload();
  void ShutDown();

  static const uint32_t CtrlPortDelta = 10000;
  void WaitForShutdown();
};


} // namespace janus
