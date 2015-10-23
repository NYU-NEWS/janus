#pragma once

#include "__dep__.h"
#include "marshal-value.h"
#include "graph.h"
#include "graph_marshaler.h"
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
  DTxnMgr* txn_mgr_;

  void SetupHeartbeat();
  void PopTable();
  void SetupService();

  static map<uint32_t, ServerWorker*> server_workers_s;

  static void server_reg_piece() {
    auto benchmark = Config::GetConfig()->get_benchmark();
    Piece *piece = Piece::get_piece(benchmark);
    piece->reg_all();
    delete piece;
    piece = NULL;
  }
};


} // namespace rococo
