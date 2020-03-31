#pragma once

#include "../janus/frame.h"
#include "../janus/scheduler.h"

namespace janus {

class TroadFrame : public JanusFrame {
 public:
  TroadFrame(int mode = MODE_TROAD) : JanusFrame(mode) {}
  Executor *CreateExecutor(cmdid_t, TxLogServer *sched) override;
  Coordinator *CreateCoordinator(cooid_t coo_id,
                                 Config *config,
                                 int benchmark,
                                 ClientControlServiceImpl *ccsi,
                                 uint32_t id,
                                 shared_ptr<TxnRegistry> txn_reg) override;
  TxLogServer *CreateScheduler() override;
  vector<rrr::Service *> CreateRpcServices(uint32_t site_id,
                                           TxLogServer *dtxn_sched,
                                           rrr::PollMgr *poll_mgr,
                                           ServerControlServiceImpl *scsi)
  override;
  mdb::Row *CreateRow(const mdb::Schema *schema,
                      vector<Value> &row_data) override;

  shared_ptr<Tx> CreateTx(epoch_t epoch, txnid_t tid,
                          bool ro, TxLogServer *mgr) override;

  Communicator *CreateCommo(PollMgr *poll = nullptr) override;
};

class TroadJanusFrame : public TroadFrame {
 public:
  using TroadFrame::TroadFrame;
  shared_ptr<Tx> CreateTx(epoch_t epoch, txnid_t tid,
                          bool ro, TxLogServer *mgr) override;
  Coordinator* CreateCoordinator(cooid_t coo_id,
                                 Config *config,
                                 int benchmark,
                                 ClientControlServiceImpl *ccsi,
                                 uint32_t id,
                                 shared_ptr<TxnRegistry> txn_reg) override;

};

class RccGraph;
class TroadCommo;
class SchedulerTroad : public SchedulerJanus {
 public:
  using SchedulerJanus::SchedulerJanus;

  map<txnid_t, shared_ptr<RccTx>> Aggregate(RccGraph& graph) override;

//  int OnPreAccept(txnid_t txnid,
//                  rank_t rank,
//                  const vector<SimpleCommand> &cmds,
//                  shared_ptr<RccGraph> sp_graph,
//                  shared_ptr<RccGraph> res_graph);

  int OnPreAccept(txnid_t txnid,
                  rank_t rank,
                  const vector<SimpleCommand> &cmds,
                  shared_ptr<RccGraph> sp_graph,
                  shared_ptr<RccGraph> res_graph) override;

  int OnAccept(txnid_t txn_id,
               rank_t rank,
               const ballot_t& ballot,
               shared_ptr<RccGraph> graph);

  int OnCommit(txnid_t txn_id,
               rank_t rank,
               bool need_validation,
               shared_ptr<RccGraph> sp_graph,
               TxnOutput *output) override;

  TroadCommo* commo();

};
} // namespace janus
