#include "__dep__.h"
#include "config.h"
#include "scheduler.h"
#include "command.h"
#include "procedure.h"
#include "command_marshaler.h"
#include "rcc/dep_graph.h"
#include "rcc_service.h"
#include "classic/sched.h"
#include "tapir/sched.h"
#include "rcc/sched.h"
#include "janus/sched.h"
#include "benchmark_control_rpc.h"

namespace rococo {

ClassicServiceImpl::ClassicServiceImpl(Scheduler *sched,
                                       rrr::PollMgr* poll_mgr,
                                       ServerControlServiceImpl *scsi)
    : scsi_(scsi), dtxn_sched_(sched) {

#ifdef PIECE_COUNT
  piece_count_timer_.start();
  piece_count_prepare_fail_ = 0;
  piece_count_prepare_success_ = 0;
#endif

  if (Config::GetConfig()->do_logging()) {
    auto path = Config::GetConfig()->log_path();
    recorder_ = new Recorder(path);
    poll_mgr->add(recorder_);
  }

  this->RegisterStats();
}

void ClassicServiceImpl::Dispatch(const vector<SimpleCommand>& cmd,
                                  int32_t *res,
                                  TxnOutput* output,
                                  rrr::DeferredReply *defer) {
  std::lock_guard<std::mutex> guard(mtx_);

#ifdef PIECE_COUNT
  piece_count_key_t piece_count_key =
      (piece_count_key_t){header.t_type, header.p_type};
  std::map<piece_count_key_t, uint64_t>::iterator pc_it =
      piece_count_.find(piece_count_key);

  if (pc_it == piece_count_.end())
      piece_count_[piece_count_key] = 1;
  else
      piece_count_[piece_count_key]++;
  piece_count_tid_.insert(header.tid);
#endif

//  output->resize(output_size);
  // find stored procedure, and run it
  Coroutine::CreateRun([&]() {
    *res = SUCCESS;
    verify(cmd.size() > 0);
    for (auto &c: cmd) {
      dtxn_sched()->OnDispatch(const_cast<TxnPieceData &>(c), *output);
    }
    defer->reply();
  });
}

void ClassicServiceImpl::Prepare(const rrr::i64 &tid,
                                 const std::vector<i32> &sids,
                                 rrr::i32 *res,
                                 rrr::DeferredReply *defer) {
  std::lock_guard<std::mutex> guard(mtx_);
  auto sched = (ClassicSched*)dtxn_sched_;
  sched->OnPrepare(tid, sids, res, [defer] () {defer->reply();});
//  auto *dtxn = (TPLDTxn *) dtxn_sched_->get(tid);
//  dtxn->prepare_launch(sids, res, defer);
// TODO move the stat to somewhere else.
#ifdef PIECE_COUNT
  std::map<piece_count_key_t, uint64_t>::iterator pc_it;
  if (*res != SUCCESS)
      piece_count_prepare_fail_++;
  else
      piece_count_prepare_success_++;
  if (piece_count_timer_.elapsed() >= 5.0) {
      Log::info("PIECE_COUNT: txn served: %u", piece_count_tid_.size());
      Log::info("PIECE_COUNT: prepare success: %llu, failed: %llu",
        piece_count_prepare_success_, piece_count_prepare_fail_);
      for (pc_it = piece_count_.begin(); pc_it != piece_count_.end(); pc_it++)
          Log::info("PIECE_COUNT: t_type: %d, p_type: %d, started: %llu",
            pc_it->first.t_type, pc_it->first.p_type, pc_it->second);
      piece_count_timer_.start();
  }
#endif
}

void ClassicServiceImpl::Commit(const rrr::i64 &tid,
                                rrr::i32 *res,
                                rrr::DeferredReply *defer) {
  std::lock_guard<std::mutex> guard(mtx_);
  auto sched = (ClassicSched*) dtxn_sched_;
  sched->OnCommit(tid, SUCCESS, res, [defer] () {defer->reply();});
}

void ClassicServiceImpl::Abort(const rrr::i64 &tid,
                               rrr::i32 *res,
                               rrr::DeferredReply *defer) {
  Log::debug("get abort_txn: tid: %ld", tid);
  auto sched = (ClassicSched*) dtxn_sched_;
  sched->OnCommit(tid, REJECT, res, [defer] () {defer->reply();});
}


void ClassicServiceImpl::rpc_null(rrr::DeferredReply *defer) {
  defer->reply();
}



void ClassicServiceImpl::UpgradeEpoch(const uint32_t& curr_epoch,
                                  int32_t *res,
                                  DeferredReply* defer) {
  *res = dtxn_sched()->OnUpgradeEpoch(curr_epoch);
  defer->reply();
}

void ClassicServiceImpl::TruncateEpoch(const uint32_t& old_epoch,
                                   DeferredReply* defer) {
  std::function<void()> func = [&] () {
    dtxn_sched()->OnTruncateEpoch(old_epoch);
    defer->reply();
  };
  Coroutine::CreateRun(func);
}

void ClassicServiceImpl::TapirAccept(const cmdid_t& cmd_id,
                              const ballot_t& ballot,
                              const int32_t& decision,
                              rrr::DeferredReply* defer) {
  verify(0);
}

void ClassicServiceImpl::TapirFastAccept(const cmdid_t& cmd_id,
                                  const vector<SimpleCommand>& txn_cmds,
                                  rrr::i32* res,
                                  rrr::DeferredReply* defer) {
  TapirSched* sched = (TapirSched*)dtxn_sched_;
  sched->OnFastAccept(cmd_id, txn_cmds, res,
                       [defer] () {defer->reply();});
}

void ClassicServiceImpl::TapirDecide(const cmdid_t& cmd_id,
                                     const rrr::i32& decision,
                                     rrr::DeferredReply* defer) {
  TapirSched* sched = (TapirSched*)dtxn_sched_;
  sched->OnDecide(cmd_id, decision, [defer] () {defer->reply();});
}

void ClassicServiceImpl::RccDispatch(const vector<SimpleCommand>& cmd,
                                    int32_t* res,
                                    TxnOutput* output,
                                    MarshallDeputy* p_md_graph,
                                    DeferredReply* defer) {
  std::lock_guard <std::mutex> guard(this->mtx_);
  RccSched* sched = (RccSched*) dtxn_sched_;
  p_md_graph->SetMarshallable(new RccGraph(), true);
  sched->OnDispatch(cmd,
                    res,
                    output,
                    dynamic_cast<RccGraph*>(p_md_graph->data_),
                    [defer]() { defer->reply(); });
}

void ClassicServiceImpl::RccFinish(const cmdid_t& cmd_id,
                                   const MarshallDeputy& md_graph,
                                   TxnOutput* output,
                                   DeferredReply* defer) {
  const RccGraph& graph = dynamic_cast<const RccGraph&>(*md_graph.data_);
  verify(graph.size() > 0);
  std::lock_guard <std::mutex> guard(mtx_);
  RccSched* sched = (RccSched*) dtxn_sched_;
  sched->OnCommit(cmd_id,
                  graph,
                  output,
                  [defer]() { defer->reply(); });

  stat_sz_gra_commit_.sample(graph.size());
}

void ClassicServiceImpl::RccInquire(const epoch_t& epoch,
                                    const txnid_t& tid,
                                    MarshallDeputy *p_md_graph,
                                    rrr::DeferredReply *defer) {
  verify(IS_MODE_RCC || IS_MODE_RO6);
  std::lock_guard <std::mutex> guard(mtx_);
  RccSched* p_sched = (RccSched*) dtxn_sched_;
  p_md_graph->SetMarshallable(new RccGraph(), true);
  p_sched->OnInquire(epoch,
                     tid,
                     dynamic_cast<RccGraph*>(p_md_graph->data_),
                     [defer]() { defer->reply(); });
}

void ClassicServiceImpl::RccDispatchRo(const SimpleCommand &cmd,
                                       map <int32_t, Value> *output,
                                       rrr::DeferredReply *defer) {
  std::lock_guard <std::mutex> guard(mtx_);
  verify(0);
  RccDTxn *dtxn = (RccDTxn *) dtxn_sched_->GetOrCreateDTxn(cmd.root_id_, true);
  dtxn->start_ro(cmd, *output, defer);
}

void ClassicServiceImpl::JanusDispatch(const vector<SimpleCommand>& cmd,
                                     int32_t* p_res,
                                     TxnOutput* p_output,
                                     MarshallDeputy* p_md_res_graph,
                                     DeferredReply* p_defer) {
  std::lock_guard <std::mutex> guard(this->mtx_);
  RccGraph* p_res_graph = new RccGraph();
  JanusSched* sched = (JanusSched*) dtxn_sched_;
  sched->OnDispatch(cmd,
                    p_res,
                    p_output,
                    p_res_graph,
                    [p_defer, p_res_graph, p_md_res_graph] () {
                      if (p_res_graph->size() <= 1) {
                        p_md_res_graph->SetMarshallable(new EmptyGraph, true);
                        delete p_res_graph;
                      } else {
                        p_md_res_graph->SetMarshallable(p_res_graph, true);
                      }
                      p_defer->reply();
                    });
}

void ClassicServiceImpl::JanusCommit(const cmdid_t& cmd_id,
                                     const MarshallDeputy& graph,
                                     int32_t *res,
                                     TxnOutput* output,
                                     DeferredReply* defer) {
  std::lock_guard <std::mutex> guard(mtx_);
  RccGraph* p_graph = dynamic_cast<RccGraph*>(graph.data_);
  JanusSched* p_sched = (JanusSched*) dtxn_sched_;
  p_sched->OnCommit(cmd_id,
                    p_graph,
                    res,
                    output,
                    [defer]() { defer->reply(); });
}

void ClassicServiceImpl::JanusCommitWoGraph(const cmdid_t& cmd_id,
                                       int32_t *res,
                                       TxnOutput* output,
                                       DeferredReply* defer) {
  std::lock_guard <std::mutex> guard(mtx_);
  JanusSched* sched = (JanusSched*) dtxn_sched_;
  sched->OnCommit(cmd_id,
                         nullptr,
                         res,
                         output,
                         [defer]() { defer->reply(); });
}

void ClassicServiceImpl::JanusInquire(const epoch_t& epoch,
                                      const cmdid_t &tid,
                                      MarshallDeputy *p_md_graph,
                                      rrr::DeferredReply *defer) {
  std::lock_guard <std::mutex> guard(mtx_);
  p_md_graph->SetMarshallable(new RccGraph(), true);
  JanusSched* p_sched = (JanusSched*) dtxn_sched_;
  p_sched->OnInquire(epoch,
                     tid,
                     dynamic_cast<RccGraph*>(p_md_graph->data_),
                     [defer]() { defer->reply(); });
}

void ClassicServiceImpl::JanusPreAccept(const cmdid_t &txnid,
                                        const vector<SimpleCommand>& cmds,
                                        const MarshallDeputy& md_graph,
                                        int32_t* res,
                                        MarshallDeputy* p_md_res_graph,
                                        DeferredReply* defer) {
  std::lock_guard <std::mutex> guard(mtx_);
  RccGraph* p_graph = dynamic_cast<RccGraph*>(md_graph.data_);
  verify(p_graph);
  p_md_res_graph->SetMarshallable(new RccGraph(), true);
  JanusSched* sched = (JanusSched*) dtxn_sched_;
  sched->OnPreAccept(txnid,
                     cmds,
                     p_graph,
                     res,
                     dynamic_cast<RccGraph*>(p_md_res_graph->data_),
                     [defer] () {defer->reply();});
}

void ClassicServiceImpl::JanusPreAcceptWoGraph(const cmdid_t& txnid,
                                             const vector<SimpleCommand>& cmds,
                                             int32_t* res,
                                             MarshallDeputy* res_graph,
                                             DeferredReply* defer) {
  std::lock_guard <std::mutex> guard(mtx_);
  res_graph->SetMarshallable(new RccGraph(), true);
  JanusSched* p_sched = (JanusSched*) dtxn_sched_;
  p_sched->OnPreAccept(txnid,
                       cmds,
                       nullptr,
                       res,
                       dynamic_cast<RccGraph*>(res_graph->data_),
                       [defer] () {defer->reply();});
}


void ClassicServiceImpl::JanusAccept(const cmdid_t &txnid,
                                   const ballot_t& ballot,
                                   const MarshallDeputy& md_graph,
                                   int32_t* res,
                                   DeferredReply* defer) {
  RccGraph* graph = dynamic_cast<RccGraph*>(md_graph.data_);
  verify(graph);
  verify(md_graph.kind_ == MarshallDeputy::RCC_GRAPH);
  JanusSched* sched = (JanusSched*) dtxn_sched_;
  sched->OnAccept(txnid,
                  ballot,
                  *graph,
                  res,
                  [defer] () {defer->reply();});
}


void ClassicServiceImpl::RegisterStats() {
  if (scsi_) {
    scsi_->set_recorder(recorder_);
    scsi_->set_recorder(recorder_);
    scsi_->set_stat(ServerControlServiceImpl::STAT_SZ_SCC,
                    &stat_sz_scc_);
    scsi_->set_stat(ServerControlServiceImpl::STAT_SZ_GRAPH_START,
                    &stat_sz_gra_start_);
    scsi_->set_stat(ServerControlServiceImpl::STAT_SZ_GRAPH_COMMIT,
                    &stat_sz_gra_commit_);
    scsi_->set_stat(ServerControlServiceImpl::STAT_SZ_GRAPH_ASK,
                    &stat_sz_gra_ask_);
    scsi_->set_stat(ServerControlServiceImpl::STAT_N_ASK,
                    &stat_n_ask_);
    scsi_->set_stat(ServerControlServiceImpl::STAT_RO6_SZ_VECTOR,
                    &stat_ro6_sz_vector_);
  }
}


} // namespace rcc
