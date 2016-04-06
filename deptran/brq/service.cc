#include "../__dep__.h"
#include "../config.h"
#include "../command.h"
#include "../command_marshaler.h"
#include "../benchmark_control_rpc.h"
#include "sched.h"
#include "dtxn.h"
#include "dep_graph.h"
#include "../rcc/graph_marshaler.h"
#include "service.h"

namespace rococo {

BrqServiceImpl::BrqServiceImpl(Scheduler *sched,
                               rrr::PollMgr* poll_mgr,
                               ServerControlServiceImpl *scsi)
    : scsi_(scsi), dtxn_sched_(sched) {

#ifdef PIECE_COUNT
  piece_count_timer_.start();
  piece_count_prepare_fail_ = 0;2
  piece_count_prepare_success_ = 0;
#endif

  if (Config::GetConfig()->do_logging()) {
    auto path = Config::GetConfig()->log_path();
    recorder_ = new Recorder(path);
    poll_mgr->add(recorder_);
  }

  this->RegisterStats();
}

// TODO find a better way to define batch
//void BrqServiceImpl::rcc_batch_start_pie(
//    const std::vector <RequestHeader> &headers,
//    const std::vector <map<int32_t, Value>> &inputs,
//    BatchChopStartResponse *res,
//    rrr::DeferredReply *defer) {
//
//  verify(false);
//    verify(IS_MODE_RCC || IS_MODE_RO6);
//    auto txn = (RCCDTxn*) txn_mgr_->get_or_create(headers[0].tid);
//
//    res->is_defers.resize(headers.size());
//    res->outputs.resize(headers.size());
//
//    auto job = [&headers, &inputs, res, defer, this, txn] () {
//        std::lock_guard<std::mutex> guard(mtx_);
//
//        Log::debug("batch req, headers size:%u", headers.size());
//        auto &tid = headers[0].tid;
////    Vertex<TxnInfo> *tv = RCC::dep_s->start_pie_txn(tid);
//
//        for (int i = 0; i < headers.size(); i++) {
//            auto &header = headers[i];
//            auto &input = inputs[i];
//            auto &output = res->outputs[i];
//
//            //    Log::debug("receive start request. txn_id: %llx, pie_id: %llx", header.tid, header.pid);
//
//            bool deferred;
////            txn->start(header, input, &deferred, &output); FIXME this is missing !!!
//            res->is_defers[i] = deferred ? 1 : 0;
//
//        }
//        RCCDTxn::dep_s->sub_txn_graph(tid, res->gra_m);
//        defer->reply();
//
//    };
//    static bool do_record = Config::get_config()->do_logging();
//
//    if (do_record) {
//        rrr::Marshal m;
//        m << headers << inputs;
//
//        recorder_->submit(m, job);
//    } else {
//        job();
//    }
//}


void BrqServiceImpl::Dispatch(const SimpleCommand& cmd,
                              int32_t* res,
                              map<int32_t, Value>* output,
                              BrqGraph* graph,
                              DeferredReply* defer) {
  std::lock_guard <std::mutex> guard(this->mtx_);
  dtxn_sched()->OnDispatch(cmd,
                           res,
                           output,
                           graph,
                           [defer]() { defer->reply(); });
}

//void BrqServiceImpl::rcc_start_pie(const SimpleCommand &cmd,
//                                      ChopStartResponse *res,
//                                      rrr::DeferredReply *defer
//) {
//  verify(0);
//    Log::debug("receive start request. txn_id: %llx, pie_id: %llx", header.tid, header.pid);
//  verify(IS_MODE_RCC || IS_MODE_RO6);
//  verify(defer);
//
//  std::lock_guard <std::mutex> guard(this->mtx_);
//  RccDTxn *dtxn = (RccDTxn *) dtxn_sched_->GetOrCreateDTxn(cmd.root_id_);
//  dtxn->StartLaunch(cmd, res, defer);

// TODO remove the stat from here.
//    auto sz_sub_gra = RCCDTxn::dep_s->sub_txn_graph(header.tid, res->gra_m);
//    stat_sz_gra_start_.sample(sz_sub_gra);
//    if (IS_MODE_RO6) {
//        stat_ro6_sz_vector_.sample(res->read_only.size());
//    }
//}

void BrqServiceImpl::Commit(const cmdid_t& cmd_id,
                            const RccGraph& graph,
                            int32_t *res,
                            TxnOutput* output,
                            DeferredReply* defer) {
  verify(graph.size() > 0);
  std::lock_guard <std::mutex> guard(mtx_);
  dtxn_sched()->OnCommit(cmd_id,
                         graph,
                         res,
                         output,
                         [defer]() { defer->reply(); });
//  RccDTxn *txn = (RccDTxn *) dtxn_sched_->GetDTxn(req.txn_id);
//  txn->commit(req, res, defer);

  stat_sz_gra_commit_.sample(graph.size());
}

// equivalent to commit phrase
//void BrqServiceImpl::rcc_finish_txn(
//    const ChopFinishRequest &req,
//    ChopFinishResponse *res,
//    rrr::DeferredReply *defer) {
//  Log::debug("receive finish request. txn_id: %llx, graph size: %d", req.txn_id, req.gra.size());
//  verify(IS_MODE_RCC || IS_MODE_RO6);
//  verify(defer);
//  verify(req.gra.size() > 0);
//
//  std::lock_guard <std::mutex> guard(mtx_);
//  RccDTxn *txn = (RccDTxn *) dtxn_sched_->GetDTxn(req.txn_id);
//  txn->commit(req, res, defer);
//
//  stat_sz_gra_commit_.sample(req.gra.size());
//}

void BrqServiceImpl::Inquire(const cmdid_t &tid,
                             BrqGraph *graph,
                             rrr::DeferredReply *defer) {
  std::lock_guard <std::mutex> guard(mtx_);
  dtxn_sched()->OnInquire(tid, graph, [defer]() { defer->reply(); });
//  RccDTxn *dtxn = (RccDTxn *) dtxn_sched_->GetDTxn(tid);
//  dtxn->inquire(res, defer);
}

void BrqServiceImpl::rcc_ro_start_pie(const SimpleCommand &cmd,
                                         map <int32_t, Value> *output,
                                         rrr::DeferredReply *defer) {
  std::lock_guard <std::mutex> guard(mtx_);
  verify(0);
//  BrqDTxn *dtxn = (BrqDTxn *) dtxn_sched_->GetOrCreateDTxn(cmd.root_id_, true);
//  dtxn->start_ro(cmd, *output, defer);
}

void BrqServiceImpl::PreAccept(const cmdid_t &txnid,
                               const RccGraph& graph,
                               int32_t* res,
                               RccGraph* res_graph,
                               DeferredReply* defer) {
  dtxn_sched()->OnPreAccept(txnid,
                            graph,
                            res,
                            res_graph,
                            [defer] () {defer->reply();});
}

void BrqServiceImpl::RegisterStats() {
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

BrqSched* BrqServiceImpl::dtxn_sched() {
  return (BrqSched*)dtxn_sched_;
}


} // namespace rococo
