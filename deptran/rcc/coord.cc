#include "marshal-value.h"
#include "coord.h"
#include "frame.h"
#include "dtxn.h"
#include "dep_graph.h"
#include "../benchmark_control_rpc.h"

namespace rococo {


RccCommo* RccCoord::commo() {
  if (commo_ == nullptr) {
    commo_ = frame_->CreateCommo();
  }
  verify(commo_ != nullptr);
  return dynamic_cast<RccCommo*>(commo_);
}

void RccCoord::PreDispatch() {
  verify(ro_state_ == BEGIN);
  TxnCommand* txn = dynamic_cast<TxnCommand*>(cmd_);
//  auto dispatch = txn->is_read_only() ?
//                  std::bind(&RccCoord::DispatchRo, this) :
//                  std::bind(&RccCoord::Dispatch, this);
  auto dispatch = std::bind(&RccCoord::Dispatch, this);
  if (recorder_) {
    std::string log_s;
    // TODO get appropriate log
//    req.get_log(cmd_->id_, log_s);
    recorder_->submit(log_s, dispatch);
  } else {
    dispatch();
  }
}


void RccCoord::Dispatch() {
  std::lock_guard<std::recursive_mutex> lock(mtx_);
//  phase_++;
  // new txn id for every new and retry.
//  RequestHeader header = gen_header(ch);

  int pi;

  map<int32_t, Value> *input = nullptr;
  int32_t server_id;
  int     res;
  int     output_size;

  int cnt;
  while (cmd_->HasMoreSubCmdReadyNotOut()) {
    auto subcmd = (SimpleCommand*) cmd_->GetNextReadySubCmd();
    subcmd->id_ = next_pie_id();
    verify(subcmd->root_id_ == cmd_->id_);
    n_dispatch_++;
    cnt++;
    Log_debug("send out start request %ld, cmd_id: %lx, inn_id: %d, pie_id: %lx",
              n_dispatch_, cmd_->id_, subcmd->inn_id_, subcmd->id_);
    dispatch_acks_[subcmd->inn_id()] = false;
    auto func = std::bind(&RccCoord::DispatchAck,
                          this,
                          phase_,
                          std::placeholders::_1,
                          std::placeholders::_2,
                          std::placeholders::_3);
    commo()->SendHandout(*subcmd, func);
  }
}

void RccCoord::DispatchAck(phase_t phase,
                           int res,
                           SimpleCommand &cmd,
                           RccGraph &graph) {
  std::lock_guard<std::recursive_mutex> lock(this->mtx_);
  verify(phase == phase_); // cannot proceed without all acks.
  TxnInfo& info = *graph.vertex_index_.at(cmd.root_id_)->data_;
  verify(cmd.root_id_ == info.id());
  verify(info.partition_.find(cmd.partition_id_) != info.partition_.end());
  n_dispatch_ack_++;
  TxnCommand *txn = (TxnCommand *) cmd_;
  dispatch_acks_[cmd.inn_id_] = true;

  Log_debug("get start ack %ld/%ld for cmd_id: %lx, inn_id: %d",
            n_dispatch_ack_, n_dispatch_, txn->id_, cmd.inn_id_);

  bool early_return = false;

  // where should I store this graph?
  Log_debug("start response graph size: %d", (int)graph.size());
  verify(graph.size() > 0);
  graph_.Aggregate(graph);

//  Log_debug(
//      "receive deptran start response, tid: %llx, pid: %llx, graph size: %d",
//      cmd.root_id_,
//      cmd->id_,
//      gra.size());

  // TODO?
  if (graph.size() > 1) txn->disable_early_return();

  txn->Merge(cmd);

  if (txn->HasMoreSubCmdReadyNotOut()) {
    Log_debug("command has more sub-cmd, cmd_id: %lx,"
                  " n_started_: %d, n_pieces: %d",
              txn->id_, txn->n_pieces_out_, txn->GetNPieceAll());
    Dispatch();
  } else if (AllDispatchAcked()) {
    Log_debug("receive all start acks, txn_id: %llx; START PREPARE", cmd_->id_);
    verify(!txn->do_early_return());
    GotoNextPhase();
  }
}

/** caller should be thread safe */
void RccCoord::Finish() {
  TxnCommand *ch = (TxnCommand*) cmd_;
  // commit or abort piece
  Log_debug(
    "send rcc finish requests to %d servers, tid: %llx, graph size: %d",
    (int)ch->partition_ids_.size(),
    cmd_->id_,
    graph_.size());
  TxnInfo& info = *graph_.FindV(cmd_->id_)->data_;
  verify(ch->partition_ids_.size() == info.partition_.size());
  info.union_status(TXN_CMT);

  verify(graph_.size() > 0);

  for (auto& rp : ch->partition_ids_) {
    commo()->SendFinish(rp,
                        cmd_->id_,
                        graph_,
                        std::bind(&RccCoord::FinishAck,
                                  this,
                                  phase_,
                                  SUCCESS,
                                  std::placeholders::_1));
//    Future::safe_release(proxy->async_rcc_finish_txn(req, fuattr));
  }
}

void RccCoord::FinishAck(phase_t phase,
                         int res,
                         map<innid_t, map<int32_t, Value>>& output) {
  std::lock_guard<std::recursive_mutex> lock(this->mtx_);
  TxnCommand* txn = (TxnCommand*) cmd_;
  verify(phase_ == phase);
  n_finish_ack_++;

  Log_debug("receive finish response. tid: %llx", cmd_->id_);
  txn->outputs_.insert(output.begin(), output.end());

  verify(!txn->do_early_return());
  if (n_finish_ack_ == txn->GetPartitionIds().size()) {
    // generate a reply and callback.
    Log_debug("deptran callback, %llx", cmd_->id_);
    committed_ = true;
    GotoNextPhase();
  }
}

void RccCoord::DispatchRo() {
  std::lock_guard<std::recursive_mutex> lock(mtx_);
  // new txn id for every new and retry.
//  RequestHeader header = gen_header(ch);

  int pi;

  map<int32_t, Value> *input = nullptr;
  int32_t server_id;
  int     res;
  int     output_size;

  int cnt;
  while (cmd_->HasMoreSubCmdReadyNotOut()) {
    auto subcmd = (SimpleCommand*) cmd_->GetNextReadySubCmd();
    subcmd->id_ = next_pie_id();
    verify(subcmd->root_id_ == cmd_->id_);
    n_dispatch_++;
    cnt++;
    Log_debug("send out start request %ld, cmd_id: %lx, inn_id: %d, pie_id: %lx",
              n_dispatch_, cmd_->id_, subcmd->inn_id_, subcmd->id_);
    dispatch_acks_[subcmd->inn_id()] = false;
    commo()->SendHandoutRo(*subcmd,
                           std::bind(&RccCoord::DispatchRoAck,
                                     this,
                                     phase_,
                                     std::placeholders::_1,
                                     std::placeholders::_2,
                                     std::placeholders::_3));
  }
}

void RccCoord::DispatchRoAck(phase_t phase,
                             int res,
                             SimpleCommand &cmd,
                             map<int, mdb::version_t> &vers) {
  std::lock_guard<std::recursive_mutex> lock(this->mtx_);
  verify(phase == phase_);

  auto ch = (TxnCommand*) cmd_;
  cmd_->Merge(cmd);
  curr_vers_.insert(vers.begin(), vers.end());

  Log_debug("receive deptran RO start response, tid: %llx, pid: %llx, ",
             cmd_->id_,
             cmd.inn_id_);

  ch->n_pieces_out_++;
  if (cmd_->HasMoreSubCmdReadyNotOut()) {
    DispatchRo();
  } else if (ch->n_pieces_out_ == ch->GetNPieceAll()) {
    if (last_vers_ == curr_vers_) {
      // TODO
    } else {
      ch->read_only_reset();
      last_vers_ = curr_vers_;
      curr_vers_.clear();
      this->Dispatch();
    }
  }
}

//void RccCoord::deptran_finish_ro(TxnCommand *ch) {
//  int pi;
//
//  map<int32_t, Value> *input = nullptr;
//  int32_t server_id;
//  int     res;
//  int     output_size;
//
//  SimpleCommand *subcmd = nullptr;
//  while ((subcmd = (SimpleCommand*) cmd_->GetNextReadySubCmd()) != nullptr) {
//    header.pid = next_pie_id();
//
//    rrr::FutureAttr fuattr;
//
//    // remember this a asynchronous call! variable funtional range is important!
//    fuattr.callback = [ch, pi, this, header](Future * fu) {
//      bool callback = false;
//      {
//        std::lock_guard<std::recursive_mutex> lock(this->mtx_);
//
//        int res;
//        map<int32_t, Value> output;
//        fu->get_reply() >> output;
//
//        Log::debug(
//          "receive deptran RO start response (phase 2), tid: %llx, pid: %llx, ",
//          header.tid,
//          header.pid);
//
//        ch->n_pieces_out_++;
//        bool do_next_piece = ch->read_only_start_callback(pi, &res, output);
//
//        if (do_next_piece) deptran_finish_ro(ch);
//        else if (ch->n_pieces_out_ == ch->GetNPieceAll()) {
//          if (res == SUCCESS) {
//            ch->reply_.res_ = SUCCESS;
//            callback        = true;
//          }
//          else if (ch->can_retry()) {
//            ch->read_only_reset();
//            double last_latency = ch->last_attempt_latency();
//
//            if (ccsi_) ccsi_->txn_retry_one(this->thread_id_,
//                                            ch->type_,
//                                            last_latency);
//            this->deptran_finish_ro(ch);
//            callback = false;
//          }
//          else {
//            ch->reply_.res_ = REJECT;
//            callback        = true;
//          }
//        }
//      }
//
//      if (callback) {
//        // generate a reply and callback.
//        Log::debug("deptran RO callback, %llx", cmd_->id_);
//        TxnReply& txn_reply_buf = ch->get_reply();
//        double    last_latency  = ch->last_attempt_latency();
//        this->report(txn_reply_buf, last_latency
//#ifdef TXN_STAT
//                     , ch
//#endif // ifdef TXN_STAT
//                     );
//        ch->callback_(txn_reply_buf);
//        delete ch;
//      }
//    };
//
//    RococoProxy *proxy = (RococoProxy*)comm()->rpc_proxies_[server_id];
//    Log::debug("send deptran RO start request (phase 2), tid: %llx, pid: %llx",
//               cmd_->id_,
//               header.pid);
//    verify(input != nullptr);
//    Future::safe_release(proxy->async_rcc_ro_start_pie(*subcmd, fuattr));
//  }
//}

void RccCoord::do_one(TxnRequest& req) {
  // pre-process
  std::lock_guard<std::recursive_mutex> lock(this->mtx_);
  TxnCommand *txn = frame_->CreateChopper(req, txn_reg_);
  verify(txn_reg_ != nullptr);
  cmd_ = txn;
  cmd_->id_ = this->next_txn_id();
  cmd_->root_id_ = this->next_txn_id();
  Reset();
  Log_debug("do one request");

  if (ccsi_) ccsi_->txn_start_one(thread_id_, cmd_->type_);
  verify((phase_ % 3) == Phase::INIT_END);
  GotoNextPhase();
}

void RccCoord::Reset() {
  ClassicCoord::Reset();
  graph_.Clear();
  ro_state_ = BEGIN;
  last_vers_.clear();
  curr_vers_.clear();
}


void RccCoord::GotoNextPhase() {
  int n_phase = 3;
  int current_phase = phase_ % n_phase;
  switch (phase_++ % n_phase) {
    case Phase::INIT_END:
      PreDispatch();
      verify(phase_ % n_phase == Phase::DISPATCH);
      break;
    case Phase::DISPATCH:
      verify(phase_ % n_phase == Phase::COMMIT);
      Finish();
      break;
    case Phase::COMMIT:
      verify(phase_ % n_phase == Phase::INIT_END);
      End();
      break;
    default:
      verify(0);
  }
}

} // namespace rococo
