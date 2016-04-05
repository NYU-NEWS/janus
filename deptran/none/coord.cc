
#include "none/coord.h"
#include "frame.h"
#include "benchmark_control_rpc.h"

namespace rococo {


/** thread safe */
//void NoneCoord::do_one(TxnRequest &req) {
//  // pre-process
//  std::lock_guard<std::recursive_mutex> lock(this->mtx_);
//  TxnCommand *cmd = frame_->CreateTxnCommand(req, txn_reg_);
//  verify(txn_reg_ != nullptr);
//  cmd_ = cmd;
//  cmd_->root_id_ = this->next_txn_id();
//  cmd_->id_ = cmd_->root_id_;
//  Reset(); // In case of reuse.
//
//  Log_debug("do one request txn_id: %ld\n", cmd_->id_);
//
//  if (ccsi_) ccsi_->txn_start_one(thread_id_, cmd->type_);
//  Dispatch();
//}
//
//void NoneCoord::Dispatch() {
//  std::lock_guard<std::recursive_mutex> lock(mtx_);
//  phase_++;
//
//  int cnt = 0;
//  while (cmd_->HasMoreSubCmdReadyNotOut()) {
//    auto subcmd = (SimpleCommand*) cmd_->GetNextReadySubCmd();
//    subcmd->id_ = next_pie_id();
//    verify(subcmd->root_id_ == cmd_->id_);
//    n_handout_++;
//    cnt++;
//    Log_debug("send out start request %ld, cmd_id: %lx, inn_id: %d, pie_id: %lx",
//              n_handout_, cmd_->id_, subcmd->inn_id_, subcmd->id_);
//    handout_acks_[subcmd->inn_id()] = false;
//    commo()->SendDispatch(*subcmd,
//                         this,
//                         std::bind(&ClassicCoord::DispatchAck,
//                                   this,
//                                   phase_,
//                                   std::placeholders::_1,
//                                   std::placeholders::_2));
//  }
//  Log_debug("sent %d SubCmds\n", cnt);
//}
//
//
//
//void NoneCoord::DispatchAck(phase_t phase, int res, Command &cmd) {
//  std::lock_guard<std::recursive_mutex> lock(this->mtx_);
//  verify(phase == phase_);
//  n_handout_ack_++;
//  TxnCommand *ch = (TxnCommand *) cmd_;
//  handout_acks_[cmd.inn_id_] = true;
//
//  Log_debug("get start ack %ld/%ld for cmd_id: %lx, inn_id: %d",
//            n_handout_ack_, n_handout_, cmd_->id_, cmd.inn_id_);
//  verify(res == SUCCESS);
//  cmd_->Merge(cmd);
//  if (cmd_->HasMoreSubCmdReadyNotOut()) {
//    Log_debug("command has more sub-cmd, cmd_id: %lx,"
//                  " n_started_: %d, n_pieces: %d",
//                cmd_->id_, ch->n_pieces_out_, ch->GetNPieceAll());
//    Dispatch();
//  } else if (AllDispatchAcked()) {
//    Log_debug("receive all handout acks, txn_id: %ld; START PREPARE",
//              cmd_->id_);
//    verify(0);
//  }
//}

void NoneCoord::GotoNextPhase() {

  int n_phase = 2;
  int current_phase = phase_ % n_phase;
  switch (phase_++ % n_phase) {
    case Phase::INIT_END:
      Dispatch();
      verify(phase_ % n_phase == Phase::DISPATCH);
      break;
    case Phase::DISPATCH:
      committed_ = true;
      verify(phase_ % n_phase == Phase::INIT_END);
      End();
      break;
    default:
      verify(0);
  }
}

//
///** thread safe */
//void NoneCoord::do_one(TxnRequest &req) {
//  // pre-process
//  std::lock_guard<std::mutex> lock(this->mtx_);
//  TxnChopper *ch = Frame().CreateChopper(req);
//  cmd_id_ = this->next_txn_id();
//
//  Log::debug("do one request");
//
//  if (ccsi_) ccsi_->txn_start_one(thread_id_, ch->txn_type_);
//
////  LegacyStart(ch);
//  Start();
//}
//
//void NoneCoord::LegacyStartAck(TxnChopper *ch,
//                               int pi,
//                               Future *fu) {
//  bool callback = false;
//  {
//    std::lock_guard<std::mutex> lock(this->mtx_);
//
//    int res;
//    std::vector<mdb::Value> output;
//    fu->get_reply() >> res >> output;
//    ch->n_pieces_out_++;
//
////        LegacyCommandOutput output;
////        fu-get_reply() >> output;
////        ch->merge(output);
//
//
//    if (!ch->commit_.load()) {
//      if (n_start_ == n_start_ack_) {
//        this->Finish();
//      }
//    } else {
//      if (ch->start_callback(pi, res, output)) {
////        this->LegacyStart(ch);
//        Start();
//      } else if (ch->n_pieces_out_ == ch->n_pieces_all_) {
//        callback = true;
//        ch->reply_.res_ = SUCCESS;
//      }
//    }
//  }
//
//  if (callback) {
//    TxnReply &txn_reply_buf = ch->get_reply();
//    double last_latency = ch->last_attempt_latency();
//    this->report(txn_reply_buf, last_latency
//#ifdef TXN_STAT
//        , ch
//#endif // ifdef TXN_STAT
//    );
//    ch->callback_(txn_reply_buf);
//    delete ch;
//  }
//}


} // namespace rococo
