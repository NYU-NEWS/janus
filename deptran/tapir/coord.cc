#include "commo.h"
#include "coord.h"
#include "frame.h"

namespace rococo {

TapirCommo* TapirCoord::commo() {
//  verify(commo_ != nullptr);
  if (commo_ == nullptr) {
    commo_ = new TapirCommo();
  }
  return (TapirCommo*)commo_;
}

void TapirCoord::do_one(TxnRequest& req) {
  std::lock_guard<std::mutex> lock(this->mtx_);
  verify(txn_reg_);
  TxnCommand *cmd = frame_->CreateTxnCommand(req, txn_reg_);
  verify(txn_reg_ != nullptr);
  cmd_ = cmd;
  cmd_->root_id_ = this->next_txn_id();
  cmd_->id_ = cmd_->root_id_;
  Reset(); // In case of reuse.

  Log::debug("do one request txn_id: %ld\n", cmd_->id_);

  if (ccsi_) ccsi_->txn_start_one(thread_id_, cmd->type_);

  FastAccept();
}

void TapirCoord::FastAccept() {
  std::lock_guard<std::mutex> lock(start_mtx_);
  phase_++;
  while (cmd_->HasMoreSubCmdReadyNotOut()) {
    auto subcmd = (SimpleCommand*)cmd_->GetNextSubCmd();
    subcmd->id_ = next_pie_id();
    verify(subcmd->root_id_ == cmd_->id_);
    n_start_++;
    Log_debug("send out start request %ld, cmd_id: %lx, inn_id: %d, pie_id: %lx",
              n_start_, cmd_->id_, subcmd->inn_id_, subcmd->id_);
    start_ack_map_[subcmd->inn_id()] = false;
    commo()->BroadcastFastAccept(*subcmd,
                                 this,
                                 std::bind(&TapirCoord::FastAcceptAck,
                                           this,
                                           phase_,
                                           subcmd->GetParId(),
                                           std::placeholders::_1));
  }
}

void TapirCoord::FastAcceptAck(phase_t phase, parid_t par_id, Future *fu) {
  std::lock_guard<std::mutex> lock(this->mtx_);
  if (phase_ > phase) return;
  // if for every partition, get OK, go into decide.
  // 
  verify(0);
}

void TapirCoord::Decide() {
  verify(0);
}

} // namespace rococo