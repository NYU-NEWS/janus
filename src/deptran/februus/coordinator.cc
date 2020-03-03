#include "../__dep__.h"
#include "coordinator.h"
#include "commo.h"

namespace janus {

CommoFebruus* CoordinatorFebruus::commo() {
  return (CommoFebruus*) commo_;
}

bool CoordinatorFebruus::PreAccept() {
  map<parid_t, unique_ptr<QuorumEvent>> map_up_quorum_event;

  for (auto par_id : tx_data().GetPartitionIds()) {
    auto cmds = tx_data().GetCmdsByPartition(par_id);
    auto n_replica = Config::GetConfig()->GetPartitionSize(par_id);
    auto n_quorum = n_replica;
    map_up_quorum_event[par_id].reset(new QuorumEvent(n_replica, n_quorum));
    commo()->BroadcastPreAccept(*map_up_quorum_event[par_id],
                                par_id, tx_data().id_);
  }
  for (auto& pair: map_up_quorum_event) {
    pair.second->Wait();
  }
  Log_debug("handle pre-accept ack tx id: %" PRIx64, tx_data().id_);
  fast_path_ = true;
  uint64_t max_timestamp = 0;
  for (auto& pair: map_up_quorum_event) {
    auto& quorum_event = *pair.second;
    auto& vec_timestamp = quorum_event.vec_timestamp_;
    bool fast = std::all_of(vec_timestamp.begin(),
                            vec_timestamp.end(),
                            [&vec_timestamp](uint64_t x) -> bool {
                              return x == vec_timestamp[0];
                            });
    auto m = std::max_element(vec_timestamp.begin(), vec_timestamp.end());
    max_timestamp = max_timestamp > *m ? max_timestamp : *m;
    if (!fast) {
      fast_path_ = false;
    }
    tx_data().timestamp_ = max_timestamp;
  }
  // TODO deal with timeout.
  // TODO deal with recovery conflict.
  return fast_path_;
}

bool CoordinatorFebruus::Accept() {
  map<parid_t, unique_ptr<QuorumEvent>> map_up_quorum_event;
  ballot_t ballot = 1; // TODO
  for (auto par_id : tx_data().GetPartitionIds()) {
    auto cmds = tx_data().GetCmdsByPartition(par_id);
    commo()->BroadcastAccept(*map_up_quorum_event[par_id],
                             par_id,
                             tx_data().id_,
                             ballot,
                             tx_data().timestamp_);
  }
  for (auto& pair: map_up_quorum_event) {
    pair.second->Wait();
  }
  Log_debug("handle accept ack tx id: %" PRIx64, tx_data().id_);
  fast_path_ = true;
  for (auto& pair: map_up_quorum_event) {
    auto& quorum_event = *pair.second;
    if (quorum_event.Yes()) {
      continue;
    } else {
      verify(0); // TODO conflict from recovery.
    }
  }
  // TODO deal with timeout.
  return true;
}

void CoordinatorFebruus::Prepare() {
  // TODO failure recovery needs this.
}

void CoordinatorFebruus::Commit() {
  commo()->BroadcastCommit(tx_data().GetPartitionIds(),
                           tx_data().id_,
                           tx_data().timestamp_);
}

void CoordinatorFebruus::DispatchAsync() {
  std::lock_guard<std::recursive_mutex> lock(mtx_);
  auto txn = (TxData*) cmd_;

  int cnt = 0;
  auto cmds_by_par = txn->GetReadyPiecesData(-1);
  Log_debug("dispatch for tx id: %" PRIx64, txn->root_id_);
  for (auto& pair: cmds_by_par) {
    const parid_t& par_id = pair.first;
    auto& vec_sp_piece_data = pair.second;
    n_dispatch_ += vec_sp_piece_data.size();
    cnt += vec_sp_piece_data.size();
    // TODO optimize?
    auto sp_vec_piece = std::make_shared<vector<shared_ptr<SimpleCommand>>>();
    for (auto c: vec_sp_piece_data) {
      c->id_ = next_pie_id();
      dispatch_acks_[c->inn_id_] = false;
      sp_vec_piece->push_back(c);
    }
    commo()->BroadcastDispatch(sp_vec_piece,
                               this,
                               std::bind(&CoordinatorFebruus::DispatchAck,
                                         this,
                                         phase_,
                                         std::placeholders::_1,
                                         std::placeholders::_2));
  }
  Log_debug("Dispatch cnt: %d for tx_id: %"
                PRIx64, cnt, txn->root_id_);
}


//bool CoordinatorFebruus::AllDispatchAcked() {
//  bool ret1 = std::all_of(dispatch_acks_.begin(),
//                          dispatch_acks_.end(),
//                          [](std::pair<innid_t, bool> pair) {
//                            return pair.second;
//                          });
//  if (ret1)
//    verify(n_dispatch_ack_ == n_dispatch_);
//  return ret1;
//}

void CoordinatorFebruus::DispatchAck(phase_t phase,
                                     int ret,
                                     TxnOutput& outputs) {
  std::lock_guard<std::recursive_mutex> lock(this->mtx_);
  if (phase != phase_) {
    // handle outdated message.
    return;
  }
  TxData* tx_data = (TxData*) cmd_;
  if (ret == REJECT) {
    Log_debug("got REJECT reply for cmd_id: %llx NOT COMMITING", tx_data->root_id_);
    aborted_ = true;
    tx_data->commit_.store(false);
    verify(0);
  }
  verify(ret == SUCCESS);
  n_dispatch_ack_ += outputs.size();
  if (aborted_) {
    if (n_dispatch_ack_ == n_dispatch_) {
      // wait until all ongoing dispatch to finish before aborting.
      Log_debug("received all start acks (at least one is REJECT); tx_id: %"
                    PRIx64, tx_data->root_id_);
      verify(0); // TODO handle aborts.
      return;
    }
  }

  for (auto& pair : outputs) {
    const innid_t& inn_id = pair.first;
    verify(dispatch_acks_.at(inn_id) == false);
    dispatch_acks_[inn_id] = true;
    Log_debug("get start ack %ld/%ld for cmd_id: %lx, inn_id: %d",
              n_dispatch_ack_, n_dispatch_, cmd_->id_, inn_id);
    tx_data->Merge(pair.first, pair.second);
  }
  if (tx_data->HasMoreUnsentPiece()) {
    Log_debug("command has more sub-cmd, cmd_id: %llx,"
                  " n_started_: %d, n_pieces: %d",
              tx_data->id_, tx_data->n_pieces_dispatched_, tx_data->GetNPieceAll());
    verify(0); // TODO
  } else if (AllDispatchAcked()) {
    Log_debug("receive all dispatch acks for tx id: %" PRIx64, tx_data->id_);
    // transaction finished successfully, callback.
    committed_ = true;
    End();
  }

}

void CoordinatorFebruus::GotoNextPhase() {
  DispatchAsync();
  if (PreAccept()) {
    Commit();
  } else {
    if (Accept()) {
      Commit();
    } else if (aborted_) {
      // TODO
    } else {
      // TODO
    }
  }
}

} // namespace janus
