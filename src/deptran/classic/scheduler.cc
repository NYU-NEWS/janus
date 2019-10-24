#include "../constants.h"
#include "../tx.h"
#include "../procedure.h"
#include "../coordinator.h"
#include "../2pl/tx.h"
#include "scheduler.h"
#include "tpc_command.h"
#include "tx.h"

namespace janus {

void SchedulerClassic::MergeCommands(vector<shared_ptr<TxPieceData>>& ops,
                                     shared_ptr<Marshallable> cmd2) {

  verify(0);
//  auto& sp_v2 = dynamic_pointer_cast<VecPieceData>(cmd2)->sp_vec_piece_data_;
//  verify(sp_v2);
//  for (auto& cmd: *sp_v2) {
//    verify(std::all_of(sp_v1->begin(), sp_v1->end(), [&cmd] (TxPieceData& d) {
//      return (cmd.root_id_ == d.root_id_) && (cmd.inn_id_ != d.inn_id_);
//    }));
//    sp_v1->push_back(cmd);
//  }
}

bool SchedulerClassic::ExecutePiece(Tx& tx,
                                    TxPieceData& piece_data,
                                    TxnOutput& ret_output) {
  auto roottype = piece_data.root_type_;
  auto subtype = piece_data.type_;
  TxnPieceDef& piece_def = txn_reg_->get(roottype, subtype);
  int ret_code;
  auto& conflicts = piece_def.conflicts_;
  piece_data.input.Aggregate(tx.ws_);
// TODO enable this verify
//  piece_data.input.VerifyReady();
  piece_def.proc_handler_(nullptr,
                          tx,
                          piece_data,
                          &ret_code,
                          ret_output[piece_data.inn_id()]);
  tx.ws_.insert(ret_output[piece_data.inn_id()]);
  return true;
}

bool SchedulerClassic::ExecuteAll(Tx &tx, TxnOutput &ret_output) {
  // wait for an execution signal.
//  Log_debug("entered waiting for tx id: %" PRIx64, tx.tid_);
//  tx.ev_execute_ready_.Wait();
//  Log_debug("finished waiting for tx id: %" PRIx64, tx.tid_);
  verify(0);
//  auto sp_vec_piece =
//      dynamic_pointer_cast<VecPieceData>(tx.cmd_)->sp_vec_piece_data_;
//  for (auto& piece_data : *sp_vec_piece) {
//    ExecutePiece(tx, piece_data, ret_output);
//  }
  tx.inuse = false;
  return true;
}

bool SchedulerClassic::DispatchPiece(Tx& tx,
                                     TxPieceData& piece_data,
                                     TxnOutput& ret_output) {
  TxnPieceDef
      & piece_def = txn_reg_->get(piece_data.root_type_, piece_data.type_);
  auto& conflicts = piece_def.conflicts_;
  auto id = piece_data.inn_id();
  // Two phase locking won't pass these
//  verify(!tx.inuse);
//  tx.inuse = true;

  for (auto& c: conflicts) {
    vector<Value> pkeys;
    for (int i = 0; i < c.primary_keys.size(); i++) {
      pkeys.push_back(piece_data.input.at(c.primary_keys[i]));
    }
    auto row = tx.Query(tx.GetTable(c.table), pkeys, c.row_context_id);
    verify(row != nullptr);
    for (auto col_id : c.columns) {
      if (!Guard(tx, row, col_id)) {
        tx.inuse = false;
//        auto reactor = Reactor::GetReactor();
//        auto sz = reactor->coros_.size();
//        verify(sz > 0);
        auto id = piece_data.inn_id();
        ret_output[id] = {}; // the client uses this to identify ack.
        return false; // abort
      }
    }
  }
//  tx.inuse = false;
  return true;
}

bool SchedulerClassic::Dispatch(cmdid_t cmd_id,
                                shared_ptr<Marshallable> cmd,
                                TxnOutput& ret_output) {
  auto sp_vec_piece =
      dynamic_pointer_cast<VecPieceData>(cmd)->sp_vec_piece_data_;
  verify(sp_vec_piece);
  auto tx = GetOrCreateTx(cmd_id);
//  MergeCommands(tx.cmd_, cmd);
  Log_debug("received dispatch for tx id: %" PRIx64, tx->tid_);
//  verify(partition_id_ == piece_data.partition_id_);
  // pre-proces
  // TODO separate pre-process and process/commit
  // TODO support user-customized pre-process.

// for debug purpose
//  bool b1 = false, b2 = false;
//  for (auto& piece_data : *sp_vec_piece) {
//    if (piece_data.inn_id_ == 200) b1 = true;
//    if (piece_data.inn_id_ == 205) b2 = true;
//  }
//  verify(b1 == b2);
  if (!tx->cmd_) {
    tx->cmd_ = cmd;
  } else {
    auto present_cmd =
        dynamic_pointer_cast<VecPieceData>(tx->cmd_)->sp_vec_piece_data_;
    for (auto& sp_piece_data : *sp_vec_piece) {
      present_cmd->push_back(sp_piece_data);
    }
  }

  // TODO investigate: change it to a reference with clang will cause crash
  for (auto sp_piece_data : *sp_vec_piece) {
    DispatchPiece(*tx, *sp_piece_data, ret_output);
  }
  // TODO reimplement this.
  if (tx->fully_dispatched_->value_ == 0) {
    tx->fully_dispatched_->Set(1);
  }
  return true;
}

// On prepare with replication
//   1. dispatch the whole transaction to others.
//   2. use a paxos command to commit the prepare request.
//   3. after that, run the function to prepare.
//   0. an non-optimized version would be.
//      dispatch the transaction command with paxos instance
bool SchedulerClassic::OnPrepare(cmdid_t tx_id,
                                 const std::vector<i32>& sids) {
  auto sp_tx = dynamic_pointer_cast<TxClassic>(GetOrCreateTx(tx_id));
  verify(sp_tx);
  Log_debug("%s: at site %d, tx: %"
                PRIx64, __FUNCTION__, this->site_id_, tx_id);
  std::lock_guard<std::recursive_mutex> lock(mtx_);
//  auto exec = dynamic_cast<ClassicExecutor*>(GetExecutor(cmd_id));
//  exec->prepare_reply_ = [res, callback] (int r) {*res = r; callback();};

  if (Config::GetConfig()->IsReplicated()) {
    auto sp_prepare_cmd = std::make_shared<TpcPrepareCommand>();
    verify(sp_prepare_cmd->kind_ == MarshallDeputy::CMD_TPC_PREPARE);
    sp_prepare_cmd->tx_id_ = tx_id;
    sp_prepare_cmd->cmd_ = sp_tx->cmd_;
    auto sp_m = dynamic_pointer_cast<Marshallable>(sp_prepare_cmd);
    CreateRepCoord()->Submit(sp_m);
//    Log_debug("wait for prepare command replicated");
    sp_tx->is_leader_hint_ = true;
    sp_tx->ev_prepare_.Wait();
//    Log_debug("finished prepare command replication");
    return sp_tx->result_prepare_;
  } else if (Config::GetConfig()->do_logging()) {
    string log;
    this->get_prepare_log(tx_id, sids, &log);
    //   recorder_->submit(log, callback);
  } else {
    return DoPrepare(tx_id);
  }
  return false;
}

int SchedulerClassic::PrepareReplicated(TpcPrepareCommand& prepare_cmd) {
  std::lock_guard<std::recursive_mutex> lock(mtx_);
  // TODO verify it is the same leader, error if not.
  // TODO and return the prepare callback here.
  auto tx_id = prepare_cmd.tx_id_;
  auto sp_tx = dynamic_pointer_cast<TxClassic>(GetOrCreateTx(tx_id));
  if (!sp_tx->is_leader_hint_) {
    // TODO follower site right now does not really do work.
    return 0;
  }
  if (!sp_tx->cmd_)
    sp_tx->cmd_ = prepare_cmd.cmd_;
  // else: is the leader.
  sp_tx->result_prepare_ = DoPrepare(sp_tx->tid_);
  Log_debug("prepare request replicated and executed for %" PRIx64 ", result: %x, sid: %x",
      sp_tx->tid_, sp_tx->result_prepare_, (int)this->site_id_);
  sp_tx->ev_prepare_.Set(1);
  Log_debug("triggering prepare replication callback %" PRIx64, sp_tx->tid_);
  return 0;
}

int SchedulerClassic::OnCommit(txnid_t tx_id, int commit_or_abort) {
  std::lock_guard<std::recursive_mutex> lock(mtx_);
  Log_debug("%s: at site %d, tx: %" PRIx64,
            __FUNCTION__, this->site_id_, tx_id);
  auto sp_tx = dynamic_pointer_cast<TxClassic>(GetOrCreateTx(tx_id));
  // TODO maybe change inuse to an event?
//  verify(!sp_tx->inuse);
//  sp_tx->inuse = true;
  if (Config::GetConfig()->IsReplicated()) {
    auto cmd = std::make_shared<TpcCommitCommand>();
    cmd->tx_id_ = tx_id;
    cmd->ret_ = commit_or_abort;
    auto sp_m = dynamic_pointer_cast<Marshallable>(cmd);
    CreateRepCoord()->Submit(sp_m);
    sp_tx->ev_commit_.Wait();
  } else {
//    verify(exec->phase_ < 3);
//    exec->phase_ = 3;
    if (commit_or_abort == SUCCESS) {
#ifdef CHECK_ISO
      //      MergeDeltas(exec->dtxn_->deltas_);
#endif
//      exec->CommitLaunch(res, callback);
      DoCommit(*sp_tx);
    } else if (commit_or_abort == REJECT) {
//      exec->AbortLaunch(res, callback);
      DoAbort(*sp_tx);
    } else {
      verify(0);
    }
//    TrashExecutor(cmd_id);
  }
//  sp_tx->inuse = false;
  return 0;
}

void SchedulerClassic::DoCommit(Tx& tx_box) {
  auto mdb_txn = RemoveMTxn(tx_box.tid_);
  verify(mdb_txn == tx_box.mdb_txn_);
  mdb_txn->commit();
  delete mdb_txn; // TODO remove this
}

void SchedulerClassic::DoAbort(Tx& tx_box) {
  auto mdb_txn = RemoveMTxn(tx_box.tid_);
  verify(mdb_txn == tx_box.mdb_txn_);
  mdb_txn->abort();
  delete mdb_txn; // TODO remove this
}

int SchedulerClassic::CommitReplicated(TpcCommitCommand& tpc_commit_cmd) {
  std::lock_guard<std::recursive_mutex> lock(mtx_);
  auto tx_id = tpc_commit_cmd.tx_id_;
  auto sp_tx = dynamic_pointer_cast<TxClassic>(GetOrCreateTx(tx_id));
  if (!sp_tx->is_leader_hint_) {
    // TODO follower site right now does not really do work.
    return 0;
  }
  int commit_or_abort = tpc_commit_cmd.ret_;
  if (commit_or_abort == SUCCESS) {
#ifdef CHECK_ISO
    MergeDeltas(exec->dtxn_->deltas_);
#endif
    sp_tx->committed_ = true;
    DoCommit(*sp_tx);
  } else if (commit_or_abort == REJECT) {
    sp_tx->aborted_ = false;
    DoAbort(*sp_tx);
  } else {
    verify(0);
  }
  sp_tx->ev_commit_.Set(1);
  sp_tx->ev_execute_ready_->Set(1);
//  verify(app_next_);
//  app_next_(*(sp_tx->cmd_));
//  TrashExecutor(tx_id);
  return 0;
}

void SchedulerClassic::Next(Marshallable& cmd) {
  if (cmd.kind_ == MarshallDeputy::CMD_TPC_PREPARE) {
    auto& c = dynamic_cast<TpcPrepareCommand&>(cmd);
    PrepareReplicated(c);
  } else if (cmd.kind_ == MarshallDeputy::CMD_TPC_COMMIT) {
    auto& c = dynamic_cast<TpcCommitCommand&>(cmd);
    CommitReplicated(c);
  } else {
    verify(0);
  }
}

} // namespace janus
