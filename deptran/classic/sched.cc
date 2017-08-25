
#include "../constants.h"
#include "../dtxn.h"
#include "../procedure.h"
#include "../coordinator.h"
#include "sched.h"
#include "exec.h"
#include "tpc_command.h"
#include "../2pl/tx_box.h"

namespace janus {

bool SchedulerClassic::OnDispatch(TxnPieceData& piece_data,
                              TxnOutput& ret_output) {

  TxBox& tx_box = *GetOrCreateDTxn(piece_data.root_id_);
  verify(partition_id_ == piece_data.partition_id_);
  verify(!tx_box.inuse);
  tx_box.inuse = true;
  TxnPieceDef& piece_def = txn_reg_->get(piece_data.root_type_,
                                         piece_data.type_);
  auto& conflicts = piece_def.conflicts_;

  for (auto& c: conflicts) {
    vector<Value> pkeys;
    for (int i = 0; i < c.primary_keys.size(); i++) {
      pkeys.push_back(piece_data.input.at(c.primary_keys[i]));
    }
    auto row = tx_box.Query(tx_box.GetTable(c.table), pkeys, c.row_context_id);
    verify(row != nullptr);
    for (auto col_id : c.columns) {
      if (!BeforeAccess(tx_box, row, col_id)) {
        tx_box.inuse = false;
        ret_output[piece_data.inn_id()] = {}; // the client use this to identify ack.
        return false; // abort
      }
    }
  }
  // wait for an execution signal.
  int ret_code;
  piece_data.input.Aggregate(tx_box.ws_);
  piece_def.proc_handler_(nullptr,
                          &tx_box,
                          const_cast<TxnPieceData&>(piece_data),
                          &ret_code,
                          ret_output[piece_data.inn_id()]);
  tx_box.ws_.insert(ret_output[piece_data.inn_id()]);
  tx_box.inuse = false;
  return true;
}

int SchedulerClassic::OnDispatchOld(const vector<SimpleCommand>& cmd,
                                int32_t *res,
                                TxnOutput* output,
                                const function<void()>& callback) {
  std::lock_guard<std::recursive_mutex> lock(mtx_);
  verify(frame_);
  auto exec = (ClassicExecutor*) GetOrCreateExecutor(cmd[0].root_id_);
  vector<int32_t> *rr = new vector<int32_t>(cmd.size());

  // XXX just touch in case
  for (auto c: cmd) {
    (*output)[c.inn_id()];
  }

  // TODO
  verify(cmd.size() > 0);
  int* sz = new int(cmd.size());
  int* i = new int(0);
  function<void()>* func = new function<void()>();
  TxnWorkspace* ws = new TxnWorkspace();
  *res = SUCCESS;
  *func = [sz, i, func, &cmd, rr, exec, output, ws, res, callback, this] () {
    std::lock_guard<std::recursive_mutex> lock(mtx_);
    verify(*sz == cmd.size());

    if (*res == REJECT) {
      callback();
      return;
    }

    if (*i > 0) { // not first time called.
      int j = (*i) - 1;
      verify(j >= 0);
      verify(j < cmd.size());
      auto& m = (*output)[cmd[j].inn_id()];
      if (cmd[j].inn_id() == 1000) {
        verify(m.count(1011) > 0);
      }
      ws->insert(m);
    }
    if (*i == *sz) {
      callback();
      return;
    } else if (*i < *sz) {
      int j = *i;
      (*i)++;
      const_cast<SimpleCommand&>(cmd[j]).input.Aggregate(*ws);
      exec->cmds_.push_back(cmd[j]);
      exec->OnDispatch(cmd[j],
                       res,
                       &(*output)[cmd[j].inn_id()],
                       *func);
    }

  };

  (*func)();

  return 0;
}

// On prepare with replication
//   1. dispatch the whole transaction to others.
//   2. use a paxos command to commit the prepare request.
//   3. after that, run the function to prepare.
//   0. an non-optimized version would be.
//      dispatch the transaction command with paxos instance
bool SchedulerClassic::OnPrepare(txnid_t tx_id,
                             const std::vector<i32> &sids) {
  Log_debug("%s: at site %d, tx: %" PRIx64, __FUNCTION__, this->site_id_, tx_id);
  std::lock_guard<std::recursive_mutex> lock(mtx_);
//  auto exec = dynamic_cast<ClassicExecutor*>(GetExecutor(cmd_id));
//  exec->prepare_reply_ = [res, callback] (int r) {*res = r; callback();};

  if (Config::GetConfig()->IsReplicated()) {
//    TpcPrepareCommand *cmd = new TpcPrepareCommand; // TODO watch out memory
//    cmd->txn_id_ = tx_id;
//    cmd->cmds_ = exec->cmds_;
//    CreateRepCoord()->Submit(*cmd);
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
  // TODO, disable for now
  verify(0);
  std::lock_guard<std::recursive_mutex> lock(mtx_);
  // TODO verify it is the same leader, error if not.
  // TODO and return the prepare callback here.
  auto tid = prepare_cmd.txn_id_;
  auto exec = dynamic_cast<ClassicExecutor*>(GetOrCreateExecutor(tid));
  verify(prepare_cmd.cmds_.size() > 0);
  if (exec->cmds_.size() == 0)
    exec->cmds_ = prepare_cmd.cmds_;
  // else: is the leader.
  verify(exec->cmds_.size() > 0);
  int r = exec->Prepare() ? SUCCESS : REJECT;
  exec->prepare_reply_(r);
  return 0;
}

int SchedulerClassic::OnCommit(txnid_t tx_id,
                           int commit_or_abort) {
  std::lock_guard<std::recursive_mutex> lock(mtx_);
  Log_debug("%s: at site %d, tx: %" PRIx64, __FUNCTION__, this->site_id_, tx_id);
  TxBox* tx_box = GetOrCreateDTxn(tx_id);
  verify(!tx_box->inuse);
  tx_box->inuse = true;
//  auto exec = (ClassicExecutor*)GetExecutor(cmd_id);
  if (Config::GetConfig()->IsReplicated()) {
//    exec->commit_reply_ = [callback] (int r) {callback();};
//    TpcCommitCommand* cmd = new TpcCommitCommand;
//    cmd->txn_id_ = cmd_id;
//    cmd->res_ = commit_or_abort;
//    CreateRepCoord()->Submit(*cmd);
  } else {
//    verify(exec->phase_ < 3);
//    exec->phase_ = 3;
    if (commit_or_abort == SUCCESS) {
#ifdef CHECK_ISO
//      MergeDeltas(exec->dtxn_->deltas_);
#endif
//      exec->CommitLaunch(res, callback);
      DoCommit(*tx_box);
    } else if (commit_or_abort == REJECT) {
//      exec->AbortLaunch(res, callback);
      DoAbort(*tx_box);
    } else {
      verify(0);
    }
//    TrashExecutor(cmd_id);
  }
  tx_box->inuse = false;
  return 0;
}

void SchedulerClassic::DoCommit(TxBox& tx_box) {
  auto mdb_txn = RemoveMTxn(tx_box.tid_);
  verify(mdb_txn == tx_box.mdb_txn_);
  mdb_txn->commit();
  delete mdb_txn; // TODO remove this
}

void SchedulerClassic::DoAbort(TxBox& tx_box) {
  auto mdb_txn = RemoveMTxn(tx_box.tid_);
  verify(mdb_txn == tx_box.mdb_txn_);
  mdb_txn->abort();
  delete mdb_txn; // TODO remove this
}

int SchedulerClassic::CommitReplicated(TpcCommitCommand& tpc_commit_cmd) {
  // TODO disable for now.
  verify(0);
  std::lock_guard<std::recursive_mutex> lock(mtx_);
  auto txn_id = tpc_commit_cmd.txn_id_;
  auto exec = (ClassicExecutor*) GetOrCreateExecutor(txn_id);
  verify(exec->phase_ < 3);
  exec->phase_ = 3;
  int commit_or_abort = tpc_commit_cmd.res_;
  if (commit_or_abort == SUCCESS) {
#ifdef CHECK_ISO
    MergeDeltas(exec->dtxn_->deltas_);
#endif
    exec->Commit();
  } else if (commit_or_abort == REJECT) {
    exec->Abort();
  } else {
    verify(0);
  }
  exec->commit_reply_(SUCCESS);
  TrashExecutor(txn_id);
}

void SchedulerClassic::OnLearn(ContainerCommand& cmd) {
  if (cmd.type_ == MarshallDeputy::CMD_TPC_PREPARE) {
    TpcPrepareCommand& c = dynamic_cast<TpcPrepareCommand&>(cmd);
    PrepareReplicated(c);
  } else if (cmd.type_ == MarshallDeputy::CMD_TPC_COMMIT) {
    TpcCommitCommand& c = dynamic_cast<TpcCommitCommand&>(cmd);
    CommitReplicated(c);
  } else {
    verify(0);
  }
}

} // namespace janus