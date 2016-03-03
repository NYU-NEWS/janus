//
// Created by lamont on 1/11/16.
//
#include <deptran/dtxn.h>
#include <deptran/txn_reg.h>
#include "deptran/txn_chopper.h"
#include "deptran/frame.h"
#include "executor.h"
#include "memdb/row.h"

namespace mdcc {
  using rococo::TxnRequest;
  using rococo::Frame;
  using rococo::SimpleCommand;

  MdccExecutor::MdccExecutor(txnid_t txn_id, Scheduler* sched) :
      Executor(txn_id, sched), sched_(static_cast<MdccScheduler*>(sched)) {
    Log_debug("mdcc executor created for txn_id %ld\n", txn_id);
  }

  void MdccExecutor::StartPiece(const rococo::SimpleCommand &cmd, int *result, DeferredReply *defer) {
    Log_info("%s type: %d; piece_id: %d;", __FUNCTION__, cmd.type_, cmd.inn_id_);
    auto handler_pair = this->txn_reg_->get(cmd);
    auto& c = const_cast<SimpleCommand&>(cmd);
    handler_pair.txn_handler(this, dtxn_, c, result, c.output);
    sched_->SendUpdateProposal(cmd_id_, cmd, result, defer);
  }

  bool MdccExecutor::ValidRead(OptionSet &option) {
    auto table = this->dtxn_->GetTable(option.Table());
    MultiBlob mb(option.Key().size());
    for (int i=0; i<mb.count(); i++) {
      mb[i] = option.Key()[i].get_blob();
    }
    auto row = static_cast<mdb::VersionedRow*>(this->dtxn_->Query(table, mb));
    for (auto& update : option.Options()) {
      if (update.version != row->get_column_ver(update.col_id))
        return false;
    }
    return true;
  }
}
