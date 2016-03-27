#include <deptran/txn_reg.h>
#include "../__dep__.h"
#include "../scheduler.h"
#include "dtxn.h"
#include "bench/tpcc/piece.h"

namespace rococo {

RccDTxn::RccDTxn(txnid_t tid,
                 Scheduler *mgr,
                 bool ro) : DTxn(tid, mgr) {
  tv_ = nullptr;
  read_only_ = ro;
  mdb_txn_ = mgr->GetOrCreateMTxn(tid_);
}

void RccDTxn::DispatchExecute(const SimpleCommand &cmd,
                              int32_t *res,
                              map<int32_t, Value> *output) {
  verify(phase_ <= PHASE_RCC_START);
  phase_ = PHASE_RCC_START;
  // execute the IR actions.
  auto pair = txn_reg_->get(cmd);
  // To tolerate deprecated codes
  int xxx, *yyy;
  if (pair.defer == DF_REAL) {
    yyy = &xxx;
    dreqs_.push_back(cmd);
  } else if (pair.defer == DF_NO) {
    yyy = &xxx;
  } else if (pair.defer == DF_FAKE) {
    verify(0);
  } else {
    verify(0);
  }
  pair.txn_handler(nullptr,
                   this,
                   const_cast<SimpleCommand&>(cmd),
                   yyy,
                   *output);
  *res = pair.defer;
}

void RccDTxn::CommitExecute() {
  verify(phase_ == PHASE_RCC_START);
  phase_ = PHASE_RCC_COMMIT;
  for (auto &cmd: dreqs_) {
    auto pair = txn_reg_->get(cmd);
    int tmp;
    pair.txn_handler(nullptr, this, cmd, &tmp, (*outputs_)[cmd.inn_id_]);
  }
}

void RccDTxn::ReplyFinishOk() {
  finish_ok_callback_();
}

bool RccDTxn::start_exe_itfr(defer_t defer_type,
                             TxnHandler &handler,
                             const SimpleCommand& cmd,
                             map<int32_t, Value> *output) {
  verify(0);
//  bool deferred;
//  switch (defer_type) {
//    case DF_NO: { // immediate
//      int res;
//      // TODO fix
//      handler(nullptr,
//              this,
//              const_cast<SimpleCommand&>(cmd),
//              &res,
//              *output);
//      deferred = false;
//      break;
//    }
//    case DF_REAL: { // defer
//      dreqs_.push_back(cmd);
//      map<int32_t, Value> no_use;
//      handler(nullptr,
//              this,
//              const_cast<SimpleCommand&>(cmd),
//              NULL,
//              no_use);
//      deferred = true;
//      break;
//    }
//    case DF_FAKE: //TODO
//    {
//      dreqs_.push_back(cmd);
//      int output_size = 300; //XXX
//      int res;
//      handler(nullptr,
//              this,
//              const_cast<SimpleCommand&>(cmd),
//              &res,
//              *output);
//      deferred = false;
//      break;
//    }
//    default:
//      verify(0);
//  }
//  return deferred;
}

//void RccDTxn::start(
//    const RequestHeader &header,
//    const std::vector<mdb::Value> &input,
//    bool *deferred,
//    ChopStartResponse *res
//) {
//  // TODO Remove
//  verify(0);
//}

void RccDTxn::start_ro(const SimpleCommand& cmd,
                       map<int32_t, Value> &output,
                       DeferredReply *defer) {

  conflict_txns_.clear();
  auto txn_handler_pair = txn_reg_->get(cmd.root_type_, cmd.type_);
  int res;
  phase_ = 1;

  int output_size;
  txn_handler_pair.txn_handler(nullptr,
                               this,
                               const_cast<SimpleCommand&>(cmd),
                               &res,
                               output);

  // get conflicting transactions
  std::vector<TxnInfo *> &conflict_txns = conflict_txns_;
  // TODO callback: read the value and return.
  std::function<void(void)> cb = [defer]() {
    defer->reply();
  };
  // wait for them become commit.

  DragonBall *ball = new DragonBall(conflict_txns.size() + 1, cb);

  for (auto tinfo: conflict_txns) {
    tinfo->register_event(TXN_DCD, ball);
  }
  ball->trigger();
}

//void RccDTxn::commit(const ChopFinishRequest &req,
//                     ChopFinishResponse *res,
//                     rrr::DeferredReply *defer) {
//  verify(0);
//}

//void RccDTxn::commit_anc_finish(
//    Vertex<TxnInfo> *v,
//    rrr::DeferredReply *defer
//) {
//  std::function<void(void)> scc_anc_commit_cb = [v, defer, this]() {
//    this->commit_scc_anc_commit(v, defer);
//  };
//  Log::debug("all ancestors have finished for txn id: %llx", v->data_->id());
//  // after all ancestors become COMMITTING
//  // wait for all ancestors of scc become DECIDED
//  std::set<Vertex<TxnInfo> *> scc_anc;
//  RccDTxn::dep_s->find_txn_scc_nearest_anc(v, scc_anc);
//
//  DragonBall *wait_commit_ball = new DragonBall(scc_anc.size() + 1,
//                                                scc_anc_commit_cb);
//  for (auto &sav: scc_anc) {
//    //Log::debug("\t ancestor id: %llx", sav->data_.id());
//    // what if the ancestor is not related and not committed or not finished?
//    sav->data_->register_event(TXN_DCD, wait_commit_ball);
//    send_ask_req(sav);
//  }
//  wait_commit_ball->trigger();
//}

//void RccDTxn::commit_scc_anc_commit(
//    Vertex<TxnInfo> *v,
//    rrr::DeferredReply *defer
//) {
//  Graph<TxnInfo> &txn_gra = RccDTxn::dep_s->txn_gra_;
//  uint64_t txn_id = v->data_->id();
//  Log::debug("all scc ancestors have committed for txn id: %llx", txn_id);
//  // after all scc ancestors become DECIDED
//  // sort, and commit.
//  TxnInfo &tinfo = *v->data_;
//  if (tinfo.is_commit()) { ;
//  } else {
//    std::vector<Vertex<TxnInfo> *> sscc;
//    txn_gra.FindSortedSCC(v, &sscc);
//    //static int64_t sample = 0;
//    //if (RandomGenerator::rand(1, 100)==1) {
//    //    scsi_->do_statistics(S_RES_KEY_N_SCC, sscc.size());
//    //}
//    //this->stat_sz_scc_.sample(sscc.size());
//    for (auto &vv: sscc) {
//      bool commit_by_other = vv->data_->get_status() & TXN_DCD;
//      if (!commit_by_other) {
//        // apply changes.
//
//        // this res may not be mine !!!!
//        if (vv->data_->res != nullptr) {
//          auto txn = (RccDTxn *) sched_->GetDTxn(vv->data_->id());
//          txn->exe_deferred(vv->data_->res->outputs);
//          sched_->DestroyDTxn(vv->data_->id());
//        }
//
//        Log::debug("txn commit. tid:%llx", vv->data_->id());
//        // delay return back to clients.
//        //
//        verify(vv->data_->committed_ == false);
//        vv->data_->committed_ = true;
//        vv->data_->union_status(TXN_DCD, false);
//      }
//    }
//    for (auto &vv: sscc) {
//      vv->data_->trigger();
//    }
//  }
//
//  if (defer != nullptr) {
//    //if (commit_by_other) {
//    //    Log::debug("reply finish request of txn: %llx, it's committed by other txn", vv->data_.id());
//    //} else {
//    Log::debug("reply finish request of txn: %llx", txn_id);
//    //}
//    defer->reply();
//  }
//}
//
//
//void RccDTxn::exe_deferred(vector<std::pair<RequestHeader,
//                                            map<int32_t, Value> > > &outputs) {
//
//  verify(phase_ == 1);
//  phase_ = 2;
//  if (dreqs_.size() == 0) {
//    // this tid does not need deferred execution.
//    //verify(0);
//  } else {
//    // delayed execution
//    outputs.clear(); // FIXME does this help? seems it does, why?
//    for (auto &cmd: dreqs_) {
//      auto txn_handler_pair = txn_reg_->get(cmd.root_type_, cmd.type_);
////      verify(header.tid == tid_);
//
//      map<int32_t, Value> output;
//      int output_size = 0;
//      int res;
//      txn_handler_pair.txn_handler(nullptr,
//                                   this,
//                                   cmd,
//                                   &res,
//                                   output);
//      if (cmd.type_ == TPCC_PAYMENT_4 && cmd.root_type_ == TPCC_PAYMENT)
//        verify(output_size == 15);
//
//      // FIXME. what the fuck happens here?
//      // XXX FIXME
//      verify(0);
////      RequestHeader header;
////      auto pp = std::make_pair(header, output);
////      outputs.push_back(pp);
//    }
//  }
//}
//

void RccDTxn::kiss(mdb::Row *r, int col, bool immediate) {
  verify(0);
  entry_t *entry = ((RCCRow *) r)->get_dep_entry(col);
  int8_t edge_type = immediate ? EDGE_I : EDGE_D;

  if (read_only_) {
    if (entry->last_)
      conflict_txns_.push_back(entry->last_->data_.get());
  } else {
    if (entry->last_ != NULL) {
      entry->last_->outgoing_[tv_] |= edge_type;
      tv_->incoming_[entry->last_] |= edge_type;
    } else {
      entry->last_ = tv_;
    }
  }
}

bool RccDTxn::ReadColumn(mdb::Row *row,
                         mdb::column_id_t col_id,
                         Value *value,
                         int hint_flag) {
  verify(!read_only_);
  if (phase_ == PHASE_RCC_START) {
    int8_t edge_type;
    if (hint_flag == TXN_BYPASS || hint_flag == TXN_INSTANT) {
      mdb_txn()->read_column(row, col_id, value);
    }

    if (hint_flag == TXN_INSTANT || hint_flag == TXN_DEFERRED) {
//      entry_t *entry = ((RCCRow *) row)->get_dep_entry(col_id);
      TraceDep(row, col_id, hint_flag);
    }
  } else if (phase_ == PHASE_RCC_COMMIT) {
    if (hint_flag == TXN_BYPASS || hint_flag == TXN_DEFERRED) {
      mdb_txn()->read_column(row, col_id, value);
    } else {
      verify(0);
    }
  } else {
    verify(0);
  }
  return true;
}

bool RccDTxn::WriteColumn(Row *row,
                          column_id_t col_id,
                          const Value &value,
                          int hint_flag) {
  verify(!read_only_);
  if (phase_ == PHASE_RCC_START) {
    int8_t edge_type;
    if (hint_flag == TXN_BYPASS || hint_flag == TXN_INSTANT) {
      mdb_txn()->write_column(row, col_id, value);
    }
    if (hint_flag == TXN_INSTANT || hint_flag == TXN_DEFERRED) {
//      entry_t *entry = ((RCCRow *) row)->get_dep_entry(col_id);
      TraceDep(row, col_id, hint_flag);
    }
  } else if (phase_ == PHASE_RCC_COMMIT) {
    if (hint_flag == TXN_BYPASS || hint_flag == TXN_DEFERRED) {
      mdb_txn()->write_column(row, col_id, value);
    } else {
      verify(0);
    }
  } else {
    verify(0);
  }
  return true;
}

void RccDTxn::TraceDep(Row* row, column_id_t col_id, int hint_flag) {
  auto r = dynamic_cast<RCCRow*>(row);
  verify(r != nullptr);
  entry_t *entry = r->get_dep_entry(col_id);
  int8_t edge_type = (hint_flag == TXN_INSTANT) ? EDGE_I : EDGE_D;
  // TODO optimize.
  RccVertex*& parent_v = entry->last_;
  if (parent_v == tv_) {
    // skip
  } else if (parent_v != nullptr) {
    tv_->AddParentEdge(parent_v, edge_type);
  } else if (parent_v == nullptr) {
    parent_v = tv_;
  } else {
    verify(0);
  }

}

} // namespace rococo
