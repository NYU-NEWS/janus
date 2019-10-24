#include <deptran/txn_reg.h>
#include "../__dep__.h"
#include "../scheduler.h"
#include "tx.h"
#include "bench/tpcc/workload.h"
#include "dep_graph.h"
#include "procedure.h"

namespace janus {

TxRococo::TxRococo(epoch_t epoch,
                 txnid_t tid,
                 Scheduler *mgr,
                 bool ro) : Tx(epoch, tid, mgr) {
  read_only_ = ro;
  mdb_txn_ = mgr->GetOrCreateMTxn(tid_);
  verify(id() == tid);
}

TxRococo::TxRococo(txnid_t id): Tx(0, id, nullptr) {
  // alert!! after this a lot of stuff need to be set manually.
  tid_ = id;
}

TxRococo::TxRococo(TxRococo& rhs_dtxn) :
    Vertex<TxRococo>(rhs_dtxn),
    Tx(rhs_dtxn.epoch_, rhs_dtxn.tid_, nullptr),
    partition_(rhs_dtxn.partition_),
    status_(rhs_dtxn.status_) {
}

void TxRococo::DispatchExecute(SimpleCommand &cmd,
                              map<int32_t, Value> *output) {
  // TODO rococo and janus should be different
  phase_ = PHASE_RCC_DISPATCH;
  for (auto& c: dreqs_) {
    if (c.inn_id() == cmd.inn_id()) // already handled?
      return;
  }
  verify(txn_reg_);
  TxnPieceDef& piece = txn_reg_->get(cmd.root_type_, cmd.type_);
  auto& conflicts = piece.conflicts_;
  for (auto& c: conflicts) {
    vector<Value> pkeys;
    for (int i = 0; i < c.primary_keys.size(); i++) {
      pkeys.push_back(cmd.input.at(c.primary_keys[i]));
    }
    auto row = Query(GetTable(c.table), pkeys, c.row_context_id);
    verify(row != nullptr);
    for (auto col_id : c.columns) {
      TraceDep(row, col_id, TXN_DEFERRED);
    }
  }
  dreqs_.push_back(cmd);

  // TODO are these preemptive actions proper?
  int ret_code;
  cmd.input.Aggregate(ws_);
  piece.proc_handler_(nullptr,
                      *this,
                      cmd,
                      &ret_code,
                      *output);
  ws_.insert(*output);
//  for (auto& c: dreqs_) {
//    if (c.inn_id() == cmd.inn_id())
//      return;
//  }
//  verify(phase_ <= PHASE_RCC_DISPATCH);
//  phase_ = PHASE_RCC_DISPATCH;
//  // execute the IR actions.
//  verify(txn_reg_);
//  auto pair = txn_reg_->get(cmd);
//  // To tolerate deprecated codes
//  int xxx, *yyy;
//  if (pair.defer == DF_REAL) {
//    yyy = &xxx;
//    dreqs_.push_back(cmd);
//  } else if (pair.defer == DF_NO) {
//    yyy = &xxx;
//  } else if (pair.defer == DF_FAKE) {
//    dreqs_.push_back(cmd);
//    return;
////    verify(0);
//  } else {
//    verify(0);
//  }
//  pair.txn_handler(nullptr,
//                   this,
//                   const_cast<SimpleCommand&>(cmd),
//                   yyy,
//                   *output);
//  *res = pair.defer;
}


void TxRococo::Abort() {
  aborted_ = true;
  // TODO. nullify changes in the staging area.
}

void TxRococo::CommitExecute() {
//  verify(phase_ == PHASE_RCC_START);
  phase_ = PHASE_RCC_COMMIT;
  TxWorkspace ws;
  for (auto &cmd: dreqs_) {
    if (cmd.rank_ != current_rank_) {
      continue;
    }
    TxnPieceDef& p = txn_reg_->get(cmd.root_type_, cmd.type_);
    int tmp;
    cmd.input.Aggregate(ws);
    auto& m = output_[cmd.inn_id_];
    p.proc_handler_(nullptr, *this, cmd, &tmp, m);
    ws.insert(m);
  }
  committed_ = true;
}

void TxRococo::ReplyFinishOk() {
//  verify(committed != aborted);
  sp_ev_commit_->Set(1);
//  int r = committed_ ? SUCCESS : REJECT;
//  if (commit_request_received_) {
//    if (__debug_replied)
//      return;
//    verify(!__debug_replied); // FIXME
//    __debug_replied = true;
//    verify(ptr_output_repy_);
//    finish_reply_callback_(r);
//  }
}

bool TxRococo::start_exe_itfr(defer_t defer_type,
                             ProcHandler &handler,
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

void TxRococo::start_ro(const SimpleCommand& cmd,
                       map<int32_t, Value> &output,
                       DeferredReply *defer) {
  verify(0);
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

void TxRococo::kiss(mdb::Row *r, int col, bool immediate) {
  verify(0);
}

bool TxRococo::ReadColumn(mdb::Row *row,
                         mdb::colid_t col_id,
                         Value *value,
                         int hint_flag) {
  verify(!read_only_);
  if (phase_ == PHASE_RCC_DISPATCH) {
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


bool TxRococo::WriteColumn(Row *row,
                          colid_t col_id,
                          const Value &value,
                          int hint_flag) {
  verify(!read_only_);
  if (phase_ == PHASE_RCC_DISPATCH) {
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

void TxRococo::AddParentEdge(shared_ptr<TxRococo> other, int8_t weight) {
  Vertex::AddParentEdge(other, weight);
  if (sched_) {
    verify(other->epoch_ > 0);
    sched_->epoch_mgr_.IncrementRef(other->epoch_);
  }
}

void TxRococo::TraceDep(Row* row, colid_t col_id, rank_t hint_flag) {
  // TODO remove pointers in outdated epoch ???
  auto r = dynamic_cast<RCCRow*>(row);
  verify(r != nullptr);
  entry_t *entry = r->get_dep_entry(col_id);
  int8_t edge_type = (hint_flag == RANK_I) ? EDGE_I : EDGE_D;
  auto parent_dtxn = dynamic_pointer_cast<TxRococo>(entry->last_);
  if (parent_dtxn == nullptr) {

  } else if (parent_dtxn.get() == this) {
    // skip
  } else {
    if (parent_dtxn != nullptr) {
      if (parent_dtxn->IsExecuted()) { ;
      } else {
        if (sched_->epoch_enabled_) {
          auto epoch1 = parent_dtxn->epoch_;
          auto epoch2 = sched_->epoch_mgr_.oldest_active_;
          // adding a parent from an inactive epoch is
          // dangerous.
          // verify(epoch1 >= epoch2);
        }
        this->AddParentEdge(parent_dtxn, edge_type);
      }
//      parent_dtxn->external_ref_ = nullptr;
    }
//    this->external_refs_.push_back((void**)(&(entry->last_)));
//    entry->last_ = this; // TODO FIXME
  }
#ifdef DEBUG_CODE
  verify(graph_);
  TxnInfo& tinfo = tv_->Get();
  auto s = tinfo.status();
  verify(s < TXN_CMT);
//  RccScc& scc = graph_->FindSCC(tv_);
//  if (scc.size() > 1 && graph_->HasICycle(scc)) {
//    verify(0);
//  }
#endif
}

} // namespace janus
