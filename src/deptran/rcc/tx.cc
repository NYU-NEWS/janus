#include <deptran/txn_reg.h>
#include "../__dep__.h"
#include "../scheduler.h"
#include "tx.h"
#include "bench/tpcc/workload.h"
#include "dep_graph.h"
#include "procedure.h"

namespace janus {

thread_local uint64_t RccTx::timestamp_ = 0;
SpinLock RccTx::__debug_parents_lock_{};
SpinLock RccTx::__debug_scc_lock_{};
std::map<txid_t, parent_set_t> RccTx::__debug_parents_{};
std::map<txid_t, vector<txid_t>> RccTx::__debug_scc_{};

void RccTx::__DebugCheckScc() {
#ifdef DEBUG_CHECK
  __debug_scc_lock_.lock();
  verify(!scc_->empty());
  auto& scc = __debug_scc_[tid_];
  vector<txid_t> another_scc{};
  for (auto x : *scc_) {
    another_scc.push_back(x->id());
  }
  if (scc.empty()) {
    scc = another_scc;
  } else {
    if (scc != another_scc) {
      auto x1 = scc.size();
      auto x2 = another_scc.size();
      verify(0);
    }
  }

  for (auto i : scc) {
    auto& y = __debug_scc_[i];
    if (!y.empty()) {
      verify(y == scc);
    } else {
      y = scc;
    }
  }
  __debug_scc_lock_.unlock();
#endif
}

void RccTx::__DebugCheckParents() {
#ifdef DEBUG_CHECK
  verify(status() >= TXN_CMT);
  verify(IsCommitting());
  __debug_parents_lock_.lock();
  auto it = __debug_parents_.find(tid_);
  if (it != __debug_parents_.end()) {
    auto& p1 = parents_;
    auto& p2 = it->second;
    verify(p1 == p2);
  } else {
    __debug_parents_[tid_] = parents_;
  }
  __debug_parents_lock_.unlock();
#endif
}

RccTx::RccTx(epoch_t epoch,
             txnid_t tid,
             TxLogServer *mgr,
             bool ro) : Tx(epoch, tid, mgr) {
  read_only_ = ro;
  mdb_txn_ = mgr->GetOrCreateMTxn(tid_);
  verify(id() == tid);
}

RccTx::RccTx(txnid_t id): Tx(0, id, nullptr) {
  // alert!! after this a lot of stuff need to be set manually.
  tid_ = id;
}

RccTx::RccTx(RccTx& rhs_dtxn) :
  Vertex<RccTx>(rhs_dtxn),
  Tx(rhs_dtxn.epoch_, rhs_dtxn.tid_, nullptr),
  partition_(rhs_dtxn.partition_),
  status_(rhs_dtxn.status_) {
  verify(0);
  if (status_.value_ >= TXN_CMT) {
  __DebugCheckParents();
  }
}

/**
 * Divide DF_I to two kinds:
 *  DF_II: required optimistic execution in either (multi-hop)
 *  DF_I: only required optimistic execution in Occ (1-hop)
 * Two cases:
 * 1. Occ
 * record versions in optimistic execution-if any (Dispatch: DF_I or DF_II)
 * commit (validation if needed) (Pre-accpet, Accept, Commit)
 * feedback needed (PostCommit)
 * 2. I-D
 * record versions in optimistic execution-if any (Dispatch: DF_II)
 * commit (validation if needed) (Pre-accpet, Accept, Commit)
 * feedback needed (PostCommit)
 * Pre-accept, Accept, Commit for DF_D
 * @param cmd
 * @param output
 */
void RccTx::DispatchExecute(SimpleCommand &cmd,
                            map<int32_t, Value> *output) {
  // TODO rococo and janus should be different
  verify(phase_ != PHASE_RCC_COMMIT);
  phase_ = PHASE_RCC_DISPATCH;
  for (auto& c: dreqs_) {
    if (c.inn_id() == cmd.inn_id()) // already handled?
      return;
  }
  TxnPieceDef& piece = txn_reg_.lock()->get(cmd.root_type_, cmd.type_);
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


void RccTx::Abort() {
  aborted_ = true;
  // TODO. nullify changes in the staging area.
}

void RccTx::CommitValidate() {
  for (auto& pair1 : read_vers_) {
    Row* r = pair1.first;
    if (r->rtti() != symbol_t::ROW_VERSIONED) {
      verify(0);
    };
    auto row = dynamic_cast<mdb::VersionedRow*>(r);
    verify(row != nullptr);
    for (auto& pair2: pair1.second) {
      auto col_id = pair2.first;
      auto ver_read = pair2.second;
      auto ver_now = row->get_column_ver(col_id);
      verify(col_id < row->prepared_rver_.size());
      verify(col_id < row->prepared_wver_.size());
      if (ver_read < ver_now) {
        local_validated_->Set(REJECT);
        return;
      }
    }
  }
  local_validated_->Set(SUCCESS);
}

void RccTx::CommitExecute() {
  phase_ = PHASE_RCC_COMMIT;
  committed_ = true;
//  if (global_validated_->Get() == REJECT) {
//    return;
//  }
  verify(!dreqs_.empty());
  TxWorkspace ws;
  for (auto &cmd: dreqs_) {
    verify (cmd.rank_ == RANK_I || cmd.rank_ == RANK_D);
    if (! __mocking_janus_) {
//      if (cmd.rank_ != current_rank_) {
//        continue;
//      }
    }
    TxnPieceDef& p = txn_reg_.lock()->get(cmd.root_type_, cmd.type_);
    int tmp;
    cmd.input.Aggregate(ws);
    auto& m = output_[cmd.inn_id_];
    p.proc_handler_(nullptr, *this, cmd, &tmp, m);
    ws.insert(m);


    auto& conflicts = p.conflicts_;
    for (auto& c: conflicts) {
      vector<Value> pkeys;
      for (int i = 0; i < c.primary_keys.size(); i++) {
        pkeys.push_back(cmd.input.at(c.primary_keys[i]));
      }
      auto row = Query(GetTable(c.table), pkeys, c.row_context_id);
      verify(row != nullptr);
      for (auto col_id : c.columns) {
        CommitDep(*row, col_id);
      }
    }
  }
}

void RccTx::start_ro(const SimpleCommand& cmd,
                     map<int32_t, Value> &output,
                     DeferredReply *defer) {
  verify(0);
}

bool RccTx::ReadColumn(mdb::Row *row,
                       mdb::colid_t col_id,
                       Value *value,
                       int hint_flag) {
  verify(!read_only_);
  auto r = dynamic_cast<mdb::VersionedRow *>(row);
  if (phase_ == PHASE_RCC_DISPATCH) {
    switch (hint_flag) {
      case TXN_BYPASS:
        // TODO starting test for troad?
        mdb_txn()->read_column(r, col_id, value);
        break;
      case TXN_IMMEDIATE: {
        verify(r->rtti() == symbol_t::ROW_VERSIONED);
        auto c = r->get_column(col_id);
        *value = c;
        if (__mocking_janus_) {
          auto ver_id = r->get_column_ver(col_id);
          row->ref_copy();
          read_vers_[row][col_id] = ver_id;
          need_validation_ = true;
        }
        // continue to trace dep
      }
      case TXN_DEFERRED:
        TraceDep(row, col_id, hint_flag);
        break;
      default:
        verify(0);
        break;
    }
  } else if (phase_ == PHASE_RCC_COMMIT) {
    if (hint_flag == TXN_BYPASS || hint_flag == TXN_DEFERRED
    || hint_flag == TXN_IMMEDIATE) {
      mdb_txn()->read_column(row, col_id, value);
    } else {
      verify(0);
    }
  } else {
    verify(0);
  }
  return true;
}


bool RccTx::WriteColumn(Row *row,
                        colid_t col_id,
                        const Value &value,
                        int hint_flag) {
  verify(!read_only_);
  if (phase_ == PHASE_RCC_DISPATCH) {
    if (hint_flag == TXN_BYPASS) {
      mdb_txn()->write_column(row, col_id, value);
    }
    if (hint_flag == TXN_IMMEDIATE) {
      // TODO, something is wrong here.
      mdb_txn()->write_column(row, col_id, value);
      verify(0);
    }
    if (hint_flag == TXN_IMMEDIATE || hint_flag == TXN_DEFERRED) {
      TraceDep(row, col_id, hint_flag);
    }
  } else if (phase_ == PHASE_RCC_COMMIT) {
    if (hint_flag == TXN_BYPASS || hint_flag == TXN_DEFERRED) {
      auto v = value;
      v.ver_ = timestamp_++;
      mdb_txn()->write_column(row, col_id, v);
      auto r = dynamic_cast<mdb::VersionedRow *>(row);
      r->set_column_ver(col_id, timestamp_++);
    } else {
      verify(0);
    }
  } else {
    verify(0);
  }
  return true;
}

void RccTx::AddParentEdge(shared_ptr<RccTx> other, int8_t weight) {
  Vertex::AddParentEdge(other, weight);
  if (sched_) {
    verify(other->epoch_ > 0);
    sched_->epoch_mgr_.IncrementRef(other->epoch_);
  }
}

void RccTx::TraceDep(Row* row, colid_t col_id, rank_t hint_flag) {
  auto r = dynamic_cast<RccRow*>(row);
  verify(r != nullptr);
  entry_t *entry = r->get_dep_entry(col_id);
//  int8_t edge_type = (hint_flag == RANK_I) ? EDGE_I : EDGE_D;
  int8_t edge_type = EDGE_D;
//  auto parent_dtxn = dynamic_pointer_cast<RccTx>(entry->last_);
  if (entry->last_) {
    auto last_commit = static_pointer_cast<RccTx>(entry->last_);
    if (last_commit.get() != this) {
      this->AddParentEdge(last_commit, EDGE_D);
    }
  }
  for (auto& x: entry->active_) {
    auto parent_dtxn = static_pointer_cast<RccTx>(x);
    if (parent_dtxn.get() != this) {
      this->AddParentEdge(parent_dtxn, edge_type);
    }
  }
  entry->active_.insert(shared_from_this());
}

void RccTx::CommitDep(Row& row, colid_t col_id) {
  auto& r = dynamic_cast<RccRow&>(row);
  entry_t *entry = r.get_dep_entry(col_id);
  entry->last_ = shared_from_this();
  entry->active_.erase(shared_from_this());

}

} // namespace janus
