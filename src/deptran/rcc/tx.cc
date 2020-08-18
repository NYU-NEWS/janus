#include <deptran/txn_reg.h>
#include "../__dep__.h"
#include "../scheduler.h"
#include "tx.h"
#include "bench/tpcc/workload.h"
#include "dep_graph.h"
#include "procedure.h"

namespace janus {

thread_local uint64_t RccTx::timestamp_i_s = 0;
thread_local uint64_t RccTx::timestamp_d_s = 0;
SpinLock RccTx::__debug_parents_lock_{};
SpinLock RccTx::__debug_scc_lock_{};
std::map<txid_t, parent_set_t> RccTx::__debug_parents_i_{};
std::map<txid_t, parent_set_t> RccTx::__debug_parents_d_{};
std::map<txid_t, vector<txid_t>> RccTx::__debug_scc_i_{};
std::map<txid_t, vector<txid_t>> RccTx::__debug_scc_d_{};

void RccTx::__DebugCheckScc(rank_t rank) {
#ifdef DEBUG_CHECK
//  verify(rank == RANK_D);
  __debug_scc_lock_.lock();
  verify(!scchelper(rank).scc_->empty());
  auto& scc = __debug_scc(rank)[tid_];
  vector<txid_t> another_scc{};
  for (auto x : *scchelper(rank).scc_) {
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

  auto sz = scc.size();
  for (auto i : scc) {
    auto& y = __debug_scc(rank)[i];
    if (!y.empty()) {
      verify(scc.size() == sz);
      verify(y == scc);
    } else {
      y = scc;
    }
  }
  __debug_scc_lock_.unlock();
#endif
}

void RccTx::__DebugCheckParents(int rank) {
  verify(rank == RANK_D || rank == RANK_I);
#ifdef DEBUG_CHECK
  verify(subtx(rank).status() >= TXN_CMT);
  verify(subtx(rank).IsCommitting());
  __debug_parents_lock_.lock();
  auto it = __debug_parents(rank).find(tid_);
  if (it != __debug_parents(rank).end()) {
    auto& p1 = scchelper(rank).parents();
    auto& p2 = it->second;
    verify(p1 == p2);
  } else {
    __debug_parents(rank)[tid_] = scchelper(rank).parents();
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
  Tx(rhs_dtxn.epoch_, rhs_dtxn.tid_, nullptr)
//  partition_(rhs_dtxn.partition_),
//  status_(rhs_dtxn.status_)
  {
  verify(0);
//  if (status_.value_ >= TXN_CMT) {
//    __DebugCheckParents();
//  }
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

//  verify(phase_ != PHASE_RCC_COMMIT);
  auto rank = cmd.rank_;
  for (auto& c: subtx(rank).dreqs_) {
    if (c.inn_id() == cmd.inn_id()) // already handled?
      return;
  }
  if ((rank == RANK_I && SKIP_I) || (rank == RANK_D && SKIP_D)) {
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
      TraceDep(row, col_id, cmd.rank_);
    }
  }
  subtx(rank).dreqs_.push_back(cmd);

  // FIXME this is temprorary hack for tpcc execution for troad.
  phase_ = PHASE_RCC_DISPATCH;
  int ret_code;
  cmd.input.Aggregate(ws_);
  piece.proc_handler_(nullptr,
                      *this,
                      cmd,
                      &ret_code,
                      *output);
  ws_.insert(*output);
}


void RccTx::Abort() {
  aborted_ = true;
  // TODO. nullify changes in the staging area.
}

void RccTx::CommitValidate(rank_t rank) {
  verify(rank == RANK_D);
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
        subtx(rank).local_validated_->Set(REJECT);
        return;
      }
    }
  }
  subtx(rank).local_validated_->Set(SUCCESS);
}

void RccTx::CommitExecute(int rank) {
  phase_ = PHASE_RCC_COMMIT;
  committed_ = true;
  verify(rank == RANK_D || rank == RANK_I);
//  if (global_validated_->Get() == REJECT) {
//    return;
//  }
//  verify(!subtx(rank).dreqs_.empty());
  TxWorkspace ws;
  for (auto &cmd: subtx(rank).dreqs_) {
    verify (cmd.rank_ == rank);
    TxnPieceDef& p = txn_reg_.lock()->get(cmd.root_type_, cmd.type_);
    int tmp;
    cmd.input.Aggregate(ws);
    auto& m = subtx(rank).output_[cmd.inn_id_];
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
      case TXN_IMMEDIATE:
      case TXN_DEFERRED:
        if (mocking_janus_) {
          mdb_txn()->read_column(r, col_id, value);
          if (mocking_janus_) {
            verify(r->rtti() == symbol_t::ROW_VERSIONED);
            auto c = r->get_column(col_id);
            *value = c;
            if (mocking_janus_) {
              auto ver_id = r->get_column_ver(col_id);
              row->ref_copy();
              read_vers_[row][col_id] = ver_id;
            }
          }
        }
      case TXN_BYPASS:
        mdb_txn()->read_column(r, col_id, value);
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
      mdb_txn()->write_column(row, col_id, value);
    }
    if (hint_flag == TXN_IMMEDIATE || hint_flag == TXN_DEFERRED) {
//      TraceDep(row, col_id, hint_flag);
    }
  } else if (phase_ == PHASE_RCC_COMMIT) {
    if (hint_flag == TXN_BYPASS || hint_flag == TXN_DEFERRED || hint_flag == RANK_I) {
      auto v = value;
      if (hint_flag == TXN_BYPASS) hint_flag = TXN_DEFERRED;
      v.ver_ = timestamp(hint_flag)++;
      mdb_txn()->write_column(row, col_id, v);
      auto r = dynamic_cast<mdb::VersionedRow *>(row);
      r->set_column_ver(col_id, timestamp(hint_flag)++);
    } else {
      verify(0);
    }
  } else {
    verify(0);
  }
  return true;
}

void RccTx::AddParentEdge(shared_ptr<RccTx> other, rank_t rank) {
  Vertex::AddParentEdge(other, rank);
  if (sched_) {
    verify(other->epoch_ > 0);
    sched_->epoch_mgr_.IncrementRef(other->epoch_);
  }
}

void RccTx::TraceDep(Row* row, colid_t col_id, rank_t rank) {
  auto r = static_cast<RccRow*>(row);
  verify(r != nullptr);
  entry_t *entry = r->get_dep_entry(col_id);
  if (entry->rank_ == RANK_UNDEFINED) {
    entry->rank_ = rank;
  }
  verify(entry->rank_ == rank);
  if (entry->last_) {
    auto last_commit = static_pointer_cast<RccTx>(entry->last_);
    if (last_commit.get() != this) {
      this->AddParentEdge(last_commit, rank);
    }
  }
  entry->last_ = shared_from_this();
//  for (auto& x: entry->active_) {
//    auto parent_dtxn = static_pointer_cast<RccTx>(x);
//    if (parent_dtxn.get() != this) {
//      this->AddParentEdge(parent_dtxn, edge_type);
//    }
//  }
//  entry->active_.insert(shared_from_this());
}

void RccTx::CommitDep(Row& row, colid_t col_id) {
  auto& r = static_cast<RccRow&>(row);
  entry_t *entry = r.get_dep_entry(col_id);
  if (entry->last_) {
    auto last_commit = static_pointer_cast<RccTx>(entry->last_);
    if (last_commit.get() == this) {
//      entry->last_ = nullptr;
    }
  }
//  entry->last_ = shared_from_this();
//  entry->active_.erase(shared_from_this());
}

} // namespace janus
