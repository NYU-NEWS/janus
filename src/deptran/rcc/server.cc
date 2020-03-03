
#include "../procedure.h"
#include "graph.h"
#include "tx.h"
#include "server.h"
#include "commo.h"
#include "frame.h"

namespace janus {

map<txnid_t, int32_t> RccServer::__debug_xxx_s{};
std::recursive_mutex RccServer::__debug_mutex_s{};
void RccServer::__DebugCheckParentSetSize(txnid_t tid, int32_t sz) {
#ifdef DEBUG_CODE
  std::lock_guard<std::recursive_mutex> guard(__debug_mutex_s);
  if (__debug_xxx_s.count(tid) > 0) {
    auto s = __debug_xxx_s[tid];
    verify(s == sz);
  } else {
    __debug_xxx_s[tid] = sz;
  }
#endif
}

RccServer::RccServer() : TxLogServer(), mtx_() {
  RccGraph::sched_ = this;
  RccGraph::partition_id_ = TxLogServer::partition_id_;
  RccGraph::managing_memory_ = false;
  epoch_enabled_ = true;
}

RccServer::~RccServer() {
}

shared_ptr<Tx> RccServer::GetOrCreateTx(txnid_t tid, bool ro) {
  auto dtxn = dynamic_pointer_cast<RccTx>(
      TxLogServer::GetOrCreateTx(tid, ro));
  dtxn->partition_.insert(TxLogServer::partition_id_);
  verify(dtxn->id() == tid);
  return dtxn;
}

int RccServer::OnDispatch(const vector<SimpleCommand>& cmd,
                          TxnOutput* output,
                          shared_ptr<RccGraph> graph) {
  std::lock_guard<std::recursive_mutex> guard(mtx_);
  verify(graph.get());
  txnid_t txn_id = cmd[0].root_id_;
  auto dtxn = dynamic_pointer_cast<RccTx>(GetOrCreateTx(txn_id));
  verify(dtxn->id() == txn_id);
  verify(RccGraph::partition_id_ == TxLogServer::partition_id_);
  verify(cmd[0].partition_id_ == TxLogServer::partition_id_);
  for (auto& c : cmd) {
    dtxn->DispatchExecute(const_cast<SimpleCommand&>(c),
                          &(*output)[c.inn_id()]);
  }
  dtxn->UpdateStatus(TXN_STD);
  int depth = 1;
  verify(cmd[0].root_id_ == txn_id);
  auto sz = MinItfrGraph(*dtxn, graph, true, depth);
//#ifdef DEBUG_CODE
//    if (sz > 4) {
//      Log_fatal("something is wrong, graph size %d", sz);
//    }
//#endif
#ifdef DEBUG_CODE
  if (sz > 0) {
    RccDTxn& info1 = dep_graph_->vertex_index_.at(cmd[0].root_id_)->Get();
    RccDTxn& info2 = graph->vertex_index_.at(cmd[0].root_id_)->Get();
    verify(info1.partition_.find(cmd[0].partition_id_)
               != info1.partition_.end());
    verify(info2.partition_.find(cmd[0].partition_id_)
               != info2.partition_.end());
    verify(sz > 0);
    if (RandomGenerator::rand(1, 2000) <= 1)
      Log_info("dispatch ret graph size: %d", graph->size());
  }
#endif
//  };
//
//  static bool do_record = Config::GetConfig()->do_logging();
//  if (do_record) {
//    Marshal m;
//    m << cmd;
//    recorder_->submit(m, job);
//  } else {
//    job();
//  }
  return dtxn->need_validation_;
}

int RccServer::OnCommit(cmdid_t cmd_id,
                        rank_t rank,
                        const RccGraph& graph,
                        TxnOutput* output,
                        const function<void()>& callback) {
  verify(0);
  return 0;
}

int RccServer::OnInquire(epoch_t epoch,
                         txnid_t txn_id,
                         shared_ptr<RccGraph> graph) {
  std::lock_guard<std::recursive_mutex> guard(mtx_);

  verify(0);
}

void RccServer::InquiredGraph(RccTx& dtxn, shared_ptr<RccGraph> graph) {
  verify(graph != nullptr);
  if (dtxn.IsDecided()) {
    // return scc is enough.
    auto& scc = FindSccPred(dtxn);
    for (auto v : scc) {
      auto vv = graph->CreateV(*v);
      verify(vv->parents_.size() == v->parents_.size());
      verify(vv->partition_.size() == v->partition_.size());
    }
  } else {
    MinItfrGraph(dtxn, graph, false, 1);
  }
}

void RccServer::__DebugExamineFridge() {
#ifdef DEBUG_CODE
  std::lock_guard<std::recursive_mutex> lock(mtx_);
  int in_ask = 0;
  int in_wait_self_cmt = 0;
  int in_wait_anc_exec = 0;
  int in_wait_anc_cmt = 0;
  int64_t id = 0;
  for (RccVertex* v : fridge_) {
    RccDTxn& tinfo = v->Get();
    if (tinfo.status() >= TXN_CMT) {
      verify(tinfo.graphs_for_inquire_.size() == 0);
    }
    if (tinfo.status() >= TXN_CMT) {
      if (AllAncCmt(v)) {
//        verify(!tinfo.IsExecuted());
//        verify(tinfo.IsDecided() || tinfo.IsAborted());
        if (!tinfo.IsExecuted()) {
          in_wait_anc_exec++;
        } else {
          // why is this still in fridge?
          verify(0);
        }
      } else {
//        RccScc& scc = dep_graph_->FindSCC(v);
//        verify(!AllAncFns(scc));
        in_wait_anc_cmt++;
      }
    }
//    Log_info("my partition: %d", (int) partition_id_);
    if (tinfo.status() < TXN_CMT) {
      if (tinfo.Involve(partition_id_)) {
        in_wait_self_cmt++;
        id = tinfo.id();
      } else {
        verify(tinfo.during_asking);
        in_ask++;
      }
    }
  }
  int sz = (int)fridge_.size();
  Log_info("examining fridge. fridge size: %d, in_ask: %d, in_wait_self_cmt: %d"
               " in_wait_anc_exec: %d, in wait anc cmt: %d, else %d",
           sz, in_ask, in_wait_self_cmt, in_wait_anc_exec, in_wait_anc_cmt,
           sz - in_ask - in_wait_anc_exec - in_wait_anc_cmt - in_wait_self_cmt);
//  Log_info("wait for myself commit: %llx par: %d", id, (int)partition_id_);
#endif
}

void RccServer::InquireAboutIfNeeded(RccTx& dtxn) {
//  Graph<RccDTxn> &txn_gra = dep_graph_->txn_gra_;
  if (
//      dtxn.status() <= TXN_STD &&
//      !dtxn.during_asking &&
      !dtxn.Involve(TxLogServer::partition_id_)) {
    verify(!dtxn.Involve(TxLogServer::partition_id_));
    verify(!dtxn.during_asking);
    parid_t par_id = *(dtxn.partition_.begin());
    dtxn.during_asking = true;
    auto sp_graph = commo()->Inquire(par_id, dtxn.epoch_, dtxn.tid_);
    Aggregate(epoch_mgr_.curr_epoch_, *sp_graph);
    verify(dtxn.status() >= TXN_CMT);
  }
}

void RccServer::InquireAck(cmdid_t cmd_id, RccGraph& graph) {
  std::lock_guard<std::recursive_mutex> lock(mtx_);
  auto v = FindV(cmd_id);
  verify(v != nullptr);
  RccTx& tinfo = *v;
  tinfo.inquire_acked_ = true;
  Aggregate(epoch_mgr_.curr_epoch_, const_cast<RccGraph&>(graph));
//  TriggerCheckAfterAggregation(const_cast<RccGraph&>(graph));
  verify(tinfo.status() >= TXN_CMT);
}

RccTx& RccServer::__DebugFindAnOngoingAncestor(RccTx& vertex) {
  RccTx* ret = nullptr;
  set<RccTx*> walked;
  std::function<bool(RccTx&)> func = [&ret, &vertex](RccTx& v) -> bool {
    RccTx& info = v;
    if (info.status() >= TXN_CMT) {
      return true;
    } else {
      ret = &v;
      return false; // stop traversing.
    }
  };
  TraversePred(vertex, -1, func, walked);
  return *ret;
}


void RccServer::WaitUntilAllPredecessorsAtLeastCommitting(RccTx* vertex) {
  std::function<int(RccTx&)> func =
      [this](RccTx& v) -> int {
        RccTx& parent = v;
        int r = RccGraph::SearchHint::Ok;
        if (parent.HasLogApplyStarted() || parent.IsAborted()) {
          r = RccGraph::SearchHint::Skip;
        } else if (parent.status() < TXN_CMT) {
          bool b = parent.Involve(TxLogServer::partition_id_);
          if (!b) {
            InquireAboutIfNeeded(parent);
            verify(parent.status() >= TXN_CMT);
          }
          parent.status_.Wait([](int val)->bool {
            return (val>=TXN_CMT);
          });
        } else if (parent.status() >= TXN_DCD) {
          return RccGraph::SearchHint::Skip;
        }
        return r;
      };
  TraversePredNonRecursive(*vertex, func);
}

bool RccServer::AllAncCmt(RccTx* vertex) {
  bool all_anc_cmt = true;
  std::function<int(RccTx&)> func =
      [&all_anc_cmt, &vertex](RccTx& v) -> int {
        RccTx& parent = v;
        int r = 0;
        if (parent.HasLogApplyStarted() || parent.IsAborted()) {
          r = RccGraph::SearchHint::Skip;
        } else if (parent.status() >= TXN_CMT) {
          r = RccGraph::SearchHint::Ok;
        } else {
          r = RccGraph::SearchHint::Exit;
          parent.to_checks_.insert(vertex);
          all_anc_cmt = false;
        }
        return r;
      };
  TraversePred(*vertex, -1, func);
  return all_anc_cmt;
}

void RccServer::Decide(const RccScc& scc) {
  for (auto v : scc) {
    UpgradeStatus(*v, TXN_DCD);
//    Log_info("txnid: %llx, parent size: %d", v->id(), v->parents_.size());
  }
}

bool RccServer::HasICycle(const RccScc& scc) {
  for (auto& vertex : scc) {
    set<RccTx*> walked;
    bool ret = false;
    std::function<bool(RccTx*)> func =
        [&ret, vertex](RccTx* v) -> bool {
          if (v == vertex) {
            ret = true;
            return false;
          }
          return true;
        };
    TraverseDescendant(*vertex, -1, func, walked, EDGE_I);
    if (ret) return true;
  }
  return false;
};

bool RccServer::HasAbortedAncestor(const RccScc& scc) {
  verify(scc.size() > 0);
  bool has_aborted = false;
  std::function<int(RccTx&)> func =
      [&has_aborted](RccTx& v) -> int {
        RccTx& info = v;
        if (info.HasLogApplyStarted()) {
          return RccGraph::SearchHint::Skip;
        }
        if (info.IsAborted()) {
          has_aborted = true; // found aborted transaction.
          return RccGraph::SearchHint::Exit; // abort traverse
        }
        return RccGraph::SearchHint::Ok;
      };
  TraversePred(*scc[0], -1, func);
  return has_aborted;
};

bool RccServer::FullyDispatched(const RccScc& scc, rank_t rank) {
  bool ret = std::all_of(scc.begin(),
                         scc.end(),
                         [this, rank](RccTx* v) {
                           RccTx& tinfo = *v;
                           if (tinfo.Involve(TxLogServer::partition_id_)) {
//                             if (tinfo.current_rank_ > rank) {
//                               return true;
//                             } else if (tinfo.current_rank_ < rank) {
//                               return false;
//                             } else {
                               return tinfo.fully_dispatched_->value_ == 1;
//                             }
                           } else {
                             return true;
                           }
                         });
  return ret;
}

bool RccServer::IsExecuted(const RccScc& scc, rank_t rank) {
  bool ret = std::any_of(scc.begin(),
                         scc.end(),
                         [this, rank](RccTx* v) {
                           RccTx& tinfo = *v;
                           if (tinfo.Involve(TxLogServer::partition_id_)) {
//                             if (tinfo.current_rank_ > rank) {
//                               return true;
//                             } else if (tinfo.current_rank_ < rank) {
//                               return false;
//                             } else {
                               return tinfo.log_apply_started_;
//                             }
                           } else {
                             return false;
                           }
                         });
  return ret;
}

void RccServer::WaitUntilAllPredSccExecuted(const RccScc& scc) {
  verify(scc.size() > 0);
  set<RccTx*> scc_set;
  scc_set.insert(scc.begin(), scc.end());
  std::function<int(RccTx&)> func =
      [&scc_set, &scc, this](RccTx& tx) -> int {
        if (tx.HasLogApplyStarted()) {
          return RccGraph::SearchHint::Skip;
        } else if (!tx.Involve(TxLogServer::partition_id_)) {
          verify(tx.status() >= TXN_CMT);
          return RccGraph::SearchHint::Skip;
        } else if (scc_set.find(&tx) != scc_set.end()) {
          // belong to the scc
          return RccGraph::SearchHint::Ok;
        } else {
          tx.status_.Wait([&tx](int v)->bool {
            return (v>=TXN_DCD);
          });
          return RccGraph::SearchHint::Ok;
        }
      };
  TraversePredNonRecursive(*scc[0], func);
}

bool RccServer::AllAncFns(const RccScc& scc) {
  verify(scc.size() > 0);
  set<RccTx*> scc_set;
  scc_set.insert(scc.begin(), scc.end());
  bool all_anc_fns = true;
  std::function<int(RccTx&)> func =
      [&all_anc_fns, &scc_set, &scc](RccTx& v) -> int {
        RccTx& info = v;
        if (info.HasLogApplyStarted()) {
          return RccGraph::SearchHint::Skip;
        } else if (info.status() >= TXN_DCD) {
          return RccGraph::SearchHint::Ok;
        } else if (scc_set.find(&v) != scc_set.end()) {
          return RccGraph::SearchHint::Ok;
        } else {
          all_anc_fns = false;
          info.to_checks_.insert(scc[0]);
          return RccGraph::SearchHint::Exit; // abort traverse
        }
      };
  TraversePred(*scc[0], -1, func);
  return all_anc_fns;
}

void RccServer::Execute(const RccScc& scc) {
  verify(scc.size() > 0);
  for (auto v : scc) {
    auto sp_tx = FindV(v->id());
    Execute(sp_tx);
  }
}

void RccServer::Execute(shared_ptr<RccTx> sp_tx) {
  tx_pending_execution_.push_back(sp_tx);
  sp_tx->log_apply_started_ = true;
  Log_debug("executing dtxn id %" PRIx64, sp_tx->id());
  verify(sp_tx->IsDecided());
  if (sp_tx->Involve(TxLogServer::partition_id_)) {
    Coroutine::CreateRun([sp_tx, this]() {
      if (sp_tx->need_validation_) {
        sp_tx->CommitValidate();
      } else {
        sp_tx->local_validated_->Set(SUCCESS);
      }
//      commo()->BroadcastValidation(sp_tx->id(), sp_tx->partition_,
//          sp_tx->local_validation_result_);
      sp_tx->sp_ev_commit_->Set(1);
      // TODO recover this?
//      sp_tx->global_validated_->Wait();
//      verify(sp_tx->global_validated_->status_ != Event::TIMEOUT);
      sp_tx->CommitExecute();
      sp_tx->local_validated_->Get();
    });
#ifdef CHECK_ISO
    MergeDeltas(sp_tx->deltas_);
#endif
  } else {
    // a tmp solution
    sp_tx->local_validated_->Set(SUCCESS);
  }
}


void RccServer::Abort(const RccScc& scc) {
  verify(0);
//  verify(scc.size() > 0);
//  for (auto v : scc) {
//    TxRococo& info = *v;
//    info.union_status(TXN_ABT); // FIXME, remove this.
//    verify(info.IsAborted());
//    auto dtxn = dynamic_pointer_cast<TxRococo>(GetTx(info.id()));
//    if (dtxn == nullptr) continue;
//    if (info.Involve(Scheduler::partition_id_)) {
//      dtxn->Abort();
//      dtxn->ReplyFinishOk();
//    }
//  }
}

RccCommo* RccServer::commo() {
//  if (commo_ == nullptr) {
//    verify(0);
//  }
  auto commo = dynamic_cast<RccCommo*>(commo_);
  verify(commo != nullptr);
  return commo;
}

void RccServer::DestroyExecutor(txnid_t txn_id) {
  auto dtxn = dynamic_pointer_cast<RccTx>(GetOrCreateTx(txn_id));
  verify(dtxn->committed_ || dtxn->aborted_);
  if (epoch_enabled_) {
//    Remove(txn_id);
    DestroyTx(txn_id);
  }
}

int RccServer::OnInquireValidation(txid_t tx_id) {
  std::lock_guard<std::recursive_mutex> lock(mtx_);
  auto dtxn = dynamic_pointer_cast<RccTx>(GetOrCreateTx(tx_id));
  int ret = 0;
  dtxn->local_validated_->Wait(100*1000*1000);
  if (dtxn->local_validated_->status_ == Event::TIMEOUT) {
    ret = -1; //TODO come back and remove this after the correctness checker.
  } else {
    dtxn->local_validated_->Wait();
    ret = dtxn->local_validated_->Get();
    verify(ret == SUCCESS || ret == REJECT);
//    verify(ret != REJECT);
  }
  return ret;
}

void RccServer::OnNotifyGlobalValidation(txid_t tx_id, int validation_result) {
  std::lock_guard<std::recursive_mutex> lock(mtx_);
  auto dtxn = dynamic_pointer_cast<RccTx>(GetOrCreateTx(tx_id));
  dtxn->global_validated_->Set(validation_result);
}

int RccServer::OnCommit(const txnid_t cmd_id,
                        rank_t rank,
                        bool need_validation,
                        shared_ptr<RccGraph> sp_graph,
                        TxnOutput *output) {
  std::lock_guard<std::recursive_mutex> lock(mtx_);
  verify(0);
//  if (RandomGenerator::rand(1, 2000) <= 1)
//    Log_info("on commit graph size: %d", graph.size());
  int ret = SUCCESS;
  // union the graph into dep graph
  auto dtxn = dynamic_pointer_cast<RccTx>(GetOrCreateTx(cmd_id));
  dtxn->fully_dispatched_->Set(1); // TODO make this and janus the same.
  verify(dtxn->p_output_reply_ == nullptr);
  dtxn->p_output_reply_ = output;
  verify(!dtxn->IsAborted());
  if (dtxn->HasLogApplyStarted()) {
    verify(dtxn->local_validated_->Get() != 0);
    ret = SUCCESS; // TODO no return output?
  } else {
//    Log_info("on commit: %llx par: %d", cmd_id, (int)partition_id_);
//    dtxn->commit_request_received_ = true;
    if (!sp_graph) {
      // quick path without graph, no contention.
      verify(dtxn->fully_dispatched_->value_); //cannot handle non-dispatched now.
      UpgradeStatus(*dtxn, TXN_DCD);
      Execute(dtxn);
    } else {
      // with graph
      auto index = Aggregate(*sp_graph);
//      TriggerCheckAfterAggregation(*sp_graph);
      WaitUntilAllPredecessorsAtLeastCommitting(dtxn.get());
      RccScc& scc = FindSccPred(*dtxn);
      Decide(scc);
      WaitUntilAllPredSccExecuted(scc);
      if (FullyDispatched(scc) && !scc[0]->HasLogApplyStarted()) {
        Execute(scc);
      }
    }
    dtxn->sp_ev_commit_->Wait();
    ret = dtxn->committed_ ? SUCCESS : REJECT;
  }
  return ret;
}

map<txnid_t, shared_ptr<RccTx>> RccServer::Aggregate(RccGraph &graph) {
  // aggregate vertexes
  map<txnid_t, shared_ptr<RccTx>> index;
  for (auto &pair: graph.vertex_index()) {
    auto rhs_v = pair.second;
    verify(pair.first == rhs_v->id());
    auto vertex = AggregateVertex(rhs_v);
    RccTx &dtxn = *vertex;
    if (dtxn.epoch_ == 0) {
      dtxn.epoch_ = epoch_mgr_.curr_epoch_;
    }
    epoch_mgr_.AddToEpoch(dtxn.epoch_, dtxn.tid_, dtxn.IsDecided());
    verify(vertex->id() == pair.second->id());
    verify(vertex_index().count(vertex->id()) > 0);
    index[vertex->id()] = vertex;
  }
  // aggregate edges.
  RebuildEdgePointer(index);

#ifdef DEBUG_CODE
  verify(index.size() == graph.vertex_index_.size());
  for (auto& pair: index) {
    txnid_t txnid = pair.first;
    RccVertex* v = pair.second;
    verify(v->parents_.size() == v->incoming_.size());
    auto sz = v->parents_.size();
    if (v->Get().status() >= TXN_CMT)
      RccSched::__DebugCheckParentSetSize(txnid, sz);
  }

  for (auto& pair: graph.vertex_index_) {
    auto txnid = pair.first;
    RccVertex* rhs_v = pair.second;
    auto lhs_v = FindV(txnid);
    verify(lhs_v != nullptr);
    // TODO, check the Sccs are the same.
    if (rhs_v->Get().status() >= TXN_DCD) {
      verify(lhs_v->Get().status() >= TXN_DCD);
      if (!AllAncCmt(rhs_v))
        continue;
      RccScc& rhs_scc = graph.FindSCC(rhs_v);
      for (RccVertex* rhs_vv : rhs_scc) {
        verify(rhs_vv->Get().status() >= TXN_DCD);
        RccVertex* lhs_vv = FindV(rhs_vv->id());
        verify(lhs_vv != nullptr);
        verify(lhs_vv->Get().status() >= TXN_DCD);
        verify(lhs_vv->GetParentSet() == rhs_vv->GetParentSet());
      }
      if (!AllAncCmt(lhs_v)) {
        continue;
      }
      RccScc& lhs_scc = FindSCC(lhs_v);
      for (RccVertex* lhs_vv : rhs_scc) {
        verify(lhs_vv->Get().status() >= TXN_DCD);
        RccVertex* rhs_vv = graph.FindV(lhs_v->id());
        verify(rhs_vv != nullptr);
        verify(rhs_vv->Get().status() >= TXN_DCD);
        verify(rhs_vv->GetParentSet() == rhs_vv->GetParentSet());
      }

      auto lhs_sz = lhs_scc.size();
      auto rhs_sz = rhs_scc.size();
      if (lhs_sz != rhs_sz) {
        // TODO
        for (auto& vv : rhs_scc) {
          auto vvv = FindV(vv->id());
          verify(vvv->Get().status() >= TXN_DCD);
        }
        verify(0);
      }
//      verify(sz >= rhs_sz);
//      verify(sz == rhs_sz);
      for (auto vv : rhs_scc) {
        bool r = std::any_of(lhs_scc.begin(),
                             lhs_scc.end(),
                             [vv] (RccVertex* vvv) -> bool {
                               return vvv->id() == vv->id();
                             }
        );
        verify(r);
      }
    }

    if (lhs_v->Get().status() >= TXN_DCD && AllAncCmt(lhs_v)) {
      RccScc& scc = FindSCC(lhs_v);
      for (auto vv : scc) {
        auto s = vv->Get().status();
        verify(s >= TXN_DCD);
      }
    }
  }

#endif
//  this->BuildEdgePointer(graph, index);
  return index;
}

} // namespace janus
