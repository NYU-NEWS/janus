
#include "deptran/procedure.h"
#include "graph.h"
#include "txn-info.h"
#include "tx.h"
#include "scheduler.h"
#include "commo.h"
#include "waitlist_checker.h"
#include "frame.h"

namespace janus {

map<txnid_t, int32_t> SchedulerRococo::__debug_xxx_s{};
std::recursive_mutex SchedulerRococo::__debug_mutex_s{};
void SchedulerRococo::__DebugCheckParentSetSize(txnid_t tid, int32_t sz) {
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

SchedulerRococo::SchedulerRococo() : Scheduler(), waitlist_(), mtx_() {
  RccGraph::sched_ = this;
  RccGraph::partition_id_ = Scheduler::partition_id_;
  RccGraph::managing_memory_ = false;
  waitlist_checker_ = new WaitlistChecker(this);
  epoch_enabled_ = true;
}

SchedulerRococo::~SchedulerRococo() {
  verify(waitlist_checker_);
  delete waitlist_checker_;
}

shared_ptr<Tx> SchedulerRococo::GetOrCreateTx(txnid_t tid, bool ro) {
  auto dtxn = dynamic_pointer_cast<TxRococo>(
      Scheduler::GetOrCreateTx(tid, ro));
  dtxn->partition_.insert(Scheduler::partition_id_);
  verify(dtxn->id() == tid);
  return dtxn;
}

int SchedulerRococo::OnDispatch(const vector<SimpleCommand>& cmd,
                                int32_t* res,
                                TxnOutput* output,
                                shared_ptr<RccGraph> graph) {
  std::lock_guard<std::recursive_mutex> guard(mtx_);
  verify(graph.get());
  txnid_t txn_id = cmd[0].root_id_;
  auto dtxn = dynamic_pointer_cast<TxRococo>(GetOrCreateTx(txn_id));
  verify(dtxn->id() == txn_id);
  verify(RccGraph::partition_id_ == Scheduler::partition_id_);
//  auto job = [&cmd, res, dtxn, callback, graph, output, this, txn_id] () {
  verify(cmd[0].partition_id_ == Scheduler::partition_id_);
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
  return 0;
}

int SchedulerRococo::OnCommit(cmdid_t cmd_id,
                              const RccGraph& graph,
                              TxnOutput* output,
                              const function<void()>& callback) {
  verify(0);
  std::lock_guard<std::recursive_mutex> lock(mtx_);
  // union the graph into dep graph
  auto dtxn = dynamic_pointer_cast<TxRococo>(GetTx(cmd_id));
  verify(dtxn != nullptr);
  verify(dtxn->ptr_output_repy_ == nullptr);
  dtxn->ptr_output_repy_ = output;
//  dtxn->finish_reply_callback_ = [callback](int r) { callback(); };
  Aggregate(epoch_mgr_.curr_epoch_, const_cast<RccGraph&>(graph));
  TriggerCheckAfterAggregation(const_cast<RccGraph&>(graph));

//  // fast path without check wait list?
//  if (graph.size() == 1) {
//    auto v = dep_graph_->FindV(cmd_id);
//    if (v->incoming_.size() == 0);
//    Execute(v->Get());
//    return 0;
//  } else {
//    Log_debug("graph size on commit, %d", (int) graph.size());
////    verify(0);
//  }
  return 0;
}

int SchedulerRococo::OnInquire(epoch_t epoch,
                               txnid_t txn_id,
                               shared_ptr<RccGraph> graph) {
  std::lock_guard<std::recursive_mutex> guard(mtx_);

  verify(0);
}

void SchedulerRococo::InquiredGraph(TxRococo& dtxn, shared_ptr<RccGraph> graph) {
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

void SchedulerRococo::CheckWaitlist() {
  std::lock_guard<std::recursive_mutex> lock(mtx_);
#ifdef DEBUG_CODE
  //  Log_info("start to check waitlist, length: %d, fridge size: %d",
  //           (int)waitlist_.size(), (int)fridge_.size());
#endif
  auto it = waitlist_.begin();
  while (it != waitlist_.end()) {
//    Log_info("checking loop, length: %d, fridge size: %d",
//             (int)waitlist_.¬size(), (int)fridge_.size());
    auto v = *it;
    verify(v != nullptr);
//  for (RccVertex *v : waitlist_) {
    // TODO minimize the length of the waitlist.
    InquireAboutIfNeeded(*v); // inquire about unknown transaction.
    if (v->status() >= TXN_CMT &&
        !v->IsExecuted()) {
      if (AllAncCmt(v)) {
//        check_again = true;
        RccScc& scc = FindSccPred(*v);
        verify(v->epoch_ > 0);
        Decide(scc);
        if (AllAncFns(scc)) {
          // FIXME
          for (auto vv : scc) {
            verify(vv->epoch_ > 0);
#ifdef DEBUG_CODE
            verify(AllAncCmt(vv));
          verify(vv->Get().status() >= TXN_DCD);
#endif
//          vv->Get().union_status(TXN_DCD);
          }
          if (FullyDispatched(scc)) {
            Execute(scc);
            for (auto v : scc) {
//              AddChildrenIntoWaitlist(v);
            }
          }
//        check_again = true;
        } // else do nothing.
#ifdef DEBUG_CODE
        for(auto vv : scc) {
          auto s = vv->Get().status();
          verify(s >= TXN_DCD);
        }
#endif
      } else {
        // else do nothing
#ifdef DEBUG_CODE
        //        auto anc_v = __DebugFindAnOngoingAncestor(v);
        //        RccDTxn& tinfo = *anc_v->data_;
        //        Log_debug("this transaction has some ongoing ancestors, tid: %llx, "
        //                      "is_involved: %d, during_inquire: %d, inquire_acked: %d",
        //                  tinfo.id(),
        //                  tinfo.Involve(partition_id_),
        //                  tinfo.during_asking,
        //                  tinfo.inquire_acked_);
#endif
      }
    } // else do nothing

    // Adjust the waitlist.
    __DebugExamineGraphVerify(*v);
    if (v->IsExecuted() ||
        v->IsAborted() ||
        (v->IsDecided() &&
            !v->Involve(Scheduler::partition_id_))) {
      // TODO? check for its descendants, perhaps
      // add redundant vertex here?
      //      AddChildrenIntoWaitlist(v);
      for (auto& vv: v->to_checks_) {
        waitlist_.insert(vv);
      }
      v->to_checks_.clear();
//      it = waitlist_.erase(it);
    } else {
//      it++;
    }
#ifdef DEBUG_CODE
    if (tinfo.IsExecuted()) {
      fridge_.erase(v);
    } else {
      fridge_.insert(v);
      if (v->Get().status() >= TXN_CMT && AllAncCmt(v)) {
        verify(v->Get().status() >= TXN_DCD);
      }
    }
#endif
    it = waitlist_.erase(it);
  }
  __DebugExamineFridge();
}

void SchedulerRococo::AddChildrenIntoWaitlist(TxRococo* v) {
  verify(0);
//  RccDTxn& tinfo = *v;
//  verify(tinfo.status() >= TXN_DCD && tinfo.IsExecuted());
//
//  for (auto& child_pair : v->outgoing_) {
//    RccDTxn* child_v = child_pair.first;
//    if (!child_v->IsExecuted() &&
//        waitlist_.count(child_v) == 0) {
//      waitlist_.insert(child_v);
//      verify(child_v->epoch_ > 0);
//    }
//  }
//  for (RccDTxn* child_v : v->removed_children_) {
//    if (!child_v->IsExecuted() &&
//        waitlist_.count(child_v) == 0) {
//      waitlist_.insert(child_v);
//      verify(child_v->epoch_ > 0);
//    }
//  }
//#ifdef DEBUG_CODE
//    fridge_.erase(v);
//#endif
}

void SchedulerRococo::__DebugExamineGraphVerify(TxRococo& v) {
#ifdef DEBUG_CODE
  for (auto& pair : v->incoming_) {
    verify(pair.first->outgoing_.count(v) > 0);
  }

  for (auto& pair : v->outgoing_) {
    verify(pair.first->incoming_.count(v) > 0);
  }
#endif
}

void SchedulerRococo::__DebugExamineFridge() {
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

void SchedulerRococo::InquireAboutIfNeeded(TxRococo& dtxn) {
//  Graph<RccDTxn> &txn_gra = dep_graph_->txn_gra_;
  if (dtxn.status() <= TXN_STD &&
      !dtxn.during_asking &&
      !dtxn.Involve(Scheduler::partition_id_)) {
    verify(!dtxn.Involve(Scheduler::partition_id_));
    verify(!dtxn.during_asking);
    parid_t par_id = *(dtxn.partition_.begin());
    dtxn.during_asking = true;
    commo()->SendInquire(par_id,
                         dtxn.epoch_,
                         dtxn.tid_,
                         std::bind(&SchedulerRococo::InquireAck,
                                   this,
                                   dtxn.id(),
                                   std::placeholders::_1));
  }
}

void SchedulerRococo::InquireAck(cmdid_t cmd_id, RccGraph& graph) {
  std::lock_guard<std::recursive_mutex> lock(mtx_);
  auto v = FindV(cmd_id);
  verify(v != nullptr);
  TxRococo& tinfo = *v;
  tinfo.inquire_acked_ = true;
  Aggregate(epoch_mgr_.curr_epoch_, const_cast<RccGraph&>(graph));
  TriggerCheckAfterAggregation(const_cast<RccGraph&>(graph));
  verify(tinfo.status() >= TXN_CMT);
}

TxRococo& SchedulerRococo::__DebugFindAnOngoingAncestor(TxRococo& vertex) {
  TxRococo* ret = nullptr;
  set<TxRococo*> walked;
  std::function<bool(TxRococo&)> func = [&ret, &vertex](TxRococo& v) -> bool {
    TxRococo& info = v;
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

bool SchedulerRococo::AllAncCmt(TxRococo* vertex) {
  bool all_anc_cmt = true;
  std::function<int(TxRococo&)> func =
      [&all_anc_cmt, &vertex](TxRococo& v) -> int {
        TxRococo& parent = v;
        int r = 0;
        if (parent.IsExecuted() || parent.IsAborted()) {
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

void SchedulerRococo::Decide(const RccScc& scc) {
  for (auto v : scc) {
    UpgradeStatus(*v, TXN_DCD);
//    Log_info("txnid: %llx, parent size: %d", v->id(), v->parents_.size());
  }
}

bool SchedulerRococo::HasICycle(const RccScc& scc) {
  for (auto& vertex : scc) {
    set<TxRococo*> walked;
    bool ret = false;
    std::function<bool(TxRococo*)> func =
        [&ret, vertex](TxRococo* v) -> bool {
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

void SchedulerRococo::TriggerCheckAfterAggregation(RccGraph& graph) {
  bool check = false;
  for (auto& pair: graph.vertex_index()) {
    // TODO optimize here.
    auto txnid = pair.first;
    auto rhs_v = pair.second;
    auto v = FindV(txnid);
    verify(v != nullptr);
#ifdef DEBUG_CODE
    // TODO, check the Sccs are the same.
    if (rhs_v->Get().status() >= TXN_DCD && dep_graph_->AllAncCmt(rhs_v)) {
      RccScc& rhs_scc = graph.FindSCC(rhs_v);
      for (auto vv : rhs_scc) {
        verify(vv->Get().status() >= TXN_DCD);
      }
      if (AllAncCmt(v)) {
        RccScc& scc = dep_graph_->FindSCC(v);
        auto sz = scc.size();
        auto rhs_sz = rhs_scc.size();
        verify(sz >= rhs_sz);
        verify(sz == rhs_sz);
        for (auto vv : rhs_scc) {
          bool r = std::any_of(scc.begin(),
                               scc.end(),
                               [vv] (RccVertex* vvv) -> bool {
                                 return vvv->id() == vv->id();
                               }
          );
          verify(r);
        }
      }
    }
//
//    if (v->Get().status() >= TXN_DCD) {
//      RccScc& scc = dep_graph_->FindSCC(v);
//      for (auto vv : scc) {
//        auto s = vv->Get().status();
//        verify(s >= TXN_DCD);
//      }
//    }
    if (v->Get().status() >= TXN_CMT && AllAncCmt(v)) {
      dep_graph_->FindSCC(v);
    }
#endif
//    if (v->Get().status() >= TXN_CMT)
    check = true;
    waitlist_.insert(v.get());
    verify(v->epoch_ > 0);
  }
  if (check)
    CheckWaitlist();
}

bool SchedulerRococo::HasAbortedAncestor(const RccScc& scc) {
  verify(scc.size() > 0);
  bool has_aborted = false;
  std::function<int(TxRococo&)> func =
      [&has_aborted](TxRococo& v) -> int {
        TxRococo& info = v;
        if (info.IsExecuted()) {
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

bool SchedulerRococo::FullyDispatched(const RccScc& scc) {
  bool ret = std::all_of(scc.begin(),
                         scc.end(),
                         [this](TxRococo* v) {
                           bool r = true;
                           TxRococo& tinfo = *v;
                           if (tinfo.Involve(Scheduler::partition_id_)) {
                             r = tinfo.fully_dispatched;
                           }
                           return r;
                         });
  return ret;
}

bool SchedulerRococo::AllAncFns(const RccScc& scc) {
  verify(scc.size() > 0);
  set<TxRococo*> scc_set;
  scc_set.insert(scc.begin(), scc.end());
  bool all_anc_fns = true;
  std::function<int(TxRococo&)> func =
      [&all_anc_fns, &scc_set, &scc](TxRococo& v) -> int {
        TxRococo& info = v;
        if (info.IsExecuted()) {
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
};

void SchedulerRococo::Execute(const RccScc& scc) {
  verify(scc.size() > 0);
  for (auto v : scc) {
    TxRococo& dtxn = *v;
    Execute(dtxn);
  }
}

void SchedulerRococo::Execute(TxRococo& dtxn) {
  dtxn.executed_ = true;
  verify(dtxn.IsDecided());
  verify(dtxn.epoch_ > 0);
  if (dtxn.Involve(Scheduler::partition_id_)) {
    dtxn.CommitExecute();
#ifdef CHECK_ISO
    MergeDeltas(dtxn.deltas_);
#endif
    dtxn.ReplyFinishOk();
    TrashExecutor(dtxn.tid_);
  }
}

void SchedulerRococo::Abort(const RccScc& scc) {
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

RccCommo* SchedulerRococo::commo() {
//  if (commo_ == nullptr) {
//    verify(0);
//  }
  auto commo = dynamic_cast<RccCommo*>(commo_);
  verify(commo != nullptr);
  return commo;
}

void SchedulerRococo::DestroyExecutor(txnid_t txn_id) {
  auto dtxn = dynamic_pointer_cast<TxRococo>(GetOrCreateTx(txn_id));
  verify(dtxn->committed_ || dtxn->aborted_);
  if (epoch_enabled_) {
//    Remove(txn_id);
    DestroyTx(txn_id);
  }
}

int SchedulerRococo::OnCommit(const txnid_t cmd_id,
                              shared_ptr<RccGraph> sp_graph,
                              TxnOutput *output) {
  std::lock_guard<std::recursive_mutex> lock(mtx_);
//  if (RandomGenerator::rand(1, 2000) <= 1)
//    Log_info("on commit graph size: %d", graph.size());
  int ret = SUCCESS;
  // union the graph into dep graph
  auto dtxn = dynamic_pointer_cast<TxRococo>(GetOrCreateTx(cmd_id));
  dtxn->fully_dispatched = true; // TODO make this and janus the same.
//  verify(dtxn != nullptr);
  verify(dtxn->ptr_output_repy_ == nullptr);
  dtxn->ptr_output_repy_ = output;

  if (dtxn->IsExecuted()) {
//    verify(info.status() >= TXN_DCD);
//    verify(info.graphs_for_inquire_.size() == 0);
    ret = SUCCESS;
  } else if (dtxn->IsAborted()) {
    verify(0);
    ret = REJECT;
  } else {
//    Log_info("on commit: %llx par: %d", cmd_id, (int)partition_id_);
    dtxn->commit_request_received_ = true;
    if (!sp_graph) {
      // quick path without graph, no contention.
      verify(dtxn->fully_dispatched); //cannot handle non-dispatched now.
      UpgradeStatus(*dtxn, TXN_DCD);
      Execute(*dtxn);
      if (dtxn->to_checks_.size() > 0) {
        for (auto child : dtxn->to_checks_) {
          waitlist_.insert(child);
        }
        CheckWaitlist();
      }
    } else {
      // with graph
      auto index = Aggregate(*sp_graph);
//      for (auto& pair: index) {
//        verify(pair.second->epoch_ > 0);
//      }
      TriggerCheckAfterAggregation(*sp_graph);
    }
    dtxn->sp_ev_commit_->Wait();
    ret = dtxn->committed_ ? SUCCESS : REJECT;
    // fast path without check wait list?
//    if (graph.size() == 1) {
//      auto v = dep_graph_->FindV(cmd_id);
//      if (v->incoming_.size() == 0);
//      CheckInquired(v->Get());
//      Execute(v->Get());
//      return;
//    } else {
//      Log_debug("graph size on commit, %d", (int) graph.size());
////    verify(0);
//    }
  }
  return ret;
}

map<txnid_t, shared_ptr<TxRococo>> SchedulerRococo::Aggregate(RccGraph &graph) {
  // aggregate vertexes
  map<txnid_t, shared_ptr<TxRococo>> index;
  for (auto &pair: graph.vertex_index()) {
    auto rhs_v = pair.second;
    verify(pair.first == rhs_v->id());
    auto vertex = AggregateVertex(rhs_v);
    TxRococo &dtxn = *vertex;
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
