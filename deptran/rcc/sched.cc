
#include "../txn_chopper.h"
#include "graph.h"
#include "txn-info.h"
#include "sched.h"
#include "dtxn.h"
#include "commo.h"
#include "waitlist_checker.h"
#include "frame.h"

namespace rococo {

map<txnid_t, int32_t> RccSched::__debug_xxx_s{};
std::recursive_mutex RccSched::__debug_mutex_s{};
void RccSched::__DebugCheckParentSetSize(txnid_t tid, int32_t sz) {
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

RccSched::RccSched() : Scheduler(), waitlist_(), mtx_() {
  RccGraph::sched_ = this;
  RccGraph::partition_id_ = Scheduler::partition_id_;
  waitlist_checker_ = new WaitlistChecker(this);
  epoch_enabled_ = false;
}

RccSched::~RccSched() {
  verify(waitlist_checker_);
  delete waitlist_checker_;
}


DTxn* RccSched::GetOrCreateDTxn(txnid_t tid, bool ro) {
//  RccDTxn* dtxn = (RccDTxn*) Scheduler::GetOrCreateDTxn(tid, ro);
  RccVertex* v = FindOrCreateRccVertex(tid, this);
//  RccDTxn* dtxn = (RccDTxn*) dep_graph_->FindOrCreateTxnInfo(tid);
  RccDTxn& dtxn = *v;
  if (dtxn.epoch_ == 0) {
    dtxn.epoch_ = epoch_mgr_.curr_epoch_;
    if (epoch_enabled_) {
      epoch_mgr_.AddToCurrent(tid);
      TriggerUpgradeEpoch();
    }
  }
  auto pair = epoch_dtxn_[dtxn.epoch_].insert(&dtxn);
  if (pair.second) {
    active_epoch_[dtxn.epoch_]++;
  }
  verify(dtxn.graph_ != nullptr);
  if (dtxn.txn_reg_ == nullptr) {
    dtxn.txn_reg_ = txn_reg_;
  }
  return &dtxn;
}

int RccSched::OnDispatch(const vector<SimpleCommand> &cmd,
                         int32_t *res,
                         TxnOutput *output,
                         RccGraph *graph,
                         const function<void()> &callback) {
  std::lock_guard<std::recursive_mutex> guard(mtx_);
  verify(graph);
  txnid_t txn_id = cmd[0].root_id_;
  RccDTxn *dtxn = (RccDTxn *) GetOrCreateDTxn(txn_id);
  verify(RccGraph::partition_id_ == Scheduler::partition_id_);
//  auto job = [&cmd, res, dtxn, callback, graph, output, this, txn_id] () {
  verify(cmd[0].partition_id_ == Scheduler::partition_id_);
  for (auto&c : cmd) {
    dtxn->DispatchExecute(c, res, &(*output)[c.inn_id()]);
  }
  dtxn->UpdateStatus(TXN_STD);
  int depth = 1;
  verify(cmd[0].root_id_ == txn_id);
  auto sz = MinItfrGraph(txn_id, graph, true, depth);
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
  callback();
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
}

int RccSched::OnCommit(cmdid_t cmd_id,
                       const RccGraph &graph,
                       TxnOutput* output,
                       const function<void()> &callback) {
  std::lock_guard<std::recursive_mutex> lock(mtx_);
  // union the graph into dep graph
  RccDTxn *dtxn = (RccDTxn*) GetDTxn(cmd_id);
  verify(dtxn != nullptr);
  verify(dtxn->ptr_output_repy_ == nullptr);
  dtxn->ptr_output_repy_ = output;
  dtxn->finish_reply_callback_ = [callback] (int r) {callback();};
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
}

int RccSched::OnInquire(cmdid_t cmd_id,
                        RccGraph *graph,
                        const function<void()> &callback) {
//  DragonBall *ball = new DragonBall(2, [this, cmd_id, callback, graph] () {
//    dep_graph_->MinItfrGraph(cmd_id, graph);
//    callback();
//  });
  std::lock_guard<std::recursive_mutex> guard(mtx_);

  verify(0);
  RccVertex* v = FindV(cmd_id);
  verify(v != nullptr);
  RccDTxn& info = *v;
  //register an event, triggered when the status >= COMMITTING;
  verify (info.Involve(Scheduler::partition_id_));

  if (info.status() >= TXN_CMT) {
    MinItfrGraph(cmd_id, graph, false, 1);
    callback();
  } else {
    info.graphs_for_inquire_.push_back(graph);
    info.callbacks_for_inquire_.push_back(callback);
    verify(info.graphs_for_inquire_.size() ==
        info.callbacks_for_inquire_.size());
  }

}

void RccSched::AnswerIfInquired(RccDTxn &tinfo) {
  // reply inquire requests if possible.
  auto sz1 = tinfo.graphs_for_inquire_.size();
  auto sz2 = tinfo.callbacks_for_inquire_.size();
  if (sz1 != sz2) {
    Log_fatal("graphs for inquire sz %d, callbacks sz %d",
              (int) sz1, (int) sz2);
  }
  if (tinfo.status() >= TXN_CMT && tinfo.graphs_for_inquire_.size() > 0) {
    for (auto& graph : tinfo.graphs_for_inquire_) {
      verify(graph != nullptr);
      MinItfrGraph(tinfo.id(), graph, false, 1);
    }
    for (auto& callback : tinfo.callbacks_for_inquire_) {
      verify(callback);
      callback();
    }
    tinfo.callbacks_for_inquire_.clear();
    tinfo.graphs_for_inquire_.clear();
  }
}

void RccSched::CheckWaitlist() {
  std::lock_guard<std::recursive_mutex> lock(mtx_);
#ifdef DEBUG_CODE
//  Log_info("start to check waitlist, length: %d, fridge size: %d",
//           (int)waitlist_.size(), (int)fridge_.size());
#endif
  while (waitlist_.size() > 0) {
//    Log_info("checking loop, length: %d, fridge size: %d",
//             (int)waitlist_.¬size(), (int)fridge_.size());
    auto it = waitlist_.begin();
    RccVertex* v = *it;
    verify(v != nullptr);
//  for (RccVertex *v : waitlist_) {
    // TODO minimize the length of the waitlist.
    RccDTxn& tinfo = *v;
    AnswerIfInquired(tinfo);
    InquireAboutIfNeeded(tinfo); // inquire about unknown transaction.
    if (tinfo.status() >= TXN_CMT &&
        !tinfo.IsExecuted()) {
      if (AllAncCmt(v)) {
//        check_again = true;
        RccScc& scc = FindSCC(v);
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
              AddChildrenIntoWaitlist(v);
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

//    if (tinfo.status() >= TXN_DCD &&
//        !tinfo.IsExecuted() &&
//        dep_graph_->AllAncCmt(v) ) {
//      RccScc& scc = dep_graph_->FindSCC(v);
//#ifdef DEBUG_CODE
//      for (auto vv : scc) {
//        auto s = vv->Get().status();
//        verify(s >= TXN_DCD);
//      }
//#endif
//      if (scc.size() > 1 && HasICycle(scc)) {
//        Abort(scc);
//      } else if (HasAbortedAncestor(scc)) {
//        Abort(scc);
//      } else if (AllAncFns(scc)) {
//        // FIXME
//        for (auto vv : scc) {
//#ifdef DEBUG_CODE
//          verify(AllAncCmt(vv));
//          verify(vv->Get().status() >= TXN_DCD);
//#endif
////          vv->Get().union_status(TXN_DCD);
//        }
//        if (FullyDispatched(scc)) {
//          Execute(scc);
//          for (auto v : scc) {
//            AddChildrenIntoWaitlist(v);
//          }
//        }
////        check_again = true;
//      } // else do nothing.
//    } // else do nothing

    // Adjust the waitlist.
    __DebugExamineGraphVerify(v);
    if (tinfo.status() >= TXN_DCD && tinfo.IsExecuted()) {
      AddChildrenIntoWaitlist(v);
    }

    if (tinfo.IsExecuted() ||
        tinfo.IsAborted() ||
        (tinfo.IsDecided() &&
            !tinfo.Involve(Scheduler::partition_id_))) {
      // check for its descendants, perhaps add redundant vertex here.
//      for (auto child_pair : v->outgoing_) {
//        auto child_v = child_pair.first;
//        waitlist_.insert(child_v);
//      }
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
    waitlist_.erase(it);
  }
//  auto sz = waitlist_.size();
//  verify(check_again == (sz > 0));
//  if (sz > 0 && check_again)
//    CheckWaitlist();
//  else {
  __DebugExamineFridge();
//  }
}

void RccSched::AddChildrenIntoWaitlist(RccVertex* v) {
  RccDTxn& tinfo = *v;
  verify(tinfo.status() >= TXN_DCD && tinfo.IsExecuted());

  for (auto& child_pair : v->outgoing_) {
    RccVertex* child_v = child_pair.first;
    if (!child_v->IsExecuted() &&
        waitlist_.count(child_v) == 0) {
      waitlist_.insert(child_v);
      verify(child_v->epoch_ > 0);
    }
  }
  for (RccVertex* child_v : v->removed_children_) {
    if (!child_v->IsExecuted() &&
        waitlist_.count(child_v) == 0) {
      waitlist_.insert(child_v);
      verify(child_v->epoch_ > 0);
    }
  }
#ifdef DEBUG_CODE
    fridge_.erase(v);
#endif
}

void RccSched::__DebugExamineGraphVerify(RccVertex *v) {
#ifdef DEBUG_CODE
  for (auto& pair : v->incoming_) {
    verify(pair.first->outgoing_.count(v) > 0);
  }

  for (auto& pair : v->outgoing_) {
    verify(pair.first->incoming_.count(v) > 0);
  }
#endif
}

void RccSched::__DebugExamineFridge() {
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

void RccSched::InquireAboutIfNeeded(RccDTxn &dtxn) {
//  Graph<RccDTxn> &txn_gra = dep_graph_->txn_gra_;
  if (dtxn.status() <= TXN_STD &&
      !dtxn.Involve(Scheduler::partition_id_) &&
      !dtxn.during_asking) {
    verify(!dtxn.Involve(Scheduler::partition_id_));
    verify(!dtxn.during_asking);
    parid_t par_id = *(dtxn.partition_.begin());
    dtxn.during_asking = true;
    commo()->SendInquire(par_id,
                         dtxn.tid_,
                         std::bind(&RccSched::InquireAck,
                                   this,
                                   dtxn.id(),
                                   std::placeholders::_1));
  }
}

void RccSched::InquireAbout(RccVertex *av) {
//  Graph<RccDTxn> &txn_gra = dep_graph_->txn_gra_;
  RccDTxn &tinfo = *av;
  verify(!tinfo.Involve(Scheduler::partition_id_));
  verify(!tinfo.during_asking);
  parid_t par_id = *(tinfo.partition_.begin());
  tinfo.during_asking = true;
  commo()->SendInquire(par_id,
                       tinfo.tid_,
                       std::bind(&RccSched::InquireAck,
                                 this,
                                 tinfo.id(),
                                 std::placeholders::_1));
}

void RccSched::InquireAck(cmdid_t cmd_id, RccGraph& graph) {
  std::lock_guard<std::recursive_mutex> lock(mtx_);
  auto v = FindV(cmd_id);
  RccDTxn& tinfo = *v;
  tinfo.inquire_acked_ = true;
  Aggregate(epoch_mgr_.curr_epoch_, const_cast<RccGraph&>(graph));
  TriggerCheckAfterAggregation(const_cast<RccGraph&>(graph));
  verify(tinfo.status() >= TXN_CMT);
}

RccVertex* RccSched::__DebugFindAnOngoingAncestor(RccVertex* vertex) {
  RccVertex* ret = nullptr;
  set<RccVertex*> walked;
  std::function<bool(RccVertex*)> func = [&ret, vertex] (RccVertex* v) -> bool {
    RccDTxn& info = *v;
    if (info.status() >= TXN_CMT) {
      return true;
    } else {
      ret = v;
      return false; // stop traversing.
    }
  };
  TraversePred(vertex, -1, func, walked);
  return ret;
}

bool RccSched::AllAncCmt(RccVertex *vertex) {
  bool all_anc_cmt = true;
  std::function<int(RccVertex*)> func = [&all_anc_cmt] (RccVertex* v) -> int {
    RccDTxn& info = *v;
    int r = 0;
    if (info.IsExecuted() || info.IsAborted()) {
      r = RccGraph::SearchHint::Skip;
    } else if (info.status() >= TXN_CMT) {
      r = RccGraph::SearchHint::Ok;
    } else {
      r = RccGraph::SearchHint::Exit;
      all_anc_cmt = false;
    }
    return r;
  };
  TraversePred(vertex, -1, func);
  return all_anc_cmt;
}

void RccSched::Decide(const RccScc& scc) {
  for (auto v : scc) {
    RccDTxn& info = *v;
    UpgradeStatus(v, TXN_DCD);
//    Log_info("txnid: %llx, parent size: %d", v->id(), v->parents_.size());
  }
}

bool RccSched::HasICycle(const RccScc& scc) {
  for (auto& vertex : scc) {
    set<RccVertex *> walked;
    bool ret = false;
    std::function<bool(RccVertex *)> func =
        [&ret, vertex](RccVertex *v) -> bool {
          if (v == vertex) {
            ret = true;
            return false;
          }
          return true;
        };
    TraverseDescendant(vertex, -1, func, walked, EDGE_I);
    if (ret) return true;
  }
  return false;
};

void RccSched::TriggerCheckAfterAggregation(RccGraph &graph) {
  bool check = false;
  for (auto& pair: graph.vertex_index_) {
    // TODO optimize here.
    auto txnid = pair.first;
    RccVertex* rhs_v = pair.second;
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
    waitlist_.insert(v);
    verify(v->epoch_ > 0);
  }
  if (check)
    CheckWaitlist();
}


bool RccSched::HasAbortedAncestor(const RccScc& scc) {
  verify(scc.size() > 0);
  bool has_aborted = false;
  std::function<int(RccVertex*)> func =
      [&has_aborted] (RccVertex* v) -> int {
        RccDTxn& info = *v;
        if (info.IsExecuted()) {
          return RccGraph::SearchHint::Skip;
        }
        if (info.IsAborted()) {
          has_aborted = true; // found aborted transaction.
          return RccGraph::SearchHint::Exit; // abort traverse
        }
        return RccGraph::SearchHint::Ok;
      };
  TraversePred(scc[0], -1, func);
  return has_aborted;
};

bool RccSched::FullyDispatched(const RccScc& scc) {
  bool ret = std::all_of(scc.begin(),
                         scc.end(),
                         [this] (RccVertex *v) {
                           bool r = true;
                           RccDTxn& tinfo = *v;
                           if (tinfo.Involve(Scheduler::partition_id_)) {
                             r = tinfo.fully_dispatched;
                           }
                           return r;
                         });
  return ret;
}

bool RccSched::AllAncFns(const RccScc& scc) {
  verify(scc.size() > 0);
  set<RccVertex*> scc_set;
  scc_set.insert(scc.begin(), scc.end());
  bool all_anc_fns = true;
  std::function<int(RccVertex*)> func =
      [&all_anc_fns, &scc_set] (RccVertex* v) -> int {
    RccDTxn& info = *v;
    if (info.IsExecuted()) {
      return RccGraph::SearchHint::Skip;
    } else if (info.status() >= TXN_DCD) {
      return RccGraph::SearchHint::Ok;
    } else if (scc_set.find(v) != scc_set.end()) {
      return RccGraph::SearchHint::Ok;
    } else {
      all_anc_fns = false;
      return RccGraph::SearchHint::Exit; // abort traverse
    }
  };
  TraversePred(scc[0], -1, func);
  return all_anc_fns;
};

void RccSched::Execute(const RccScc& scc) {
  verify(scc.size() > 0);
  for (auto v : scc) {
    RccDTxn& dtxn = *v;
    Execute(dtxn);
  }
}

void RccSched::Execute(RccDTxn& dtxn) {
  dtxn.executed_ = true;
  verify(dtxn.IsDecided());
  verify(dtxn.epoch_ > 0);
  if (dtxn.Involve(Scheduler::partition_id_)) {
    dtxn.CommitExecute();
    dtxn.ReplyFinishOk();
    TrashExecutor(dtxn.tid_);
  }
}

void RccSched::Abort(const RccScc& scc) {
  verify(0);
  verify(scc.size() > 0);
  for (auto v : scc) {
    RccDTxn& info = *v;
    info.union_status(TXN_ABT); // FIXME, remove this.
    verify(info.IsAborted());
    RccDTxn *dtxn = (RccDTxn *) GetDTxn(info.id());
    if (dtxn == nullptr) continue;
    if (info.Involve(Scheduler::partition_id_)) {
      dtxn->Abort();
      dtxn->ReplyFinishOk();
    }
  }
}

RccCommo* RccSched::commo() {
//  if (commo_ == nullptr) {
//    verify(0);
//  }
  auto commo = dynamic_cast<RccCommo*>(commo_);
  verify(commo != nullptr);
  return commo;
}

void RccSched::DestroyExecutor(txnid_t txn_id) {
  RccDTxn* dtxn = (RccDTxn*) GetOrCreateDTxn(txn_id);
  verify(dtxn->committed_ || dtxn->aborted_);
  if (epoch_enabled_) {
    Remove(txn_id);
  }
}

} // namespace rococo
