
#include "../procedure.h"
#include "graph.h"
#include "tx.h"
#include "server.h"
#include "commo.h"
#include "frame.h"

namespace janus {


RccServer::RccServer() : TxLogServer(), mtx_() {
  RccGraph::sched_ = this;
//  RccGraph::partition_id_ = TxLogServer::partition_id_;
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
//  verify(RccGraph::partition_id_ == TxLogServer::partition_id_);
  verify(cmd[0].partition_id_ == TxLogServer::partition_id_);
  for (auto& c : cmd) {
    dtxn->DispatchExecute(const_cast<SimpleCommand&>(c),
                          &(*output)[c.inn_id()]);
  }
  dtxn->UpdateStatus(TXN_STD);
  int depth = 1;
  verify(cmd[0].root_id_ == txn_id);
//  auto sz = MinItfrGraph(*dtxn, graph, true, depth);
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

map<txid_t, parent_set_t>
RccServer::OnInquire(txnid_t tx_id) {
  std::lock_guard<std::recursive_mutex> lock(mtx_);
  auto sp_tx = dynamic_pointer_cast<RccTx>(GetOrCreateTx(tx_id));
  verify (sp_tx->Involve(TxLogServer::partition_id_));
  //register an event, triggered when the status >= COMMITTING;
  sp_tx->status_.WaitUntilGreaterOrEqualThan(TXN_CMT);
  map<txid_t, parent_set_t> ret;
  if (sp_tx->IsDecided()) {
    verify(!sp_tx->scc_->empty());
    for (auto v : *sp_tx->scc_) {
      ret[v->id()] = v->parents_;
    }
  } else {
    ret[0] = sp_tx->parents_;
  }
  return ret;
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
//    verify(!dtxn.during_asking);
    parid_t par_id = *(dtxn.partition_.begin());
    verify(dtxn.partition_.size() > 0);
    if (!dtxn.during_asking) {
      dtxn.during_asking = true;
      auto result = commo()->Inquire(par_id, dtxn.tid_);
//      auto s = result->size() > 1 ? TXN_DCD : TXN_CMT;
      auto scc = std::make_shared<vector<RccTx*>>();
      verify(!result->empty());
      if (result->size() == 1) {
        for (auto& pair: *result) {
          auto& id = pair.first;
          ReplaceParentEdges(dtxn.parents_, pair.second);
          dtxn.UpdateStatus(TXN_CMT);
          if (id > 0) {
            dtxn.scc_->push_back(&dtxn);
            dtxn.UpdateStatus(TXN_DCD);
          }
        }
      } else {
        for (auto& pair : *result) {
          auto id = pair.first;
          verify(id > 0);
          auto& parents_map = pair.second;
          auto tx = FindOrCreateV(id);
          ReplaceParentEdges(tx->parents_, parents_map);
          tx->UpdateStatus(TXN_CMT);
          scc->push_back(tx.get());
        }
        for (auto v : *scc) {
          if(!v->scc_->empty()) {
#ifdef DEBUG_CHECK
            verify(*v->scc_ == *scc);
#endif
            continue;
          } else {
            v->scc_ = scc;
          }
        }
      }
      verify(dtxn.status() >= TXN_CMT);
    }
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
  if (vertex->all_anc_cmt_hint) {
    return;
  }
  if (vertex->waiting_all_anc_committing_) {
    vertex->wait_all_anc_commit_done_.WaitUntilGreaterOrEqualThan(1);
  }


//  bool skip_search = true;
//  for (auto& pair : vertex->parents_) {
//    auto parent = FindOrCreateParentVPtr(*vertex, pair.first, pair.second);
//    if (parent->all_anc_cmt_hint) {
//      continue;
//    }
//    if (!parent->Involve(partition_id_)) {
//      skip_search = false;
//      continue;
//    }
//    parent->status_.WaitUntilGreaterOrEqualThan(TXN_CMT);
//    if (parent->waiting_all_anc_committing_) {
//      parent->wait_all_anc_commit_done_.WaitUntilGreaterOrEqualThan(1);
//    } else {
//      skip_search = false;
//    }
//  }
//  if (skip_search) {
//    vertex->all_anc_cmt_hint = true;
//    vertex->wait_all_anc_commit_done_.Set(1);
//    return;
//  }

//
//  if (skip_search) {
//    vertex->waiting_all_anc_committing_ = true;
//    for (auto& pair : vertex->parents_) {
//      auto parent = FindOrCreateParentVPtr(*vertex, pair.first, pair.second);
//      parent->wait_all_anc_commit_done_.WaitUntilGreaterOrEqualThan(1);
//    }
//    vertex->wait_all_anc_commit_done_.Set(1);
//    vertex->all_anc_cmt_hint = true;
//    return;
//  }
#ifdef DEBUG_CHECK
  verify(!vertex->__debug_entered_committing_);
  auto __debug_id = vertex->id();
  auto rno = RandomGenerator::rand(1,10000000);
  vertex->__debug_random_number_ = rno;
  vertex->__debug_entered_committing_ = true;
  auto& xxpair = __debug_search_status_[vertex->id()];
  auto& xxxxx = xxpair.first;
  auto& xxxxxyyyyy = xxpair.second;
  verify(xxxxx == 0);
  verify(xxxxxyyyyy == nullptr);
  xxxxx = RccTx::TRAVERSING;
#endif
//  if (vertex->waiting_all_anc_committing_) {
//     already on a search path
//    verify(vertex->waiting_on_ != nullptr);
//    verify(*vertex->waiting_on_ != nullptr);
//    vertex->wait_all_anc_commit_done_.WaitUntilGreaterOrEqualThan(1);
//    return;
//  }
//  RccTx** waitingon = new (RccTx*); // TODO memory leak, fix later
//  *waitingon = nullptr;
//  vertex->waiting_on_ = waitingon;
  auto searched_set = std::make_shared<set<RccTx*>>();
  auto self_searched_set = std::make_shared<set<RccTx*>>();
  auto func =
      [&](RccTx& self, RccTx& parent, unordered_set<RccTx*>& walked_set) -> int {
#ifdef DEBUG_CHECK
        searched_set->insert(&parent);
        self_searched_set->insert(&self);
        verify(&self != &parent);
        verify(vertex->id() == __debug_id);
        if (self.waiting_all_anc_committing_) {
//          verify(self.__debug_search_path_init_txid_ > 0);
          if (!self.all_anc_cmt_hint) {
            auto yyyy = self.traverse_path_start_->id();
            auto zzpair = this->__debug_search_status_[yyyy];
            auto zzfirst = zzpair.first;
            auto zzsecond = zzpair.second;
            auto wo = self.traverse_path_start_->traverse_path_waitingon_;
            verify(zzsecond == wo);
            if (wo == nullptr) {
              verify(self.traverse_path_start_->id() == __debug_id);
//              verify(self.__debug_random_number_2 == vertex->__debug_random_number_init_);
//              verify(self.__debug_random_number_ == rno);
            }
          }
        }
        self.__debug_random_number_ = rno;
        self.__debug_random_number_2 = vertex->__debug_random_number_init_;
//        self.__debug_search_path_init_txid_ = __debug_id;
//        verify(self.__debug_search_path_init_txid_ == __debug_id);
        verify(vertex->id() == __debug_id);
#endif
        self.waiting_all_anc_committing_ = true;
        self.traverse_path_start_ = vertex; // some redundancy here, can be optimized later.
        if (self.all_anc_cmt_hint) {
          return RccGraph::SearchHint::Skip;
        }
        if (parent.all_anc_cmt_hint) {
          return RccGraph::SearchHint::Skip;
        }
        if (parent.HasLogApplyStarted()) {
          verify(parent.status() >= TXN_CMT);
          verify(parent.all_anc_cmt_hint);
          return RccGraph::SearchHint::Skip;
        }
        if (parent.IsDecided()) {
//          verify(parent.all_anc_cmt_hint);
          return RccGraph::SearchHint::Skip;
        }
        auto& self_par_id = TxLogServer::partition_id_;
        // reduce traverse graph time
        // event parent is in the same SCC, at least one transaction in the SCC should commit.
        if (parent.Involve(self_par_id)) {
//          if (!parent.IsCommitting()) {
            if (parent.tid_ < vertex->tid_) {
#ifdef DEBUG_CHECK
            xxxxx = RccTx::WAITING_NO_DEADLOCK;
            xxxxxyyyyy = &parent;
#endif
            vertex->traverse_path_waitingon_ = &parent;
            vertex->traverse_path_waiting_status_ = RccTx::WAITING_NO_DEADLOCK;
            parent.log_apply_finished_.WaitUntilGreaterOrEqualThan(1);
//            parent.status_.WaitUntilGreaterOrEqualThan(TXN_CMT);
            self.traverse_path_start_ = vertex;
            vertex->traverse_path_waitingon_ = nullptr ;
            vertex->traverse_path_waiting_status_ = RccTx::TRAVERSING;
#ifdef DEBUG_CHECK
            xxxxxyyyyy = nullptr;
            xxxxx = RccTx::TRAVERSING;
//            self.__debug_search_path_init_txid_ = vertex->id();
#endif
            return RccGraph::SearchHint::Skip;
          }
        }
//        if (parent.waiting_all_anc_committing_) {
//          // already in a search.
//          verify(!parent.all_anc_cmt_hint);
//          verify(parent.traverse_path_start_ != nullptr);
//          if (parent.traverse_path_start_->traverse_path_waitingon_ == nullptr) {
//            // in my own search
//#ifdef DEBUG_CHECK
//            verify(!parent.all_anc_cmt_hint);
//            verify(parent.traverse_path_start_ == vertex);
////            verify(parent.__debug_random_number_ == rno);
//            verify(self_searched_set->count(&parent) > 0);
//            verify(walked_set.count(&parent) > 0);
//#endif
//            return RccGraph::SearchHint::Skip;
//          } else {
//            // find ultimate waiting on and do cycle detection
//            auto& waitstatus = parent.traverse_path_start_->traverse_path_waiting_status_;
//            if (waitstatus == RccTx::WAITING_NO_DEADLOCK) {
//              // no possible deadlock and can wait in relax
//#ifdef DEBUG_CHECK
//              xxxxx = RccTx::WAITING_POSSIBLE_DEADLOCK;
//              xxxxxyyyyy = &parent;
//#endif
//              vertex->traverse_path_waiting_status_ = RccTx::WAITING_POSSIBLE_DEADLOCK;
//              vertex->traverse_path_waitingon_ = &parent;
//              parent.wait_all_anc_commit_done_.WaitUntilGreaterOrEqualThan(1);
//              self.traverse_path_start_ = vertex;
//              vertex->traverse_path_waiting_status_ = RccTx::TRAVERSING;
//              vertex->traverse_path_waitingon_ = nullptr;
//#ifdef DEBUG_CHECK
//              xxxxx = RccTx::TRAVERSING;
////              self.__debug_search_path_init_txid_ = vertex->id();
//              xxxxxyyyyy = nullptr;
//#endif
//              return RccGraph::SearchHint::Skip;
//            } else if (waitstatus == RccTx::WAITING_POSSIBLE_DEADLOCK) {
//              // posibble deadlock.
//              // TODO for future.
//            } else {
//              verify(0);
//            }
////            bool deadlock = false;
////            auto xwaitingon = parent.traverse_path_start_->traverse_path_waitingon_;
////            do {
////              if (xwaitingon->traverse_path_start_ == vertex) {
//////                 deadlock? break cycle
////                deadlock = true;
////                break;
////              }
//////              if (walked_set.count(xwaitingon) > 0) {
//////                 deadlock? break cycle
//////                deadlock = true;
//////                break;
//////              }
////              if (xwaitingon->traverse_path_start_) {
////                xwaitingon = xwaitingon->traverse_path_start_->traverse_path_waitingon_;
////              } else {
////                break;
////              }
////            } while (xwaitingon != nullptr);
////            if (!deadlock) {
////#ifdef DEBUG_CHECK
////              xxxxx = 2;
////              xxxxxyyyyy = &parent;
////#endif
////              self.traverse_path_start_->traverse_path_waitingon_ = &parent;
////              parent.wait_all_anc_commit_done_.WaitUntilGreaterOrEqualThan(1);
////              vertex->traverse_path_waitingon_ = nullptr;
////              self.traverse_path_start_ = vertex;
////#ifdef DEBUG_CHECK
////              xxxxx = 1;
////              xxxxxyyyyy = nullptr;
////#endif
////              return RccGraph::SearchHint::Skip;
////            } else {
////              // TODO optimize here.
//////              find a way to set all_anc_commit_hit quickly for the cycle.
////            }
//          }
//        }

        int r = RccGraph::SearchHint::Ok;
        if (parent.status() < TXN_CMT) {
//          auto& edge = self.parents_[parent.id()];
//          auto& x = edge.partitions_;
//          if (x.count(self_par_id) > 0) {
//             do nothing
//          } else
          if (parent.Involve(self_par_id)) {
            // do nothing;
          } else {
            if (parent.partition_.empty()) {
              // TODO optimize this part?
              for (auto &pair: self.parents_) {
                if (pair.first == parent.id()) {
                  auto &x = pair.second.partitions_;
                  parent.partition_.insert(x.begin(), x.end());
                  break;
                }
              }
            }
#ifdef DEBUG_CHECK
            verify(!parent.partition_.empty());
            verify(self.traverse_path_start_->traverse_path_waitingon_ == nullptr);
            xxxxx = RccTx::WAITING_NO_DEADLOCK;
            xxxxxyyyyy = &parent;
#endif
            vertex->traverse_path_waitingon_ = &parent;
            verify(self.traverse_path_start_->traverse_path_waitingon_ != nullptr);
            vertex->traverse_path_waiting_status_ = RccTx::WAITING_NO_DEADLOCK;
            InquireAboutIfNeeded(parent);
            vertex->traverse_path_waitingon_ = nullptr;
            self.traverse_path_start_ = vertex;
            vertex->traverse_path_waiting_status_ = RccTx::TRAVERSING;
#ifdef DEBUG_CHECK
//            self.__debug_search_path_init_txid_ = vertex->id();
//            *self.waiting_on_ = nullptr;
            xxxxx = RccTx::TRAVERSING;
            xxxxxyyyyy = nullptr;
#endif
          }
#ifdef DEBUG_CHECK
          xxxxx = RccTx::WAITING_NO_DEADLOCK;
          xxxxxyyyyy = &parent;
#endif
          vertex->traverse_path_waitingon_ = &parent;
          vertex->traverse_path_waiting_status_ = RccTx::WAITING_NO_DEADLOCK;
          parent.status_.WaitUntilGreaterOrEqualThan(TXN_CMT);
          vertex->traverse_path_waitingon_ = nullptr;
          vertex->traverse_path_waiting_status_ = RccTx::TRAVERSING;
          self.traverse_path_start_ = vertex;
#ifdef DEBUG_CHECK
          xxxxx = RccTx::TRAVERSING;
          xxxxxyyyyy = nullptr;
//          *self.waiting_on_ = nullptr;
          parent.__DebugCheckParents();
          verify(parent.status() >= TXN_CMT);
//          self.__debug_search_path_init_txid_ = vertex->id();
#endif
          // XXX debuging
//        } else if (parent.status() >= TXN_DCD) {
//          return RccGraph::SearchHint::Skip;
//        }
        }
        verify(parent.status() >= TXN_CMT);
        return r;
      };
  std::function<void(RccTx&)> func_end = [] (RccTx& v) -> void {
    v.all_anc_cmt_hint = true;
    v.wait_all_anc_commit_done_.Set(1);
  };
  TraversePredNonRecursive(*vertex, func, func_end);
  vertex->wait_all_anc_commit_done_.Set(1);
#ifdef DEBUG_CHECK
//  xxxxx = 3;
//  auto func2 =
//      [this, searched_set](RccTx& self, RccTx& parent, unordered_set<RccTx*> walked_set) -> int {
////        verify(searched_set->count(&parent) > 0);
//        if (parent.HasLogApplyStarted() || parent.IsDecided() || parent.all_ancestors_committing()) {
//          return RccGraph::SearchHint::Skip;
//        }
////        verify(parent.status() >= TXN_CMT);
//        int r = RccGraph::SearchHint::Ok;
//        return r;
//      };
//  TraversePredNonRecursive(*vertex, func2, [] (RccTx& v) -> void {;});
#endif
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
  bool r = false;
  for (auto v : scc) {
    r = v->status() >= TXN_DCD;
    if (r) break;
  }
  if (r) {
    for (auto v : scc) {
      verify(v->status() >= TXN_DCD);
    }
  }
  for (auto v : scc) {
    verify(v->status() >= TXN_CMT);
    verify(v->all_anc_cmt_hint);
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
void RccServer::WaitNonSccParentsExecuted(const janus::RccScc& scc1){
  verify(scc1.size() > 0);
  auto scc = scc1;
  for (auto& v: scc) {
    if (v->all_nonscc_parents_executed_hint) {
      continue;
    }
    auto parents = v->parents_;
    for (auto& pair: parents) {
      auto& parent = *FindOrCreateParentVPtr(*v, pair.first, pair.second);
      if (parent.Involve(partition_id_)) {
        if (parent.scc_ != scc[0]->scc_) {
          parent.log_apply_finished_.WaitUntilGreaterOrEqualThan(1);
        }
      }
    }
    v->all_nonscc_parents_executed_hint = true;
  }
}

void RccServer::WaitUntilAllPredSccExecuted(const RccScc& scc) {
  verify(scc.size() > 0);
  set<RccTx*> scc_set;
  scc_set.insert(scc.begin(), scc.end());
  auto func =
      [&scc_set, &scc, this](RccTx& self, RccTx& tx, unordered_set<RccTx*>&) -> int {
        if (scc_set.find(&tx) != scc_set.end()) {
          // belong to the scc
          verify (tx.IsDecided());
          return RccGraph::SearchHint::Ok;
        }
        if (tx.log_apply_finished_.value_ >= 1) {
          return RccGraph::SearchHint::Skip;
        }
        if (!tx.Involve(partition_id_)) {
          verify(tx.status() >= TXN_CMT);
          return RccGraph::SearchHint::Skip;
        }
        tx.log_apply_finished_.WaitUntilGreaterOrEqualThan(1);
        return RccGraph::SearchHint::Skip;
        verify(0);
      };
  TraversePredNonRecursive(*scc[0], func);
}

bool RccServer::AllAncFns(const RccScc& scc) {
  verify(0);
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

void RccServer::Execute(RccScc& scc) {
  verify(scc.size() > 0);
  for (auto v : scc) {
    auto sp_tx = FindV(v->id());
    Execute(sp_tx);
  }
}

void RccServer::Execute(shared_ptr<RccTx> sp_tx) {
  if (sp_tx->log_apply_started_) {
    sp_tx->log_apply_finished_.WaitUntilGreaterOrEqualThan(1);
    return;
  }
  verify(sp_tx->all_anc_cmt_hint);
  sp_tx->log_apply_started_ = true;
  Log_debug("executing dtxn id %" PRIx64, sp_tx->id());
  verify(sp_tx->IsDecided());
  if (sp_tx->Involve(partition_id_)) {
    sp_tx->commit_received_.WaitUntilGreaterOrEqualThan(1);
//    Coroutine::CreateRun([sp_tx, this]() {
//      if (sp_tx->need_validation_) {
//        sp_tx->CommitValidate();
//      } else {
        sp_tx->local_validated_->Set(SUCCESS);
//      }
//      commo()->BroadcastValidation(sp_tx->id(), sp_tx->partition_,
//          sp_tx->local_validation_result_);
      sp_tx->sp_ev_commit_->Set(1);
      // TODO recover this?
//      sp_tx->global_validated_->Wait();
//      verify(sp_tx->global_validated_->status_ != Event::TIMEOUT);
      sp_tx->CommitExecute();
//      sp_tx->local_validated_->Get();
//    });
  } else {
    // a tmp solution
    sp_tx->__debug_local_validated_foreign_ = true;
    sp_tx->local_validated_->Set(SUCCESS);
  }
  sp_tx->phase_ = PHASE_RCC_COMMIT;
  sp_tx->log_apply_finished_.Set(1);
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
  verify(dtxn->__commit_received_);
  dtxn->local_validated_->Wait(400*1000*1000);
  if (dtxn->local_validated_->status_ == Event::TIMEOUT) {
    verify(dtxn->status()>=TXN_CMT);
    verify(0);
    ret = -1; //TODO come back and remove this after the correctness checker.
  } else {
    ret = dtxn->local_validated_->Get();
    verify(ret == SUCCESS || ret == REJECT);
    verify(ret != REJECT);
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

int RccServer::OnPreAccept(txnid_t txn_id,
                           rank_t rank,
                           const vector<SimpleCommand> &cmds,
                           parent_set_t& res_parents) {
//  Log_info("on preaccept: %llx par: %d", txn_id, (int)partition_id_);
//  if (RandomGenerator::rand(1, 2000) <= 1)
//    Log_info("on pre-accept graph size: %d", graph.size());
  verify(txn_id > 0);
  verify(cmds[0].root_id_ == txn_id);
  verify(rank == RANK_I || rank == RANK_D);
  auto dtxn = dynamic_pointer_cast<RccTx>(GetOrCreateTx(txn_id));
  verify(dtxn->current_rank_ == rank);
  dtxn->UpdateStatus(TXN_PAC);
  dtxn->involve_flag_ = RccTx::INVOLVED;
  if (dtxn->max_seen_ballot_ > 0) {
    return REJECT;
  }
  // assuming an ordered message queue and inquire depth always as 1.
//  verify(dtxn->status() < TXN_CMT); TODO re-enable this check.
  if (dtxn->status() < TXN_CMT) {
    if (dtxn->phase_ <= PHASE_RCC_DISPATCH) {
      for (auto &c: cmds) {
        map<int32_t, Value> output;
        dtxn->DispatchExecute(const_cast<SimpleCommand &>(c), &output); // track dependencies
      }
    }
  } else {
    if (dtxn->dreqs_.size() == 0) {
      for (auto &c: cmds) {
        dtxn->dreqs_.push_back(c);
      }
    }
  }
  verify(!dtxn->fully_dispatched_->value_);
  dtxn->fully_dispatched_->Set(1);
  res_parents = dtxn->parents_;
  return SUCCESS;
}

int RccServer::OnAccept(const txnid_t txn_id,
                        const rank_t rank,
                        const ballot_t &ballot,
                        const parent_set_t& parents) {
  std::lock_guard<std::recursive_mutex> lock(mtx_);
  auto dtxn = dynamic_pointer_cast<RccTx>(GetOrCreateTx(txn_id));
//  verify(dtxn->current_rank_ == rank);
  if (dtxn->max_seen_ballot_ > ballot) {
    verify(0); // do not support failure recovery at the moment.
    return REJECT;
  } else {
    dtxn->max_seen_ballot_ = ballot;
    dtxn->max_accepted_ballot_ = ballot;
    ReplaceParentEdges<RccTx>(dtxn->parents_,
        const_cast<parent_set_t&>(parents));
//    for (auto p : parents) {
//      FindOrCreateV(p.first);
//    }
    return SUCCESS;
  }
}


int RccServer::OnCommit(const txnid_t cmd_id,
                        const rank_t rank,
                        const bool need_validation,
                        const parent_set_t& parents,
                        TxnOutput *output) {
  std::lock_guard<std::recursive_mutex> lock(mtx_);
  Log_debug("committing dtxn %" PRIx64, cmd_id);
//  if (RandomGenerator::rand(1, 2000) <= 1)
//    Log_info("on commit graph size: %d", graph.size());
  auto sp_tx = dynamic_pointer_cast<RccTx>(GetOrCreateTx(cmd_id));
//  verify(rank == dtxn->current_rank_);
//  verify(sp_tx->p_output_reply_ == nullptr);
//  sp_tx->p_output_reply_ = output;
  verify(!sp_tx->IsAborted());
  sp_tx->__commit_received_ = true;
  bool weird = sp_tx->HasLogApplyStarted();
#ifdef DEBUG_CHECK
  Coroutine::CreateRun([sp_tx, weird](){
    auto sp_e = Reactor::CreateSpEvent<Event>();
    sp_e->test_ = [sp_tx] (int v) -> bool {
      return sp_tx->local_validated_->is_set_;
    };
    sp_e->Wait(400 * 1000 * 1000);
    if (sp_e->status_ == Event::TIMEOUT) {
      verify(!weird);
      verify(0);
    }
  });
#endif
  verify(sp_tx->partition_.count(TxLogServer::partition_id_) > 0);
// TODO XXXX whyyyyyyyyyyyyyyy?
//  if (weird) {
//    verify(0);
//    verify(sp_tx->status() > TXN_CMT);
//    sp_tx->__DebugCheckParents();
//    return SUCCESS;
//  }
//  verify (!sp_tx->HasLogApplyStarted());
//  if (sp_tx->HasLogApplyStarted()) {
//    return SUCCESS;
//  }
//    Log_info("on commit: %llx par: %d", cmd_id, (int)partition_id_);
//    dtxn->commit_request_received_ = true;
  verify(sp_tx->Involve(partition_id_));
//  if (sp_tx->status() >= TXN_CMT) {
//    verify(parents == sp_tx->parents_);
//  }
//  for (auto p : parents) {
//    FindOrCreateV(p.first);
//  }
  ReplaceParentEdges(sp_tx->parents_,
      const_cast<parent_set_t&>(parents));
  verify(sp_tx->commit_received_.value_ == 0);
  verify(!sp_tx->__debug_local_validated_foreign_);
  verify(!sp_tx->local_validated_->is_set_);
  sp_tx->commit_received_.Set(1);
  UpgradeStatus(*sp_tx, TXN_CMT);
  sp_tx->__DebugCheckParents();
  WaitUntilAllPredecessorsAtLeastCommitting(sp_tx.get());
  if (sp_tx->scc_->empty()) {
    FindSccPred(*sp_tx);
  }
  sp_tx->__DebugCheckScc();
  RccScc& scc = *sp_tx->scc_;
  verify(sp_tx->Involve(partition_id_));
  Decide(scc);
  WaitNonSccParentsExecuted(scc);
//  WaitUntilAllPredSccExecuted(scc);
//  if (FullyDispatched(scc, rank) && !IsExecuted(scc, rank)) {
//  if (!IsExecuted(scc, rank)) {
  Execute(scc);
  sp_tx->local_validated_->Wait();
//  }
  // TODO verify by a wait time.
//    dtxn->sp_ev_commit_->Wait(1*1000*1000);
//    dtxn->sp_ev_commit_->Wait();
//    verify(dtxn->sp_ev_commit_->status_ != Event::TIMEOUT);
//    ret = dtxn->local_validation_result_ > 0 ? SUCCESS : REJECT;
//  dtxn->CommitRank();
  return sp_tx->local_validated_->Get();
}

} // namespace janus
