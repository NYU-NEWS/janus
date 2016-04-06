

#include "graph.h"
#include "txn-info.h"
#include "sched.h"
#include "dtxn.h"
#include "commo.h"
#include "waitlist_checker.h"
#include "frame.h"

namespace rococo {

RccSched::RccSched() : Scheduler(), waitlist_(), mtx_() {
  verify(dep_graph_ == nullptr);
  dep_graph_ = new RccGraph();
  dep_graph_->partition_id_ = partition_id_;
  waitlist_checker_ = new WaitlistChecker(this);
}

RccSched::~RccSched() {
  verify(waitlist_checker_);
  delete waitlist_checker_;
}

int RccSched::OnDispatch(const SimpleCommand &cmd,
                         int32_t *res,
                         map<int32_t, Value> *output,
                         RccGraph *graph,
                         const function<void()> &callback) {
  RccDTxn *dtxn = (RccDTxn *) GetOrCreateDTxn(cmd.root_id_);
  dep_graph_->FindOrCreateTxnInfo(cmd.root_id_, &dtxn->tv_);
  verify(dep_graph_->partition_id_ == partition_id_);

  auto job = [&cmd, res, dtxn, callback, graph, output, this]() {
    verify(cmd.partition_id_ == this->partition_id_);
    dtxn->DispatchExecute(cmd, res, output);
    dtxn->UpdateStatus(TXN_STD);
    auto sz = dep_graph_->MinItfrGraph(cmd.root_id_, graph);
    TxnInfo& info1 = *dep_graph_->vertex_index_.at(cmd.root_id_)->data_;
    TxnInfo& info2 = *graph->vertex_index_.at(cmd.root_id_)->data_;
    verify(info1.partition_.find(cmd.partition_id_) != info1.partition_.end());
    verify(info2.partition_.find(cmd.partition_id_) != info2.partition_.end());
    verify(sz > 0);
    callback();
  };

  static bool do_record = Config::GetConfig()->do_logging();
  if (do_record) {
    Marshal m;
    m << cmd;
    recorder_->submit(m, job);
  } else {
    job();
  }
}

int RccSched::OnCommit(cmdid_t cmd_id,
                       const RccGraph &graph,
                       TxnOutput* output,
                       const function<void()> &callback) {
  std::lock_guard<std::recursive_mutex> lock(mtx_);
  // union the graph into dep graph
  RccDTxn *dtxn = (RccDTxn*) GetDTxn(cmd_id);
  verify(dtxn != nullptr);
  dep_graph_->Aggregate(const_cast<RccGraph&>(graph));
  for (auto& pair: graph.vertex_index_) {
    // TODO optimize here.
    auto txnid = pair.first;
    auto v = dep_graph_->FindV(txnid);
    verify(v != nullptr);
    waitlist_.push_back(v);
  }
  verify(dtxn->outputs_ == nullptr);
  dtxn->outputs_ = output;
  dtxn->finish_ok_callback_ = callback;
//  Graph<TxnInfo> &txn_gra_ = RccDTxn::dep_s->txn_gra_;
//  tv_->data_->res = res;
//  tv_->data_->union_status(TXN_CMT);

  CheckWaitlist();
}

int RccSched::OnInquire(cmdid_t cmd_id,
                        RccGraph *graph,
                        const function<void()> &callback) {
//  DragonBall *ball = new DragonBall(2, [this, cmd_id, callback, graph] () {
//    dep_graph_->MinItfrGraph(cmd_id, graph);
//    callback();
//  });
  RccVertex* v = dep_graph_->FindV(cmd_id);
  TxnInfo& info = *v->data_;
  //register an event, triggered when the status >= COMMITTING;
  verify (info.Involve(partition_id_));

  if (info.IsCommitting()) {
    dep_graph_->MinItfrGraph(cmd_id, graph);
    callback();
  } else {
    info.graphs_for_inquire_.push_back(graph);
    info.callbacks_for_inquire_.push_back(callback);
    verify(info.graphs_for_inquire_.size() ==
        info.callbacks_for_inquire_.size());
  }

//  v->data_->register_event(TXN_CMT, ball);
//  ball->trigger();
}

void RccSched::CheckWaitlist() {
  std::lock_guard<std::recursive_mutex> lock(mtx_);
  bool check_again = false;
  auto it = waitlist_.begin();
  Log_debug("waitlist length: %d", (int)waitlist_.size());
  while (it != waitlist_.end()) {
    RccVertex* v = *it;
    verify(v != nullptr);
//  for (RccVertex *v : waitlist_) {
    // TODO minimize the length of the waitlist.
    TxnInfo& tinfo = *(v->data_);
    // reply inquire requests if possible.
    verify(tinfo.graphs_for_inquire_.size() ==
        tinfo.callbacks_for_inquire_.size());
    if (tinfo.IsCommitting() && tinfo.graphs_for_inquire_.size() > 0) {
      for (auto graph : tinfo.graphs_for_inquire_) {
        dep_graph_->MinItfrGraph(tinfo.id(), graph);
      }
      for (auto callback : tinfo.callbacks_for_inquire_) {
        callback();
      }
      tinfo.callbacks_for_inquire_.clear();
      tinfo.graphs_for_inquire_.clear();
    }
    // inquire about unknown transaction.
    if (tinfo.status() <= TXN_STD &&
        !tinfo.Involve(partition_id_) &&
        !tinfo.during_asking) {
      InquireAbout(v);
    } else if (tinfo.status() >= TXN_CMT && tinfo.status() < TXN_DCD) {
      if (AllAncCmt(v)) {
        check_again = true;
        Decide(dep_graph_->FindSCC(v));
      } else {
        // else do nothing
        auto anc_v = __DebugFindAnOngoingAncestor(v);
        TxnInfo& tinfo = *anc_v->data_;
        Log_debug("this transaction has some ongoing ancestors, tid: %llx, "
                      "is_involved: %d, during_inquire: %d, inquire_acked: %d",
                  tinfo.id(),
                  tinfo.Involve(partition_id_),
                  tinfo.during_asking,
                  tinfo.inquire_acked_);
      }
    } // else do nothing

    if (tinfo.status() >= TXN_DCD &&
        !tinfo.IsExecuted() &&
        AllAncFns(dep_graph_->FindSCC(v))) {
      check_again = true;
      Execute(dep_graph_->FindSCC(v));
    } // else do nothing

    // Adjust the waitlist.
    if (tinfo.IsExecuted() ||
        (tinfo.IsDecided() && !tinfo.Involve(partition_id_))) {
      // check for its descendants, perhaps add redundant vertex here.
//      for (auto child_pair : v->outgoing_) {
//        auto child_v = child_pair.first;
//        waitlist_.push_back(child_v);
//      }
      it = waitlist_.erase(it);
    } else {
      it++;
    }
  }
  if (check_again)
    CheckWaitlist();
  else
    __DebugExamineWaitlist();
  // TODO optimize for the waitlist.
}

void RccSched::__DebugExamineWaitlist() {
  std::lock_guard<std::recursive_mutex> lock(mtx_);
  set<RccVertex*> vertexes;
  for (auto v : waitlist_) {
    auto& tinfo = *v->data_;
    auto pair = vertexes.insert(v);
    if (!pair.second) {
      Log_debug("duplicated vertex in waitlist");
    }
    if (tinfo.status() >= TXN_CMT && AllAncCmt(v)) {
      verify(!tinfo.IsExecuted());
      verify(tinfo.IsDecided());
    }
  }
}
//
//void RccSched::to_decide(Vertex<TxnInfo> *v,
//                         rrr::DeferredReply *defer) {
//  TxnInfo &tinfo = *(v->data_);
//  if (tinfo.during_commit) {
//    Log_debug("this txn is already during commit! tid: %llx", tinfo.id());
//    return;
//  } else {
//    tinfo.during_commit = true;
//  }
//  std::unordered_set<Vertex<TxnInfo> *> anc;
//  dep_graph_->find_txn_anc_opt(v, anc);
//  std::function<void(void)> anc_finish_cb =
//      [this, v, defer]() {
//        this->dep_graph_->commit_anc_finish(v, defer);
//      };
//  DragonBall *wait_finish_ball =
//      new DragonBall(anc.size() + 1, anc_finish_cb);
//  for (auto &av: anc) {
//    Log::debug("\t ancestor id: %llx", av->data_->id());
//    av->data_->register_event(TXN_CMT, wait_finish_ball);
//    InquireAbout(av);
//  }
//  wait_finish_ball->trigger();
//}

void RccSched::InquireAbout(Vertex<TxnInfo> *av) {
//  Graph<TxnInfo> &txn_gra = dep_graph_->txn_gra_;
  TxnInfo &tinfo = *(av->data_);
  verify(!tinfo.Involve(partition_id_));
  verify(!tinfo.during_asking);
  parid_t par_id = *(tinfo.partition_.begin());
  tinfo.during_asking = true;
  commo()->SendInquire(par_id,
                       tinfo.txn_id_,
                       std::bind(&RccSched::InquireAck,
                                 this,
                                 tinfo.id(),
                                 std::placeholders::_1));
//  if (!tinfo.is_involved()) {
//    if (tinfo.is_commit()) {
//      Log_debug("observed commited unrelated txn, id: %llx", tinfo.id());
//    } else if (tinfo.is_finish()) {
//      Log_debug("observed finished unrelated txn, id: %llx", tinfo.id());
//      to_decide(av, nullptr);
//    } else if (tinfo.during_asking) {
//      // don't have to ask twice
//      Log_debug("observed in-asking unrealted txn, id: %llx", tinfo.id());
//    } else {
//      static int64_t sample = 0;
//      int32_t sid = tinfo.random_server();
//      RococoProxy *proxy = RccDTxn::dep_s->get_server_proxy(sid);
//
//      rrr::FutureAttr fuattr;
//      fuattr.callback = [this, av, &txn_gra](Future *fu) {
//        // std::lock_guard<std::mutex> guard(this->mtx_);
//        int e = fu->get_error_code();
//        if (e != 0) {
//          Log_info("connection failed: e: %d =%s", e, strerror(e));
//          verify(0);
//        }
//        Log_debug("got finish request for this txn, it is not related"
//                       " to this server tid: %llx.", av->data_->id());
//
//        CollectFinishResponse res;
//        fu->get_reply() >> res;
//
//        //stat_sz_gra_ask_.sample(res.gra_m.gra->size());
//        // Be careful! this one could bring more evil than we want.
//        txn_gra.Aggregate(*(res.gra_m.gra), true);
//        // for every transaction it unions,  handle this transaction
//        // like normal finish workflow.
//        // FIXME is there problem here?
//        to_decide(av, nullptr);
//      };
//      Log_debug("observed uncommitted unrelated txn, tid: %llx, related"
//                     " server id: %x", tinfo.id(), sid);
//      tinfo.during_asking = true;
//      //stat_n_ask_.sample();
//      Future *f1 = proxy->async_rcc_ask_txn(tinfo.txn_id_, fuattr);
//      verify(f1 != nullptr);
//      Future::safe_release(f1);
//      // verify(av->data_.is_involved());
//      // n_asking_ ++;
//    }
//  } else {
//    // This txn belongs to me, sooner or later I'll receive the finish request.
//  }
}

void RccSched::InquireAck(cmdid_t cmd_id, RccGraph& graph) {
  std::lock_guard<std::recursive_mutex> lock(mtx_);
  dep_graph_->Aggregate(const_cast<RccGraph&>(graph));
  auto v = dep_graph_->FindV(cmd_id);
  TxnInfo& tinfo = *v->data_;
  verify(tinfo.IsCommitting());
  tinfo.inquire_acked_ = true;
  for (auto& pair: graph.vertex_index_) {
    auto v = dep_graph_->FindV(pair.first);
    verify(v != nullptr);
    waitlist_.push_back(v);
  }
//  Graph<TxnInfo> &txn_gra_ = RccDTxn::dep_s->txn_gra_;
//  tv_->data_->res = res;
//  tv_->data_->union_status(TXN_CMT);
  CheckWaitlist();
}

RccVertex* RccSched::__DebugFindAnOngoingAncestor(RccVertex* vertex) {
  RccVertex* ret = nullptr;
  set<RccVertex*> walked;
  std::function<bool(RccVertex*)> func = [&ret, vertex] (RccVertex* v) -> bool {
    TxnInfo& info = *v->data_;
    if (info.status() >= TXN_CMT) {
      return true;
    } else {
      ret = v;
      return false; // stop traversing.
    }
  };
  dep_graph_->TraversePred(vertex, -1, func, walked);
  return ret;
}

bool RccSched::AllAncCmt(RccVertex *vertex) {
  set<RccVertex*> walked;
  bool ret = true;
  std::function<bool(RccVertex*)> func = [&ret] (RccVertex* v) -> bool {
    TxnInfo& info = *v->data_;
    if (info.status() >= TXN_CMT) {
      return true;
    } else {
      ret = false;
      return false;
    }
  };
  dep_graph_->TraversePred(vertex, -1, func, walked);
  return ret;
}

void RccSched::Decide(const RccScc& scc) {
  for (auto v : scc) {
    TxnInfo& info = *v->data_;
    info.union_status(TXN_DCD);
  }
}

bool RccSched::AllAncFns(const RccScc& scc) {
  verify(scc.size() > 0);
  set<RccVertex*> scc_set;
  scc_set.insert(scc.begin(), scc.end());
  set<RccVertex*> walked;
  bool ret = true;
  std::function<bool(RccVertex*)> func =
      [&ret, &scc_set] (RccVertex* v) -> bool {
    TxnInfo& info = *v->data_;
    if (info.status() >= TXN_DCD) {
      return true;
    } else if (scc_set.find(v) != scc_set.end()) {
      return true;
    } else {
      ret = false;
      return false; // abort traverse
    }
  };
  dep_graph_->TraversePred(scc[0], -1, func, walked);
  return ret;
};

void RccSched::Execute(const RccScc& scc) {
  verify(scc.size() > 0);
  for (auto v : scc) {
    TxnInfo& info = *(v->data_);
    info.executed_ = true;
    RccDTxn *dtxn = (RccDTxn *) GetDTxn(info.id());
    if (dtxn == nullptr) continue;
    if (info.Involve(partition_id_)) {
      dtxn->CommitExecute();
      dtxn->ReplyFinishOk();
    }
  }
}

RccCommo* RccSched::commo() {
  if (commo_ == nullptr) {
    commo_ = dynamic_cast<RccCommo*>(frame_->CreateCommo());
  }
  verify(commo_ != nullptr);
  return commo_;
}


} // namespace rococo