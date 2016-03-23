

#include "graph.h"
#include "txn-info.h"
#include "sched.h"
#include "dtxn.h"
#include "commo.h"

namespace rococo {

int RccSched::OnHandoutRequest(const SimpleCommand &cmd,
                               int32_t *res,
                               map<int32_t, Value> *output,
                               RccGraph *graph,
                               const function<void()> &callback) {
  RccDTxn *dtxn = (RccDTxn *) GetOrCreateDTxn(cmd.root_id_);
  dep_graph_->FindOrCreateTxnInfo(cmd.root_id_, &dtxn->tv_);

  auto job = [&cmd, res, dtxn, callback, graph, output, this]() {
    dtxn->Execute(cmd, res, output);
    dtxn->UpdateStatus(TXN_STD);
    dep_graph_->MinItfrGraph(cmd.id_, *graph);
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

int RccSched::OnFinishRequest(cmdid_t cmd_id,
                              const RccGraph &graph,
                              map<int32_t, Value> *output,
                              const function<void()> &callback) {
  // union the graph into dep graph
  RccDTxn *dtxn = (RccDTxn*) GetDTxn(cmd_id);
  verify(dtxn != nullptr);
  dep_graph_->Aggregate(const_cast<RccGraph&>(graph));
  for (auto& pair: graph.txn_gra_.vertex_index_) {
    waitlist_.push_back(pair.second);
  }
//  Graph<TxnInfo> &txn_gra_ = RccDTxn::dep_s->txn_gra_;
//  tv_->data_->res = res;
//  tv_->data_->union_status(TXN_CMT);

  CheckWaitlist();
}

int RccSched::OnInquiryRequest(cmdid_t cmd_id,
                               RccGraph *graph,
                               const function<void()> &callback) {

  DragonBall *ball = new DragonBall(2, [this, cmd_id, callback, graph] () {
    dep_graph_->MinItfrGraph(cmd_id, *graph);
    callback();
  });
  // TODO Optimize this.
  Vertex<TxnInfo> *v = dep_graph_->txn_gra_.FindV(cmd_id);
  //register an event, triggered when the status >= COMMITTING;
  verify (v->data_->is_involved());
  v->data_->register_event(TXN_CMT, ball);
  ball->trigger();
}

void RccSched::CheckWaitlist() {
  for (Vertex<TxnInfo> *v : waitlist_) {
    TxnInfo& tinfo = *(v->data_);
    if (tinfo.status() <= TXN_STD &&
        !tinfo.is_involved() &&
        tinfo.during_asking) {
      InquireAbout(v);
    } else if (tinfo.status() == TXN_CMT) {
      if (AllAncCmt(v)) {
        Decide(FindScc(v));
      } // else do nothing
    } // else do nothing

    if (tinfo.status() == TXN_DCD &&
        !tinfo.is_finish() &&
        AllAncFns(FindScc(v))) {
        Execute(FindScc(v));
    } // else do nothing
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
  Graph<TxnInfo> &txn_gra = dep_graph_->txn_gra_;
  TxnInfo &tinfo = *(av->data_);
  verify(!tinfo.is_involved());
  verify(!tinfo.during_asking);
  parid_t par_id = *(tinfo.servers_.begin());
  commo()->SendInquire(par_id,
                       tinfo.txn_id_,
                       std::bind(&RccSched::InquireAck,
                                 this,
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

void RccSched::InquireAck(RccGraph& graph) {
  dep_graph_->Aggregate(const_cast<RccGraph&>(graph));
  for (auto& pair: graph.txn_gra_.vertex_index_) {
    waitlist_.push_back(pair.second);
  }
//  Graph<TxnInfo> &txn_gra_ = RccDTxn::dep_s->txn_gra_;
//  tv_->data_->res = res;
//  tv_->data_->union_status(TXN_CMT);
  CheckWaitlist();
}


} // namespace rococo