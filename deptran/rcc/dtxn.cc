#include "../__dep__.h"
#include "../scheduler.h"
#include "dtxn.h"
#include "bench/tpcc/piece.h"

namespace rococo {

RccDTxn::RccDTxn(
    i64 tid,
    Scheduler *mgr,
    bool ro) : DTxn(tid, mgr) {
  tv_ = nullptr;
  read_only_ = ro;
  mdb_txn_ = mgr->GetOrCreateMTxn(tid_);
}

void RccDTxn::StartLaunch(const SimpleCommand& cmd,
                          ChopStartResponse *res,
                          rrr::DeferredReply *defer) {
  verify(defer);
  static bool do_record = Config::GetConfig()->do_logging();
  if (do_record) {
    Marshal m;
    m << cmd;
    auto job = [&cmd, res, defer, this]() {
      this->StartAfterLog(cmd, res, defer);
    };
    recorder_->submit(m, job);
  } else {
    this->StartAfterLog(cmd, res, defer);
  }
}

void RccDTxn::StartAfterLog(const SimpleCommand& cmd,
                            ChopStartResponse *res,
                            rrr::DeferredReply *defer) {
  verify(phase_ <= PHASE_RCC_START);
  Vertex<TxnInfo> *tv = NULL;
  RccDTxn::dep_s->start_pie(cmd.root_id_, &tv);
  if (tv_) verify(tv_ == tv); else tv_ = tv;
  phase_ = PHASE_RCC_START;
  // execute the IR actions.
  auto pair = txn_reg_->get(cmd.root_type_, cmd.type_);
  bool deferred = start_exe_itfr(pair.defer,
                                 pair.txn_handler,
                                 cmd,
                                 &res->output);
  // Reply
  res->is_defered = deferred ? 1 : 0;
  auto sz_sub_gra = RccDTxn::dep_s->sub_txn_graph(cmd.id_, res->gra_m);
  if (defer) defer->reply();
  // TODO fix stat
  //stat_sz_gra_start_.sample(sz_sub_gra);
  //if (IS_MODE_RO6) {
  //    stat_ro6_sz_vector_.sample(res->read_only.size());
  //}

//    Log::debug("reply to start request. txn_id: %llx, pie_id: %llx, graph size: %d", header.tid, header.pid, (int)res->gra.size());
}

bool RccDTxn::start_exe_itfr(defer_t defer_type,
                             TxnHandler &handler,
                             const SimpleCommand& cmd,
                             map<int32_t, Value> *output) {
  bool deferred;
  switch (defer_type) {
    case DF_NO: { // immediate
      int res;
      // TODO fix
      handler(nullptr,
              this,
              const_cast<SimpleCommand&>(cmd),
              &res,
              *output);
      deferred = false;
      break;
    }
    case DF_REAL: { // defer
      dreqs_.push_back(cmd);
      map<int32_t, Value> no_use;
      handler(nullptr,
              this,
              const_cast<SimpleCommand&>(cmd),
              NULL,
              no_use);
      deferred = true;
      break;
    }
    case DF_FAKE: //TODO
    {
      dreqs_.push_back(cmd);
      int output_size = 300; //XXX
      int res;
      handler(nullptr,
              this,
              const_cast<SimpleCommand&>(cmd),
              &res,
              *output);
      deferred = false;
      break;
    }
    default:
      verify(0);
  }
  return deferred;
}

void RccDTxn::start(
    const RequestHeader &header,
    const std::vector<mdb::Value> &input,
    bool *deferred,
    ChopStartResponse *res
) {
  // TODO Remove
}

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

void RccDTxn::commit(
    const ChopFinishRequest &req,
    ChopFinishResponse *res,
    rrr::DeferredReply *defer) {
  // union the graph into dep graph
  verify(tv_ != nullptr);
  RccDTxn::dep_s->txn_gra_.Aggregate(req.gra, true);
  Graph<TxnInfo> &txn_gra_ = RccDTxn::dep_s->txn_gra_;

  tv_->data_->res = res;
  tv_->data_->union_status(TXN_CMT);

  to_decide(tv_, defer);
}

void RccDTxn::to_decide(
    Vertex<TxnInfo> *v,
    rrr::DeferredReply *defer
) {
  TxnInfo &tinfo = *(v->data_);
  if (tinfo.during_commit) {
    Log::debug("this txn is already during commit! tid: %llx", tinfo.id());
    return;
  } else {
    tinfo.during_commit = true;
  }
  std::unordered_set<Vertex<TxnInfo> *> anc;
  RccDTxn::dep_s->find_txn_anc_opt(v, anc);
  std::function<void(void)> anc_finish_cb =
      [this, v, defer]() {
        this->commit_anc_finish(v, defer);
      };
  DragonBall *wait_finish_ball =
      new DragonBall(anc.size() + 1, anc_finish_cb);
  for (auto &av: anc) {
    Log::debug("\t ancestor id: %llx", av->data_->id());
    av->data_->register_event(TXN_CMT, wait_finish_ball);
    send_ask_req(av);
  }
  wait_finish_ball->trigger();
}

void RccDTxn::commit_anc_finish(
    Vertex<TxnInfo> *v,
    rrr::DeferredReply *defer
) {
  std::function<void(void)> scc_anc_commit_cb = [v, defer, this]() {
    this->commit_scc_anc_commit(v, defer);
  };
  Log::debug("all ancestors have finished for txn id: %llx", v->data_->id());
  // after all ancestors become COMMITTING
  // wait for all ancestors of scc become DECIDED
  std::set<Vertex<TxnInfo> *> scc_anc;
  RccDTxn::dep_s->find_txn_scc_nearest_anc(v, scc_anc);

  DragonBall *wait_commit_ball = new DragonBall(scc_anc.size() + 1,
                                                scc_anc_commit_cb);
  for (auto &sav: scc_anc) {
    //Log::debug("\t ancestor id: %llx", sav->data_.id());
    // what if the ancestor is not related and not committed or not finished?
    sav->data_->register_event(TXN_DCD, wait_commit_ball);
    send_ask_req(sav);
  }
  wait_commit_ball->trigger();
}

void RccDTxn::commit_scc_anc_commit(
    Vertex<TxnInfo> *v,
    rrr::DeferredReply *defer
) {
  Graph<TxnInfo> &txn_gra = RccDTxn::dep_s->txn_gra_;
  uint64_t txn_id = v->data_->id();
  Log::debug("all scc ancestors have committed for txn id: %llx", txn_id);
  // after all scc ancestors become DECIDED
  // sort, and commit.
  TxnInfo &tinfo = *v->data_;
  if (tinfo.is_commit()) { ;
  } else {
    std::vector<Vertex<TxnInfo> *> sscc;
    txn_gra.FindSortedSCC(v, &sscc);
    //static int64_t sample = 0;
    //if (RandomGenerator::rand(1, 100)==1) {
    //    scsi_->do_statistics(S_RES_KEY_N_SCC, sscc.size());
    //}
    //this->stat_sz_scc_.sample(sscc.size());
    for (auto &vv: sscc) {
      bool commit_by_other = vv->data_->get_status() & TXN_DCD;
      if (!commit_by_other) {
        // apply changes.

        // this res may not be mine !!!!
        if (vv->data_->res != nullptr) {
          auto txn = (RccDTxn *) sched_->GetDTxn(vv->data_->id());
          txn->exe_deferred(vv->data_->res->outputs);
          sched_->DestroyDTxn(vv->data_->id());
        }

        Log::debug("txn commit. tid:%llx", vv->data_->id());
        // delay return back to clients.
        //
        verify(vv->data_->committed_ == false);
        vv->data_->committed_ = true;
        vv->data_->union_status(TXN_DCD, false);
      }
    }
    for (auto &vv: sscc) {
      vv->data_->trigger();
    }
  }

  if (defer != nullptr) {
    //if (commit_by_other) {
    //    Log::debug("reply finish request of txn: %llx, it's committed by other txn", vv->data_.id());
    //} else {
    Log::debug("reply finish request of txn: %llx", txn_id);
    //}
    defer->reply();
  }
}

void RccDTxn::send_ask_req(Vertex<TxnInfo> *av) {
  Graph<TxnInfo> &txn_gra = RccDTxn::dep_s->txn_gra_;
  TxnInfo &tinfo = *(av->data_);
  if (!tinfo.is_involved()) {

    if (tinfo.is_commit()) {
      Log::debug("observed commited unrelated txn, id: %llx", tinfo.id());
    } else if (tinfo.is_finish()) {
      Log::debug("observed finished unrelated txn, id: %llx", tinfo.id());
      to_decide(av, nullptr);
    } else if (tinfo.during_asking) {
      // don't have to ask twice
      Log::debug("observed in-asking unrealted txn, id: %llx", tinfo.id());
    } else {
      static int64_t sample = 0;
      int32_t sid = tinfo.random_server();
      RococoProxy *proxy = RccDTxn::dep_s->get_server_proxy(sid);

      rrr::FutureAttr fuattr;
      fuattr.callback = [this, av, &txn_gra](Future *fu) {
        // std::lock_guard<std::mutex> guard(this->mtx_);
        int e = fu->get_error_code();
        if (e != 0) {
          Log::info("connection failed: e: %d =%s", e, strerror(e));
          verify(0);
        }
        Log::debug("got finish request for this txn, it is not related"
                       " to this server tid: %llx.", av->data_->id());

        CollectFinishResponse res;
        fu->get_reply() >> res;

        //stat_sz_gra_ask_.sample(res.gra_m.gra->size());
        // Be careful! this one could bring more evil than we want.
        txn_gra.Aggregate(*(res.gra_m.gra), true);
        // for every transaction it unions,  handle this transaction
        // like normal finish workflow.
        // FIXME is there problem here?
        to_decide(av, nullptr);
      };
      Log::debug("observed uncommitted unrelated txn, tid: %llx, related"
                     " server id: %x", tinfo.id(), sid);
      tinfo.during_asking = true;
      //stat_n_ask_.sample();
      Future *f1 = proxy->async_rcc_ask_txn(tinfo.txn_id_, fuattr);
      verify(f1 != nullptr);
      Future::safe_release(f1);
      // verify(av->data_.is_involved());
      // n_asking_ ++;
    }
  } else {
    // This txn belongs to me, sooner or later I'll receive the finish request.
  }

}

void RccDTxn::inquire(
    CollectFinishResponse *res,
    rrr::DeferredReply *defer
) {
  std::function<void(void)> callback = [this, res, defer]() {
    // not the entire graph!!!!!
    // should only return a part of the graph.
    RccDTxn::dep_s->sub_txn_graph(this->tid_, res->gra_m);
    defer->reply();
  };
  DragonBall *ball = new DragonBall(2, callback);
  // TODO Optimize this.
  Vertex<TxnInfo> *v = RccDTxn::dep_s->txn_gra_.FindV(tid_);
  //register an event, triggered when the status >= COMMITTING;
  verify (v->data_->is_involved());
  v->data_->register_event(TXN_CMT, ball);
  ball->trigger();
}

void RccDTxn::exe_deferred(
    std::vector<std::pair<RequestHeader,
                          map<int32_t, Value> > > &outputs) {

  verify(phase_ == 1);
  phase_ = 2;
  if (dreqs_.size() == 0) {
    // this tid does not need deferred execution.
    //verify(0);
  } else {
    // delayed execution
    outputs.clear(); // FIXME does this help? seems it does, why?
    for (auto &cmd: dreqs_) {
      auto txn_handler_pair = txn_reg_->get(cmd.root_type_, cmd.type_);
//      verify(header.tid == tid_);

      map<int32_t, Value> output;
      int output_size = 0;
      int res;
      txn_handler_pair.txn_handler(nullptr,
                                   this,
                                   cmd,
                                   &res,
                                   output);
      if (cmd.type_ == TPCC_PAYMENT_4 && cmd.root_type_ == TPCC_PAYMENT)
        verify(output_size == 15);

      // FIXME. what the fuck happens here?
      // XXX FIXME
      RequestHeader header;
      auto pp = std::make_pair(header, output);
      outputs.push_back(pp);
    }
  }
}


void RccDTxn::kiss(mdb::Row *r, int col, bool immediate) {

  entry_t *entry = ((RCCRow *) r)->get_dep_entry(col);

  if (read_only_) {
    if (entry->last_)
      conflict_txns_.push_back(entry->last_->data_.get());
  } else {
    int8_t edge_type = immediate ? EDGE_I : EDGE_D;
    if (entry->last_ != NULL) {
      entry->last_->outgoing_[tv_] |= edge_type;
      tv_->incoming_[entry->last_] |= edge_type;
    } else {
      entry->last_ = tv_;
    }
  }
}
} // namespace rococo
