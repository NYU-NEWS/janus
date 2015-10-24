#include "all.h"

namespace rococo {

RCCDTxn::RCCDTxn(
    i64 tid,
    DTxnMgr *mgr,
    bool ro
) : DTxn(tid, mgr) {
  tv_ = nullptr;
  read_only_ = ro;
  mdb_txn_ = mgr->get_mdb_txn(tid_);
}

void RCCDTxn::StartLaunch(
    const RequestHeader &header,
    const std::vector<mdb::Value> &input,
    ChopStartResponse *res,
    rrr::DeferredReply *defer) {
  verify(defer);
  static bool do_record = Config::GetConfig()->do_logging();
  if (do_record) {
    Marshal m;
    m << header;
    m << input;
    auto job = [&header, &input, res, defer, this]() {
      this->StartAfterLog(header, input, res, defer);
    };
    recorder_->submit(m, job);
  } else {
    this->StartAfterLog(header, input, res, defer);
  }
}

void RCCDTxn::StartAfterLog(
    const RequestHeader &header,
    const std::vector<mdb::Value> &input,
    ChopStartResponse *res,
    rrr::DeferredReply *defer
) {
  verify(phase_ <= PHASE_RCC_START);
  Vertex<TxnInfo> *tv = NULL;
  RCCDTxn::dep_s->start_pie(header.tid, &tv);
  if (tv_) verify(tv_ == tv); else tv_ = tv;
  phase_ = PHASE_RCC_START;
  // execute the IR actions.
  auto pair = txn_reg_->get(header.t_type, header.p_type);
  bool deferred = start_exe_itfr(
      pair.defer, pair.txn_handler, header, input, &res->output);
  // Reply
  res->is_defered = deferred ? 1 : 0;
  auto sz_sub_gra = RCCDTxn::dep_s->sub_txn_graph(header.tid, res->gra_m);
  if (defer) defer->reply();
  // TODO fix stat
  //stat_sz_gra_start_.sample(sz_sub_gra);
  //if (IS_MODE_RO6) {
  //    stat_ro6_sz_vector_.sample(res->read_only.size());
  //}

//    Log::debug("reply to start request. txn_id: %llx, pie_id: %llx, graph size: %d", header.tid, header.pid, (int)res->gra.size());
}

bool RCCDTxn::start_exe_itfr(
    defer_t defer_type,
    TxnHandler &handler,
    const RequestHeader &header,
    const std::vector<mdb::Value> &input,
    std::vector<mdb::Value> *output
) {
  bool deferred;
  switch (defer_type) {
    case DF_NO: { // immediate
      int output_size = 300;
      output->resize(output_size);
      int res;
      handler(this,
              header,
              input.data(),
              input.size(),
              &res,
              output->data(),
              &output_size,
              NULL);
      output->resize(output_size);
      deferred = false;
      break;
    }
    case DF_REAL: { // defer
      DeferredRequest dr;
      dr.header = header;
      dr.inputs = input;
      std::vector<DeferredRequest> &drs = dreqs_;
      if (drs.size() == 0) {
        drs.reserve(100); //XXX
      }
      drs.push_back(dr);
      handler(this,
              header,
              drs.back().inputs.data(),
              drs.back().inputs.size(),
              NULL,
              NULL,
              NULL,
              &drs.back().row_map);
      deferred = true;
      break;
    }
    case DF_FAKE: //TODO
    {
      DeferredRequest dr;
      dr.header = header;
      dr.inputs = input;
      std::vector<DeferredRequest> &drs = dreqs_;
      if (drs.size() == 0) {
        drs.reserve(100); //XXX
      }
      drs.push_back(dr);
      int output_size = 300; //XXX
      output->resize(output_size);
      int res;
      handler(this,
              header,
              drs.back().inputs.data(),
              drs.back().inputs.size(),
              &res,
              output->data(),
              &output_size,
              &drs.back().row_map);
      output->resize(output_size);
      deferred = false;
      break;
    }
    default:
      verify(0);
  }
  return deferred;
}

void RCCDTxn::start(
    const RequestHeader &header,
    const std::vector<mdb::Value> &input,
    bool *deferred,
    ChopStartResponse *res
) {
  // TODO Remove
}

void RCCDTxn::start_ro(
    const RequestHeader &header,
    const std::vector<mdb::Value> &input,
    std::vector<mdb::Value> &output,
    DeferredReply *defer) {

  conflict_txns_.clear();
  auto txn_handler_pair = txn_reg_->get(header.t_type, header.p_type);
  int output_size = 300;
  output.resize(output_size);
  int res;
  phase_ = 1;

  txn_handler_pair.txn_handler(this, header, input.data(), input.size(), &res,
                               output.data(), &output_size, NULL);
  output.resize(output_size);

  // get conflicting transactions
  std::vector<TxnInfo *> &conflict_txns = conflict_txns_;
  // TODO callback: read the value and return.
  std::function<void(void)> cb = [header, /*&input, output, */defer]() {
    defer->reply();
  };
  // wait for them become commit.

  DragonBall *ball = new DragonBall(conflict_txns.size() + 1, cb);

  for (auto tinfo: conflict_txns) {
    tinfo->register_event(TXN_DCD, ball);
  }
  ball->trigger();
}

void RCCDTxn::commit(
    const ChopFinishRequest &req,
    ChopFinishResponse *res,
    rrr::DeferredReply *defer) {
  // union the graph into dep graph
  verify(tv_ != nullptr);
  RCCDTxn::dep_s->txn_gra_.Aggregate(req.gra, true);
  Graph<TxnInfo> &txn_gra_ = RCCDTxn::dep_s->txn_gra_;

  tv_->data_->res = res;
  tv_->data_->union_status(TXN_CMT);

  to_decide(tv_, defer);
}

void RCCDTxn::to_decide(
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
  RCCDTxn::dep_s->find_txn_anc_opt(v, anc);
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

void RCCDTxn::commit_anc_finish(
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
  RCCDTxn::dep_s->find_txn_scc_nearest_anc(v, scc_anc);

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

void RCCDTxn::commit_scc_anc_commit(
    Vertex<TxnInfo> *v,
    rrr::DeferredReply *defer
) {
  Graph<TxnInfo> &txn_gra = RCCDTxn::dep_s->txn_gra_;
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
          auto txn = (RCCDTxn *) mgr_->get(vv->data_->id());
          txn->exe_deferred(vv->data_->res->outputs);
          mgr_->destroy(vv->data_->id());
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

void RCCDTxn::send_ask_req(Vertex<TxnInfo> *av) {
  Graph<TxnInfo> &txn_gra = RCCDTxn::dep_s->txn_gra_;
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
      RococoProxy *proxy = RCCDTxn::dep_s->get_server_proxy(sid);

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

void RCCDTxn::inquire(
    CollectFinishResponse *res,
    rrr::DeferredReply *defer
) {
  std::function<void(void)> callback = [this, res, defer]() {
    // not the entire graph!!!!!
    // should only return a part of the graph.
    RCCDTxn::dep_s->sub_txn_graph(this->tid_, res->gra_m);
    defer->reply();
  };
  DragonBall *ball = new DragonBall(2, callback);
  // TODO Optimize this.
  Vertex<TxnInfo> *v = RCCDTxn::dep_s->txn_gra_.FindV(tid_);
  //register an event, triggered when the status >= COMMITTING;
  verify (v->data_->is_involved());
  v->data_->register_event(TXN_CMT, ball);
  ball->trigger();
}

void RCCDTxn::exe_deferred(
    std::vector<std::pair<RequestHeader, std::vector<mdb::Value> > >
    &outputs) {

  verify(phase_ == 1);
  phase_ = 2;
  if (dreqs_.size() == 0) {
    // this tid does not need deferred execution.
    //verify(0);
  } else {
    // delayed execution
    outputs.clear(); // FIXME does this help? seems it does, why?
    for (auto &req: dreqs_) {
      auto &header = req.header;
      auto &input = req.inputs;
      auto txn_handler_pair = txn_reg_->get(header.t_type, header.p_type);
      verify(header.tid == tid_);

      std::vector<Value> output;
      int output_size = 300;
      output.resize(output_size);
      int res;

      txn_handler_pair.txn_handler(this, header, input.data(), input.size(),
                                   &res, output.data(), &output_size, &req.row_map);
      if (header.p_type == TPCC_PAYMENT_4
          && header.t_type == TPCC_PAYMENT)
        verify(output_size == 15);
      output.resize(output_size);

      // FIXME. what the fuck happens here?
      auto pp = std::make_pair(header, output);
      outputs.push_back(pp);
    }
  }
}


void RCCDTxn::kiss(mdb::Row *r, int col, bool immediate) {

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
