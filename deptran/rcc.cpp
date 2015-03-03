#include "all.h"

namespace rococo {

DepGraph *RCCDTxn::dep_s = NULL;

void RCCDTxn::start(
        const RequestHeader &header,
        const std::vector<mdb::Value> &input,
        bool *deferred,
        ChopStartResponse *res ) {

    PieInfo pi;
    pi.pie_id_ = header.pid;
    pi.txn_id_ = header.tid;
    pi.type_ = header.p_type;

    std::vector<mdb::Value> *output = &res->output;

    //std::unordered_map<cell_locator_t, int,
    //                   cell_locator_t_hash> opset; //OP_W, OP_DR, OP_IR
    // get the read and write sets;)
    //lock_oracle(header, input.data(), input.size(), &opset);

    Vertex<PieInfo> *pv = NULL;
    Vertex<TxnInfo> *tv = NULL;
    RCCDTxn::dep_s->start_pie(pi, &pv, &tv);
    tv_ = tv;
    verify(phase_ == 0);
    phase_ = 1;

    // execute the IR actions.
    *deferred = pi.defer_;
    verify(pv && tv);

    auto txn_handler_pair = TxnRegistry::get(header.t_type, header.p_type);

    switch (txn_handler_pair.defer) {
        case DF_NO:
        { // immediate
            int output_size = 300;
            output->resize(output_size);
            int res;
            txn_handler_pair.txn_handler(this,
                    header, input.data(),
                    input.size(), &res, output->data(),
                    &output_size, NULL);
            output->resize(output_size);
            *deferred = false;
            break;
        }
        case DF_REAL:
        { // defer
            DeferredRequest dr;
            dr.header = header;
            dr.inputs = input;
            std::vector<DeferredRequest> &drs = dreqs_;
            if (drs.size() == 0) {
                drs.reserve(100); //XXX
            }
            drs.push_back(dr);
            txn_handler_pair.txn_handler(this,
                    header, drs.back().inputs.data(),
                    drs.back().inputs.size(), NULL, NULL,
                    NULL, &drs.back().row_map);
            *deferred = true;
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
            txn_handler_pair.txn_handler(this,
                    header, drs.back().inputs.data(),
                    drs.back().inputs.size(), &res,
                    output->data(), &output_size,
                    &drs.back().row_map);
            output->resize(output_size);
            *deferred = false;
            break;
        }
        default:
            verify(0);
    }
}

void RCCDTxn::start_ro(
        const RequestHeader &header,
        const std::vector<mdb::Value> &input,
        std::vector<mdb::Value> &output) {

    conflict_txns_.clear();
    auto txn_handler_pair = TxnRegistry::get(header.t_type, header.p_type);
    int output_size = 300;
    output.resize(output_size);
    int res;
    phase_ = 1;

    txn_handler_pair.txn_handler(this, header, input.data(), input.size(), &res,
            output.data(), &output_size, NULL);
    output.resize(output_size);
}

void RCCDTxn::commit(
        const ChopFinishRequest &req,
        ChopFinishResponse* res,
        rrr::DeferredReply *defer) {
    // union the graph into dep graph
    RCCDTxn::dep_s->txn_gra_.union_graph(req.gra, true);

    Graph<TxnInfo> &txn_gra_ = RCCDTxn::dep_s->txn_gra_;
    Vertex<TxnInfo> *v = txn_gra_.find(req.txn_id);

    verify(v != NULL);
    v->data_.res = res;
    v->data_.union_status(TXN_CMT);

    to_decide(v, defer);
}

void RCCDTxn::to_decide(
        Vertex<TxnInfo> *v,
        rrr::DeferredReply* defer) {

    TxnInfo &tinfo = v->data_;
    if (tinfo.during_commit) {
        Log::debug("this txn is already during commit! tid: %llx", tinfo.id());
        return;
    } else {
        tinfo.during_commit = true;
    }

    Graph<TxnInfo> &txn_gra = RCCDTxn::dep_s->txn_gra_;
    // a commit function.

    std::function<void(void)> scc_anc_commit_cb = [&txn_gra, v, defer, this] () {
        uint64_t txn_id = v->data_.id();
        Log::debug("all scc ancestors have committed for txn id: %llx", txn_id);
        // after all scc ancestors become DECIDED
        // sort, and commit.
        TxnInfo &tinfo = v->data_;
        if (tinfo.is_commit()) {
        } else {
            std::vector<Vertex<TxnInfo>*> sscc;
            txn_gra.sorted_scc(v, &sscc);
            //static int64_t sample = 0;
            //if (RandomGenerator::rand(1, 100)==1) {
            //    scsi_->do_statistics(S_RES_KEY_N_SCC, sscc.size());
            //}
            //this->stat_sz_scc_.sample(sscc.size());
            for(auto& vv: sscc) {
                bool commit_by_other = vv->data_.get_status() & TXN_DCD;

                if (!commit_by_other) {
                    // apply changes.

                    // this res may not be mine !!!!
                    if (vv->data_.res != nullptr) {
                        auto txn = (RCCDTxn*) mgr_->get(vv->data_.id());
                        txn->exe_deferred(vv->data_.res->outputs);
                        mgr_->destroy(vv->data_.id());
                    }

                    Log::debug("txn commit. tid:%llx", vv->data_.id());
                    // delay return back to clients.
                    //
                    verify(vv->data_.committed_ == false);
                    vv->data_.committed_ = true;
                    vv->data_.union_status(TXN_DCD, false);
                }
            }

            for (auto& vv: sscc) {
                vv->data_.trigger();
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
    };

    //std::function<void(void)> check_commit_cb = [&txn_gra, v, defer, scc_anc_commit_cb] () {
    //    v->data_.wait_commit_--;
    //    Log::debug("\tan scc ancestor of txn %llx is committed, %d left", v->data_.id(), (int) v->data_.wait_commit_);
    //    if (v->data_.wait_commit_ == 0) {
    //        scc_anc_commit_cb();
    //    }
    //};

    std::function<void(void)> anc_finish_cb =
            [this, &txn_gra, v, scc_anc_commit_cb] () {

                Log::debug("all ancestors have finished for txn id: %llx", v->data_.id());
                // after all ancestors become COMMITTING
                // wait for all ancestors of scc become DECIDED
                std::set<Vertex<TxnInfo>* > scc_anc;
                RCCDTxn::dep_s->find_txn_scc_nearest_anc(v, scc_anc);

                DragonBall *wait_commit_ball = new DragonBall(scc_anc.size() + 1,
                        scc_anc_commit_cb);
                for (auto &sav: scc_anc) {
                    //Log::debug("\t ancestor id: %llx", sav->data_.id());
                    // what if the ancestor is not related and not committed or not finished?
                    sav->data_.register_event(TXN_DCD, wait_commit_ball);
                    send_ask_req(sav);
                }
                wait_commit_ball->trigger();
            };

    std::unordered_set<Vertex<TxnInfo>*> anc;
    RCCDTxn::dep_s->find_txn_anc_opt(v, anc);


    DragonBall *wait_finish_ball = new DragonBall(anc.size() + 1, anc_finish_cb);
    for (auto &av: anc) {
        Log::debug("\t ancestor id: %llx", av->data_.id());
        av->data_.register_event(TXN_CMT, wait_finish_ball);
        send_ask_req(av);
    }
    wait_finish_ball->trigger();
}

void RCCDTxn::send_ask_req(Vertex<TxnInfo>* av) {
    Graph<TxnInfo> &txn_gra = RCCDTxn::dep_s->txn_gra_;
    TxnInfo &tinfo = av->data_;
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
            RococoProxy* proxy = RCCDTxn::dep_s->get_server_proxy(sid);

            rrr::FutureAttr fuattr;
            fuattr.callback = [this, av, &txn_gra] (Future *fu) {
                // std::lock_guard<std::mutex> guard(this->mtx_);
                int e = fu->get_error_code();
                if (e != 0) {
                    Log::info("connection failed: e: %d =%s", e, strerror(e));
                    verify(0);
                }
                Log::debug("got finish request for this txn, it is not related"
                        " to this server tid: %llx.", av->data_.id());

                CollectFinishResponse res;
                fu->get_reply() >> res;

                //stat_sz_gra_ask_.sample(res.gra_m.gra->size());
                // Be careful! this one could bring more evil than we want.
                txn_gra.union_graph(*(res.gra_m.gra), true);
                // for every transaction it unions,  handle this transaction like normal finish workflow.
                // FIXME is there problem here?
                to_decide(av, nullptr);
            };
            Log::debug("observed uncommitted unrelated txn, tid: %llx, related"
                    " server id: %x", tinfo.id(), sid);
            tinfo.during_asking = true;
            //stat_n_ask_.sample();
            Future* f1 = proxy->async_rcc_ask_txn(tinfo.txn_id_, fuattr);
            verify(f1 != nullptr);
            Future::safe_release(f1);
            // verify(av->data_.is_involved());
            // n_asking_ ++;
        }
    } else {
        // This txn belongs to me, sooner or later I'll receive the finish request.
    }

}



void RCCDTxn::exe_deferred(
        std::vector<std::pair<RequestHeader, std::vector<mdb::Value> > >
        &outputs) {
    if (dreqs_.size() == 0) {
        // this tid does not need deferred execution.
        //verify(0);
    } else {
        // delayed execution
        outputs.clear(); // FIXME does this help? seems it does, why?
        for (auto &req: dreqs_) {
            auto &header = req.header;
            auto &input = req.inputs;
            auto txn_handler_pair = TxnRegistry::get(header.t_type, header.p_type);
            verify(header.tid == tid_);

            std::vector<Value> output;
            int output_size = 300;
            output.resize(output_size);
            int res;
            verify(phase_ == 1);
            phase_ = 2;
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

} // namespace rcc
