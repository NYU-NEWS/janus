#include "all.h"

namespace rcc {

DepGraph *RococoServiceImpl::dep_s = NULL;

RococoServiceImpl::RococoServiceImpl(ServerControlServiceImpl *scsi) : 
    scsi_(scsi) {

#ifdef PIECE_COUNT
    piece_count_timer_.start();
    piece_count_prepare_fail_ = 0;
    piece_count_prepare_success_ = 0;
#endif
    dep_s = new DepGraph();

    if (Config::get_config()->do_logging()) {
        auto path = Config::get_config()->log_path();
        // TODO free this
        recorder_ = new Recorder(path);
    }
}

// deprecated
void RococoServiceImpl::do_start_pie(
        const RequestHeader &header,
        const Value *input,
        i32 input_size,
        rrr::i32 *res,
        Value *output,
        i32 *output_size) {

    *res = SUCCESS;
    if (TxnRunner::get_running_mode() == MODE_2PL) {
        TxnRegistry::execute(header, input, input_size,
			     res, output, output_size);
    }
    else if (TxnRunner::get_running_mode() == MODE_NONE) {
        TxnRegistry::execute(header, input, input_size,
			     res, output, output_size);
    }
    else if (TxnRunner::get_running_mode() == MODE_OCC) {
        TxnRegistry::execute(header, input, input_size,
			     res, output, output_size);
    }
    else {
        verify(0);
    }
}


void RococoServiceImpl::naive_batch_start_pie(
        const std::vector<RequestHeader> &headers,
        const std::vector<vector<Value>> &inputs,
        const std::vector<i32> &output_sizes,
        std::vector<i32> *results,
        std::vector<vector<Value>> *outputs,
        rrr::DeferredReply *defer) {
    std::lock_guard<std::mutex> guard(mtx_);

    DragonBall *defer_reply_db = NULL;
    if (TxnRunner::get_running_mode() == MODE_2PL) {
        defer_reply_db = new DragonBall(headers.size(), [/*&headers, */defer/*, results*/]() {
                //for (int i = 0; i < results->size(); i++) {
                //    Log::debug("tid: %ld, pid: %ld, results[%d]: %d", headers[i].tid, headers[i].pid, i, (*results)[i]);
                //}
                defer->reply();
                });
    }
    Log::debug("naive_batch_start_pie: tid: %ld", headers[0].tid);
    results->resize(headers.size());
    outputs->resize(headers.size());
    int num_pieces = headers.size();
    for (int i = 0; i < num_pieces; i++) {
        (*outputs)[i].resize(output_sizes[i]);
        if (defer_reply_db) {
            TxnRegistry::pre_execute_2pl(headers[i], inputs[i],
                     &((*results)[i]), &((*outputs)[i]), defer_reply_db);
        }
        else
            TxnRegistry::execute(headers[i], inputs[i],
                     &(*results)[i], &(*outputs)[i]);
    }
    if (!defer_reply_db)
        defer->reply();
    Log::debug("still fine");
}

void RococoServiceImpl::start_pie(
        const RequestHeader& header,
        const std::vector<mdb::Value>& input,
        const rrr::i32 &output_size,
        rrr::i32* res,
        std::vector<mdb::Value>* output,
        rrr::DeferredReply* defer) {
    std::lock_guard<std::mutex> guard(mtx_);

#ifdef PIECE_COUNT
    piece_count_key_t piece_count_key = 
        (piece_count_key_t){header.t_type, header.p_type};
    std::map<piece_count_key_t, uint64_t>::iterator pc_it = 
        piece_count_.find(piece_count_key);

    if (pc_it == piece_count_.end())
        piece_count_[piece_count_key] = 1;
    else
        piece_count_[piece_count_key]++;
    piece_count_tid_.insert(header.tid);
#endif

    output->resize(output_size);
    // find stored procedure, and run it
    *res = SUCCESS;
    if (TxnRunner::get_running_mode() == MODE_2PL) {
        DragonBall *defer_reply_db = new DragonBall(1, [defer]() {
                defer->reply();
                });
        TxnRegistry::pre_execute_2pl(header, input, res, output, defer_reply_db);
    }
    else if (TxnRunner::get_running_mode() == MODE_NONE) {
        TxnRegistry::execute(header, input, res, output);
        defer->reply();
    }
    else if (TxnRunner::get_running_mode() == MODE_OCC) {
        TxnRegistry::execute(header, input, res, output);
        defer->reply();
    }
    else
        verify(0);
}

void RococoServiceImpl::prepare_txn(
        const rrr::i64& tid,
        const std::vector<i32> &sids,
        rrr::i32* res,
        rrr::DeferredReply* defer) {
    // logging.
    // generate proper logging staff.

    if (Config::get_config()->do_logging()) {
        string log_s;
        prepare_txn_job(tid, sids, res, NULL, &log_s);
        //Log::debug("here evil comes!");
        auto job = [defer] () {
            defer->reply();
        };

        if (*res == SUCCESS)
            recorder_->submit(log_s, job);
        else
            defer->reply();
    } else {
        prepare_txn_job(tid, sids, res, defer);
    }

}

void RococoServiceImpl::prepare_txn_job(
        const rrr::i64& tid,
        const std::vector<i32> &sids,
        rrr::i32* res,
        rrr::DeferredReply* defer,
        std::string *log_s) {

    std::lock_guard<std::mutex> guard(mtx_);
    if (log_s)
        TxnRunner::get_prepare_log(tid, sids, log_s);

    *res = TxnRunner::do_prepare(tid);

#ifdef PIECE_COUNT
    std::map<piece_count_key_t, uint64_t>::iterator pc_it;
    if (*res != SUCCESS)
        piece_count_prepare_fail_++;
    else
        piece_count_prepare_success_++;
    if (piece_count_timer_.elapsed() >= 5.0) {
        Log::info("PIECE_COUNT: txn served: %u", piece_count_tid_.size());
        Log::info("PIECE_COUNT: prepare success: %llu, failed: %llu",
		  piece_count_prepare_success_, piece_count_prepare_fail_);
        for (pc_it = piece_count_.begin(); pc_it != piece_count_.end(); pc_it++)
            Log::info("PIECE_COUNT: t_type: %d, p_type: %d, started: %llu",
		      pc_it->first.t_type, pc_it->first.p_type, pc_it->second);
        piece_count_timer_.start();
    }
#endif
    if (defer)
        defer->reply();
}

void RococoServiceImpl::commit_txn(
        const rrr::i64& tid,
        rrr::i32* res,
        rrr::DeferredReply* defer) {

    std::lock_guard<std::mutex> guard(mtx_);
    *res = TxnRunner::do_commit(tid);
    if (Config::get_config()->do_logging()) {
        const char commit_tag = 'c';
        std::string log_s;
        log_s.resize(sizeof(tid) + sizeof(commit_tag));
        memcpy((void *)log_s.data(), (void *)&tid, sizeof(tid));
        memcpy((void *)log_s.data(), (void *)&commit_tag, sizeof(commit_tag));
        recorder_->submit(log_s);
    }
    defer->reply();
}

void RococoServiceImpl::abort_txn(
        const rrr::i64& tid,
        rrr::i32* res,
        rrr::DeferredReply* defer) {
    
    std::lock_guard<std::mutex> guard(mtx_);

    Log::debug("get abort_txn: tid: %ld", tid);
    //if (TxnRunner::get_running_mode() != MODE_2PL) {
    //    *res = TxnRunner::do_abort(tid, defer);
    //    defer->reply();
    //}
    //else {
    //    TxnRunner::do_abort(tid, defer);
    //}
    *res = TxnRunner::do_abort(tid);
    if (Config::get_config()->do_logging()) {
        const char abort_tag = 'a';
        std::string log_s;
        log_s.resize(sizeof(tid) + sizeof(abort_tag));
        memcpy((void *)log_s.data(), (void *)&tid, sizeof(tid));
        memcpy((void *)log_s.data(), (void *)&abort_tag, sizeof(abort_tag));
        recorder_->submit(log_s);
    }
    defer->reply();
    Log::debug("abort finish");
}

void RococoServiceImpl::batch_start_pie(
        const BatchRequestHeader& batch_header, 
        const std::vector<Value>& input, 
        rrr::i32* res, 
        std::vector<Value>* output) {

    verify(0);
    RequestHeader header;
    header.t_type = batch_header.t_type;
    header.cid = batch_header.cid;
    header.tid = batch_header.tid;
    BatchStartArgsHelper bsah_input, bsah_output;
    int ret;
    ret = bsah_input.init_input(&input, batch_header.num_piece);
    verify(ret == 0);
    ret = bsah_output.init_output(output, batch_header.num_piece, 
				  batch_header.expected_output_size);
    verify(ret == 0);
    Value const* piece_input;
    i32 input_size;
    i32 output_size;
    *res = SUCCESS;
    while (0 == bsah_input.get_next_input(&header.p_type, &header.pid, 
					  &piece_input, &input_size, 
					  &output_size)) {
        i32 res_buf;
        do_start_pie(header, piece_input, input_size, &res_buf, 
		     bsah_output.get_output_ptr(), &output_size);
        ret = bsah_output.put_next_output(res_buf, output_size);
        verify(ret == 0);
        if (res_buf != SUCCESS)
            *res = REJECT;
    }

    bsah_output.done_output();
}

void RococoServiceImpl::rcc_batch_start_pie(
        const std::vector<RequestHeader> &headers,
        const std::vector<std::vector<Value>> &inputs,
        BatchChopStartResponse* results,
        rrr::DeferredReply* defer) {

    verify(TxnRunner::get_running_mode() == MODE_RCC);

    static bool do_record = Config::get_config()->do_logging();

    if (do_record) {
        auto job = [&headers, &inputs, results, defer, this] () {
            this->rcc_batch_start_pie_job(headers, inputs, results, defer);
        };

        rrr::Marshal m;
        m << headers << inputs;

        recorder_->submit(m, job);
    } else {
        rcc_batch_start_pie_job(headers, inputs, results, defer);
        //for (int i = 0; i < headers.size(); i++) {
        //    Log::debug("g size: %u", (*results)[i].gra_m.gra->size());
        //}
        //defer->reply();
    }
}

void RococoServiceImpl::rcc_batch_start_pie_job(
        const std::vector<RequestHeader> &headers,
        const std::vector<std::vector<Value>> &inputs,
        BatchChopStartResponse* res,
        rrr::DeferredReply* defer) {

    std::lock_guard<std::mutex> guard(mtx_);

    res->is_defers.resize(headers.size());
    res->outputs.resize(headers.size());

    Log::debug("batch req, headers size:%u", headers.size());

    // TODO
    verify(TxnRunner::get_running_mode() == MODE_RCC);

    Vertex<TxnInfo> *tv = dep_s->start_pie_txn(headers[0].tid);

    for (int i = 0; i < headers.size(); i++) {
        auto &header = headers[i];
        auto &input = inputs[i];
        auto &output = res->outputs[i];

        //    Log::debug("receive start request. txn_id: %llx, pie_id: %llx", header.tid, header.pid);

        // XXX generate the IR, DR, W op sets;
        //auto lock_oracle = TxnRegistry::get_lock_oracle(header);

        // add the piece into the dep graph
        PieInfo pi;
        pi.pie_id_ = header.pid;
        pi.txn_id_ = header.tid;
        pi.type_ = header.p_type;

        //std::unordered_map<cell_locator_t, int, cell_locator_t_hash> opset; //OP_W, OP_DR, OP_IR
        // get the read and write sets;
        //lock_oracle(header, input.data(), input.size(), &opset);

        Vertex<PieInfo> *pv;
        dep_s->start_pie(pi, &pv, NULL);

        // execute the IR actions.
        bool &is_defered_buf = pi.defer_;
        //cell_entry_map_t rw_entry;
        TxnRegistry::exe_deptran_start(header, input, is_defered_buf, output, pv, tv);
        res->is_defers[i] = is_defered_buf ? 1 : 0;

        //dep_s->start_pie(pi, opset, rw_entry);
    }
    //  return the resutls.
    // only return a part of the graph.
    auto &tid = headers[0].tid;
    dep_s->sub_txn_graph(tid, res->gra_m);

    //static int64_t sample = 0;
    //if (sample++ % 100 == 0) {
    //    int sz = res->gra_m.ret_set.size();
    //      scsi_->do_statistics(S_RES_KEY_GRAPH_SIZE, sz);
    //}
    defer->reply();
}

void RococoServiceImpl::rcc_start_pie(
        const RequestHeader &header,
        const std::vector<Value> &input,
        ChopStartResponse* res,
        rrr::DeferredReply* defer) {
    verify(TxnRunner::get_running_mode() == MODE_RCC);

    static bool do_record = Config::get_config()->do_logging();

    if (do_record) {

        auto job = [&header, &input, res, defer, this] () {
            this->rcc_start_pie_job(header, input, res, defer);
        };
        
        Marshal m;
        m << header;
        m << input;
        recorder_->submit(m, job);
    } else {
        rcc_start_pie_job(header, input, res, defer);
    }
}

void RococoServiceImpl::rcc_start_pie_job(
        const RequestHeader &header,
        const std::vector<Value> &input,
        ChopStartResponse* res,
        rrr::DeferredReply* defer) {
    std::lock_guard<std::mutex> guard(mtx_);

    verify(TxnRunner::get_running_mode() == MODE_RCC);

//    Log::debug("receive start request. txn_id: %llx, pie_id: %llx", header.tid, header.pid);

    // XXX generate the IR, DR, W op sets;
    //auto lock_oracle = TxnRegistry::get_lock_oracle(header);

    // add the piece into the deptran graph
    PieInfo pi;
    pi.pie_id_ = header.pid;
    pi.txn_id_ = header.tid;
    pi.type_ = header.p_type;

    //std::unordered_map<cell_locator_t, int, 
    //                   cell_locator_t_hash> opset; //OP_W, OP_DR, OP_IR
    // get the read and write sets;
    //lock_oracle(header, input.data(), input.size(), &opset);

    Vertex<PieInfo> *pv = NULL;
    Vertex<TxnInfo> *tv = NULL;
    dep_s->start_pie(pi, &pv, &tv);

    // execute the IR actions.
    bool &is_defered_buf = pi.defer_;
    //cell_entry_map_t rw_entry;
    TxnRegistry::exe_deptran_start(header, 
                                   input, 
                                   is_defered_buf, 
                                   res->output, pv, 
                                   tv/*&rw_entry*/);
    res->is_defered = is_defered_buf ? 1 : 0;

    //dep_s->start_pie(pi, opset, rw_entry);

    //  return the 2resutls.
    // only return a part of the graph.
    auto sz_sub_gra = dep_s->sub_txn_graph(header.tid, res->gra_m);

    stat_sz_gra_start_.sample(sz_sub_gra);

    //static int64_t sample = 0;
    //if (sample++ % 100 == 0) {
    //    int sz = res->gra_m.ret_set.size();
    //    scsi_->do_statistics(S_RES_KEY_GRAPH_SIZE, sz);
    //}

    if (defer)
        defer->reply();

//    Log::debug("reply to start request. txn_id: %llx, pie_id: %llx, graph size: %d", header.tid, header.pid, (int)res->gra.size());
}

void RococoServiceImpl::rcc_finish_txn(
        const ChopFinishRequest& req,
        ChopFinishResponse* res,
        rrr::DeferredReply* defer) {
    std::lock_guard<std::mutex> guard(mtx_);

    //Log::debug("receive finish request. txn_id: %llx, graph size: %d", req.txn_id, req.gra.size());

    verify(TxnRunner::get_running_mode() == MODE_RCC);
    verify(req.gra.size() > 0);
    stat_sz_gra_commit_.sample(req.gra.size());

    static bool do_record = Config::get_config()->do_logging();
    if (do_record) {
        Marshal m;
        m << req;
        recorder_->submit(m);
    }

    // union the graph into dep graph
    dep_s->txn_gra_.union_graph(req.gra, true);

    Graph<TxnInfo> &txn_gra_ = dep_s->txn_gra_;
    Vertex<TxnInfo> *v = txn_gra_.find(req.txn_id);

    verify(v != NULL);
    v->data_.res = res;

    v->data_.union_status(TXN_FINISH);

    rcc_finish_to_commit(v, defer);


}

void RococoServiceImpl::rcc_finish_to_commit(
        Vertex<TxnInfo> *v,
        rrr::DeferredReply* defer) {

    TxnInfo &tinfo = v->data_;
    if (tinfo.during_commit) {
        Log::debug("this txn is already during commit! tid: %llx", tinfo.id());
        return;
    } else {
        tinfo.during_commit = true;
    }

    Graph<TxnInfo> &txn_gra = dep_s->txn_gra_;
    // a commit function.

    std::function<void(void)> scc_anc_commit_cb = [&txn_gra, v, defer, this] () {
        uint64_t txn_id = v->data_.id();
        Log::debug("all scc ancestors have committed for txn id: %llx", txn_id);
        // after all scc ancestors become COMMIT
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
            this->stat_sz_scc_.sample(sscc.size());
            for(auto& vv: sscc) {
                bool commit_by_other = vv->data_.get_status() & TXN_COMMIT;

                if (!commit_by_other) {
                    // apply changes.

                    // this res may not be mine !!!!
                    if (vv->data_.res != nullptr) {
                        TxnRegistry::exe_deptran_finish(vv->data_.id(), vv->data_.res->outputs);
                    }

                    Log::debug("txn commit. tid:%llx", vv->data_.id());
                    // delay return back to clients.
                    //
                    verify(vv->data_.committed_ == false);
                    vv->data_.committed_ = true;
                    vv->data_.union_status(TXN_COMMIT, false);
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
        // after all ancestors become FINISH
        // wait for all ancestors of scc become COMMIT
        std::set<Vertex<TxnInfo>* > scc_anc;
        this->dep_s->find_txn_scc_nearest_anc(v, scc_anc);

        DragonBall *wait_commit_ball = new DragonBall(scc_anc.size() + 1, 
                                                      scc_anc_commit_cb);
        for (auto &sav: scc_anc) {
            //Log::debug("\t ancestor id: %llx", sav->data_.id());
            // what if the ancestor is not related and not committed or not finished?
            sav->data_.register_event(TXN_COMMIT, wait_commit_ball);
            rcc_ask_finish(sav);
        }
        wait_commit_ball->trigger();
    };

    std::unordered_set<Vertex<TxnInfo>*> anc;
    dep_s->find_txn_anc_opt(v, anc);


    DragonBall *wait_finish_ball = new DragonBall(anc.size() + 1, anc_finish_cb);
    for (auto &av: anc) {
        Log::debug("\t ancestor id: %llx", av->data_.id());
        av->data_.register_event(TXN_FINISH, wait_finish_ball);
        rcc_ask_finish(av);
    }
    wait_finish_ball->trigger();
}

void RococoServiceImpl::rcc_ask_finish(Vertex<TxnInfo>* av) {
    Graph<TxnInfo> &txn_gra = this->dep_s->txn_gra_;
    TxnInfo &tinfo = av->data_;
    if (!tinfo.is_involved()) {

        if (tinfo.is_commit()) {
            Log::debug("observed commited unrelated txn, id: %llx", tinfo.id());
        } else if (tinfo.is_finish()) {
            Log::debug("observed finished unrelated txn, id: %llx", tinfo.id());
            rcc_finish_to_commit(av, nullptr);
        } else if (tinfo.during_asking) {
            // don't have to ask twice
            Log::debug("observed in-asking unrealted txn, id: %llx", tinfo.id());
        } else {
            static int64_t sample = 0;
            if (sample++ % 100 == 0) {
                scsi_->do_statistics(S_RES_KEY_N_ASK, 1);
            }
            int32_t sid = tinfo.random_server();
            RococoProxy* proxy = dep_s->get_server_proxy(sid);

            rrr::FutureAttr fuattr;
            fuattr.callback = [this, av, &txn_gra] (Future *fu) {
                std::lock_guard<std::mutex> guard(this->mtx_);
                int e = fu->get_error_code();
                if (e != 0) {
                    Log::info("connection failed: e: %d =%s", e, strerror(e));
                    verify(0);
                }
                Log::debug("got finish request for this txn, it is not related"
			   " to this server tid: %llx.", av->data_.id());

                CollectFinishResponse res;
                fu->get_reply() >> res;
                
                static bool do_record = Config::get_config()->do_logging();
                if (do_record) {
                    Marshal m;
                    m << res;
                    this->recorder_->submit(m);
                }

                stat_sz_gra_ask_.sample(res.gra_m.gra->size()); 
                // Be careful! this one could bring more evil than we want.
                txn_gra.union_graph(*(res.gra_m.gra), true);
                // for every transaction it unions,  handle this transaction like normal finish workflow.
                // FIXME is there problem here?
                rcc_finish_to_commit(av, nullptr);
            };
            Log::debug("observed uncommitted unrelated txn, tid: %llx, related"
		       " server id: %x", tinfo.id(), sid);
            tinfo.during_asking = true;
            stat_n_ask_.sample();
            Future* f1 = proxy->async_rcc_ask_txn(tinfo.txn_id_, fuattr);
            verify(f1 != nullptr);
            Future::safe_release(f1);
            //verify(av->data_.is_involved());
            n_asking_ ++;
        }
    } else {
        // This txn belongs to me, sooner or later I'll receive the finish request.
    }

}


void RococoServiceImpl::rcc_ask_txn(
        const rrr::i64& tid,
        CollectFinishResponse* res,
        rrr::DeferredReply* defer) {

    std::lock_guard<std::mutex> guard(mtx_);

    verify(TxnRunner::get_running_mode() == MODE_RCC);
    Vertex<TxnInfo> *v = dep_s->txn_gra_.find(tid);

    std::function<void(void)> callback = [this, res, defer, tid] () {

        // not the entire graph!!!!!
        // should only return a part of the graph.
        this->dep_s->sub_txn_graph(tid, res->gra_m);
//       Log::debug("return a sub graph for other server's unrelated txn: %llx, graph size: %d", tid, (int) res->gra_m.gra->size());

        //Graph<TxnInfo> &txn_gra = this->dep_s->txn_gra_;
        //Log::debug("return the entire graph for txn: %llx, graph size: %d", tid, txn_gra.size());
        //res->gra = this->dep_s->txn_gra_;
        // return the subgraph in a delayed manner.
        defer->reply();
    };


    DragonBall *ball = new DragonBall(2, callback);

    //register an event, triggered when the status >= FINISH;
    verify (v->data_.is_involved());
    v->data_.register_event(TXN_FINISH, ball);
    ball->trigger();
}

void RococoServiceImpl::rcc_ro_start_pie(
        const RequestHeader &header,
        const vector<Value> &input,
        vector<Value> *output,
        rrr::DeferredReply *defer) {
    std::lock_guard<std::mutex> guard(mtx_);
    // do read only transaction
    //auto lock_oracle = TxnRegistry::get_lock_oracle(header);

    // execute the IR actions.

    //cell_entry_map_t rw_entry;
    std::vector<TxnInfo*> conflict_txns;
    TxnRegistry::exe_deptran_start_ro(header, input, *output, &conflict_txns);

    // get conflicting transactions
    //dep_s->conflict_txn(opset, conflict_txns, rw_entry);

    // TODO callback: read the value and return.
    std::function<void(void)> cb = [header, /*&input, output, */defer] () {
        defer->reply();
    };

    // wait for them become commit.
    //

    DragonBall *ball = new DragonBall(conflict_txns.size() + 1, cb);

    for (auto tinfo: conflict_txns) {
        tinfo->register_event(TXN_COMMIT, ball);
    }
    ball->trigger();

}

void RococoServiceImpl::rpc_null(rrr::DeferredReply* defer) {
    defer->reply();
}

} // namespace rcc
