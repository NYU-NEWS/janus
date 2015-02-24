#include "all.h"

namespace rococo {


RococoServiceImpl::RococoServiceImpl(ServerControlServiceImpl *scsi) : 
    scsi_(scsi) {

#ifdef PIECE_COUNT
    piece_count_timer_.start();
    piece_count_prepare_fail_ = 0;
    piece_count_prepare_success_ = 0;
#endif
    verify(RCCDTxn::dep_s == NULL);
    RCCDTxn::dep_s = new DepGraph();

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

    *res = TPL::do_prepare(tid);

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
    *res = TPL::do_commit(tid);
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
    *res = TPL::do_abort(tid);
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


void RococoServiceImpl::rcc_batch_start_pie(
        const std::vector<RequestHeader> &headers,
        const std::vector<std::vector<Value>> &inputs,
        BatchChopStartResponse* res,
        rrr::DeferredReply* defer) {

    verify(TxnRunner::get_running_mode() & (MODE_RCC | MODE_ROT));
    auto txn = (RCCDTxn*) txn_mgr_.get_or_create(headers[0].tid);

    res->is_defers.resize(headers.size());
    res->outputs.resize(headers.size());

    auto job = [&headers, &inputs, res, defer, this] () {
        std::lock_guard<std::mutex> guard(mtx_);

        Log::debug("batch req, headers size:%u", headers.size());
        auto &tid = headers[0].tid;
//    Vertex<TxnInfo> *tv = RCC::dep_s->start_pie_txn(tid);

        for (int i = 0; i < headers.size(); i++) {
            auto &header = headers[i];
            auto &input = inputs[i];
            auto &output = res->outputs[i];

            //    Log::debug("receive start request. txn_id: %llx, pie_id: %llx", header.tid, header.pid);

            bool deferred;
            txn->start_pie(header, input, &deferred, &output);
            res->is_defers[i] = deferred ? 1 : 0;

        }
        RCCDTxn::dep_s->sub_txn_graph(tid, res->gra_m);
        defer->reply();

    };
    static bool do_record = Config::get_config()->do_logging();

    if (do_record) {
        rrr::Marshal m;
        m << headers << inputs;

        recorder_->submit(m, job);
    } else {
        job();
    }
}

void RococoServiceImpl::rcc_start_pie(
        const RequestHeader &header,
        const std::vector<Value> &input,
        ChopStartResponse* res,
        rrr::DeferredReply* defer) {
//    Log::debug("receive start request. txn_id: %llx, pie_id: %llx", header.tid, header.pid);
    verify(TxnRunner::get_running_mode() & (MODE_RCC | MODE_ROT));
    auto txn = (RCCDTxn*) txn_mgr_.get_or_create(header.tid);

    static bool do_record = Config::get_config()->do_logging();

    auto job = [&header, &input, res, defer, this] () {
        std::lock_guard<std::mutex> guard(this->mtx_);
        bool deferred;
        txn->start(header, input, &deferred, &res->output);

        res->is_defered = deferred ? 1 : 0;
        auto sz_sub_gra = RCCDTxn::dep_s->sub_txn_graph(header.tid, res->gra_m);
        stat_sz_gra_start_.sample(sz_sub_gra);

        if (defer) defer->reply();
//    Log::debug("reply to start request. txn_id: %llx, pie_id: %llx, graph size: %d", header.tid, header.pid, (int)res->gra.size());
    };

    if (do_record) {
        Marshal m;
        m << header;
        m << input;
        recorder_->submit(m, job);
    } else {
        job();
    }
}

void RococoServiceImpl::rcc_finish_txn( // equivalent to commit phrase
        const ChopFinishRequest& req,
        ChopFinishResponse* res,
        rrr::DeferredReply* defer) {
    std::lock_guard<std::mutex> guard(mtx_);

    //Log::debug("receive finish request. txn_id: %llx, graph size: %d", req.txn_id, req.gra.size());

    verify(TxnRunner::get_running_mode() & (MODE_RCC | MODE_ROT));
    verify(req.gra.size() > 0);
    stat_sz_gra_commit_.sample(req.gra.size());

    static bool do_record = Config::get_config()->do_logging();
    if (do_record) {
        Marshal m;
        m << req;
        recorder_->submit(m);
    }

    auto txn = (RCCDTxn*) txn_mgr_.get(req.txn_id);

    txn->commit(req, defer);


}

void RococoServiceImpl::rcc_ask_txn(
        const rrr::i64& tid,
        CollectFinishResponse* res,
        rrr::DeferredReply* defer) {

    std::lock_guard<std::mutex> guard(mtx_);

    verify(TxnRunner::get_running_mode() == MODE_RCC);
    Vertex<TxnInfo> *v = RCCDTxn::dep_s->txn_gra_.find(tid);

    std::function<void(void)> callback = [this, res, defer, tid] () {

        // not the entire graph!!!!!
        // should only return a part of the graph.
        RCCDTxn::dep_s->sub_txn_graph(tid, res->gra_m);
//       Log::debug("return a sub graph for other server's unrelated txn: %llx, graph size: %d", tid, (int) res->gra_m.gra->size());

        //Graph<TxnInfo> &txn_gra = this->dep_s->txn_gra_;
        //Log::debug("return the entire graph for txn: %llx, graph size: %d", tid, txn_gra.size());
        //res->gra = this->dep_s->txn_gra_;
        // return the subgraph in a delayed manner.
        defer->reply();
    };


    DragonBall *ball = new DragonBall(2, callback);

    //register an event, triggered when the status >= COMMITTING;
    verify (v->data_.is_involved());
    v->data_.register_event(TXN_CMT, ball);
    ball->trigger();
}

void RococoServiceImpl::rcc_ro_start_pie(
        const RequestHeader &header,
        const vector<Value> &input,
        vector<Value> *output,
        rrr::DeferredReply *defer) {
    std::lock_guard<std::mutex> guard(mtx_);

    auto txn = (RCCDTxn*) txn_mgr_.get_or_create(header.tid);

    // do read only transaction
    //auto lock_oracle = TxnRegistry::get_lock_oracle(header);

    // execute the IR actions.

    //cell_entry_map_t rw_entry;
    std::vector<TxnInfo*> conflict_txns;
    txn->start_ro(header, input, *output, &conflict_txns);

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
        tinfo->register_event(TXN_DCD, ball);
    }
    ball->trigger();

}

void RococoServiceImpl::rpc_null(rrr::DeferredReply* defer) {
    defer->reply();
}

} // namespace rcc
