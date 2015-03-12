#include "all.h"

/**
 * What shoud we do to change this to asynchronous?
 * 1. Fisrt we need to have a queue to hold all transaction requests.
 * 2. pop next request, send start request for each piece until there is no available left.
 *          in the callback, send the next piece of start request.
 *          if responses to all start requests are collected.
 *              send the finish request
 *                  in callback, remove it from queue.
 *
 *
 */

namespace rococo {

Coordinator::Coordinator(
        uint32_t coo_id,
        std::vector<std::string>& addrs,
        int32_t benchmark,
        int32_t mode,
        ClientControlServiceImpl *ccsi,
        uint32_t thread_id,
        bool batch_optimal) : coo_id_(coo_id),
                              benchmark_(benchmark),
                              mode_(mode),
                              ccsi_(ccsi),
                              thread_id_(thread_id),
                              batch_optimal_(batch_optimal),
                              site_prepare_(addrs.size(), 0),
                              site_commit_(addrs.size(), 0),
                              site_abort_(addrs.size(), 0),
                              site_piece_(addrs.size(), 0) {

    uint64_t k = coo_id_;
    k <<= 32;

    this->next_pie_id_.store(k);
    this->next_txn_id_.store(k);

    rpc_poll_ = new PollMgr(1);

    for (auto& addr : addrs) {
        rrr::Client *rpc_cli = new rrr::Client(rpc_poll_);
        Log::info("connect to site: %s", addr.c_str());
        auto ret = rpc_cli->connect(addr.c_str());
        verify(ret == 0);
        RococoProxy *rpc_proxy = new RococoProxy(rpc_cli);
        vec_rpc_cli_.push_back(rpc_cli);
        vec_rpc_proxy_.push_back(rpc_proxy);
    }

    recorder_ = NULL;
    if (Config::get_config()->do_logging()) {
        std::string log_path(Config::get_config()->log_path());
        log_path.append(std::to_string(coo_id_));
        recorder_ = new Recorder(log_path.c_str());
        rpc_poll_->add(recorder_);
    }

    retry_wait_ = Config::get_config()->retry_wait();
}


RequestHeader Coordinator::gen_header(TxnChopper* ch) {
    RequestHeader header;
    header.cid = coo_id_;
    header.tid = ch->txn_id_;
    header.t_type = ch->txn_type_;
    return header;
}

BatchRequestHeader Coordinator::gen_batch_header(TxnChopper* ch) {
    BatchRequestHeader batch_header;
    batch_header.t_type = ch->txn_type_;
    batch_header.cid = coo_id_;
    batch_header.tid = ch->txn_id_;
    return batch_header;
}


/** thread safe */
void Coordinator::do_one(TxnRequest& req) {
    // pre-process
    ScopedLock(this->mtx_);
    TxnChopper *ch = TxnChopperFactory::gen_chopper(req, benchmark_);
    ch->txn_id_ = this->next_txn_id();

    // turn off batch now
    // if (batch_optimal_)
    //     batch_start(ch);

    Log::debug("do one request");

    if (ccsi_)
        ccsi_->txn_start_one(thread_id_, ch->txn_type_);
    switch (mode_) {
    case MODE_NONE:
    case MODE_OCC:
    case MODE_2PL:
        //        if (recorder_) {
        if (false) {
            std::string log_s;
            req.get_log(ch->txn_id_, log_s);
            std::function<void(void)> start_func = [this, ch] () {
                if (batch_optimal_)
                    naive_batch_start(ch);
                else
                    start(ch);
            };
            recorder_->submit(log_s, start_func);
        }
        else {
            if (batch_optimal_)
                naive_batch_start(ch);
            else
                start(ch);
        }
        break;
    case MODE_RCC:
    case MODE_RO6:
        if (recorder_) {
               std::string log_s;
            req.get_log(ch->txn_id_, log_s);
            std::function<void(void)> start_func = [this, ch] () {
                if (ch->is_read_only())
                    deptran_start_ro(ch);
                else {
                    if (batch_optimal_)
                        deptran_batch_start(ch);
                    else
                        deptran_start(ch);
                }
            };
            recorder_->submit(log_s, start_func);
        } else {
            if (ch->is_read_only())
                deptran_start_ro(ch);
            else {
                if (batch_optimal_)
                    deptran_batch_start(ch);
                else
                    deptran_start(ch);
            }
        }
        break;
    case MODE_RPC_NULL:
        rpc_null_start(ch);
        break;
    default:
        verify(0);
    }
    // finish request is triggered in the callback of start request.
}

void Coordinator::rpc_null_start(TxnChopper *ch) {
    rrr::FutureAttr fuattr;
    fuattr.callback = [ch, this] (Future *fu) {
        ch->reply_.res_ = SUCCESS;
        TxnReply &txn_reply_buf = ch->get_reply();
        double last_latency = ch->last_attempt_latency();
        this->report(txn_reply_buf, last_latency
#ifdef TXN_STAT
                , ch
#endif
                );
        ch->callback_(txn_reply_buf);
        delete ch;
    };
    RococoProxy* proxy = vec_rpc_proxy_[coo_id_ % vec_rpc_proxy_.size()];
    Future::safe_release(proxy->async_rpc_null(fuattr));
}

void Coordinator::restart(TxnChopper* ch) {
    ScopedLock(this->mtx_);
    ch->n_start_sent_ = 0;
    ch->retry();
    double last_latency = ch->last_attempt_latency();
    if (ccsi_)
        ccsi_->txn_retry_one(this->thread_id_, ch->txn_type_, last_latency);
    if (batch_optimal_)
        naive_batch_start(ch);
    else
        start(ch);
}

void Coordinator::start(TxnChopper* ch) {
    // new txn id for every new and retry.
    RequestHeader header = gen_header(ch);

    int pi;
    std::vector<Value>* input;
    int32_t server_id;
    int res;
    int output_size;
    while ((res = ch->next_piece(input,
                                 output_size,
                                 server_id,
                                 pi,
                                 header.p_type)) == 0) {
        header.pid = next_pie_id();

        rrr::FutureAttr fuattr;
        // remember this a asynchronous call!
        // variable funtional range is important!
        fuattr.callback = [ch, pi, this] (Future* fu) {
            bool callback = false;
            {
                ScopedLock(this->mtx_);

                int res;
                std::vector<mdb::Value> output;
                fu->get_reply() >> res >> output;
                ch->n_started_++;
                ch->n_start_sent_--;

                if (res == REJECT) {
                    verify(this->mode_ == MODE_2PL);
                    ch->commit_.store(false);
                }
                if (!ch->commit_.load()) {
                    if (ch->n_start_sent_ == 0) {
                        this->finish(ch);
                    }
                } else {
                    if (ch->start_callback(pi, res, output))
                        this->start(ch);
                    else if (ch->n_started_ == ch->n_pieces_) {
                        if (this->mode_ == MODE_OCC) {
                            this->prepare(ch);
                        } else if (this->mode_ == MODE_NONE) {
                            callback = true;
                            ch->reply_.res_ = SUCCESS;
                        } else if (this->mode_ == MODE_2PL) {
                            this->prepare(ch);
                        }
                    }
                }
            }

            if (callback) {
                TxnReply &txn_reply_buf = ch->get_reply();
                double last_latency = ch->last_attempt_latency();
                this->report(txn_reply_buf, last_latency
#ifdef TXN_STAT
                        , ch
#endif
                        );
                ch->callback_(txn_reply_buf);
                delete ch;
            }
        };

        RococoProxy* proxy = vec_rpc_proxy_[server_id];
        Future::safe_release(proxy->async_start_pie(header,
                                                    *input,
                                                    (i32)output_size,
                                                    fuattr));
        ch->n_start_sent_++;
        site_piece_[server_id]++;
    }
}

void Coordinator::naive_batch_start(TxnChopper *ch) {

    // new txn id for every new and retry.
    RequestHeader header = gen_header(ch);

    int pi;
    std::vector<Value>* input = nullptr;
    int32_t server_id;
    int res;
    int output_size;
    std::unordered_map<int32_t, deptran_batch_start_t> serv_start_reqs;
    while ((res = ch->next_piece(input, output_size, server_id, pi, header.p_type)) == 0) {
        header.pid = next_pie_id();
        auto it = serv_start_reqs.find(server_id);
        if (it == serv_start_reqs.end()) {
            it = serv_start_reqs.insert(std::pair<int32_t, deptran_batch_start_t>(server_id, deptran_batch_start_t())).first;
        }
        verify(input != nullptr);
        it->second.headers.push_back(header);
        it->second.inputs.push_back(*input);
        it->second.output_sizes.push_back(output_size);
        it->second.pis.push_back(pi);
    }

    for (auto it = serv_start_reqs.begin(); it != serv_start_reqs.end(); it++) {
        // remember this a asynchronous call! variable funtional range is important!
        std::vector<int> pis(it->second.pis);
        std::vector<RequestHeader> headers(it->second.headers);

        it->second.fuattr.callback = [ch, pis, this, headers](Future* fu) {
            bool callback = false;
            {
                Log::debug("Batch back");
                ScopedLock(this->mtx_);

                std::vector<i32> results;
                std::vector<std::vector<Value>> outputs;
                fu->get_reply() >> results >> outputs;
                verify(pis.size() == results.size());
                for (int i = 0; i < results.size(); i++)
                    Log::debug("result[%d]: %d", i, results[i]);

                bool callback_ret = false, call_finish = false;
                for (int i = 0; i < pis.size(); i++) {
                    int pi = pis[i];
                    int res = results[i];
                    std::vector<Value> &output = outputs[i];

                    ch->n_started_++;
                    ch->n_start_sent_--;

                    if (res == REJECT) {
                        verify(this->mode_ == MODE_2PL);
                        ch->commit_.store(false);
                    }
                    if (!ch->commit_.load()) {
                        if (ch->n_start_sent_ == 0) {
                            call_finish = true;
                            //this->finish(ch);
                        }
                    } else {
                        if (ch->start_callback(pi, res, output))
                            callback_ret = true;
                    }
                }
                if (call_finish) {
                    this->finish(ch);
                } else if (callback_ret) {
                    this->naive_batch_start(ch);
                } else if (ch->n_started_ == ch->n_pieces_) {
                    if (this->mode_ == MODE_OCC) {
                        this->prepare(ch);
                    } else if (this->mode_ == MODE_NONE) {
                        callback = true;
                        ch->reply_.res_ = SUCCESS;
                    } else if (this->mode_ == MODE_2PL) {
                        this->prepare(ch);
                    }
                }

            }
            if (callback) {
                ch->reply_.res_ = SUCCESS;
                TxnReply &txn_reply_buf = ch->get_reply();
                double last_latency = ch->last_attempt_latency();
                this->report(txn_reply_buf, last_latency
#ifdef TXN_STAT
                        , ch
#endif
                        );
                ch->callback_(txn_reply_buf);
            }

        };

        RococoProxy* proxy = vec_rpc_proxy_[it->first];
        Future::safe_release(proxy->async_naive_batch_start_pie(
                    it->second.headers,
                    it->second.inputs,
                    it->second.output_sizes,
                    it->second.fuattr
                    ));
        Log::debug("naive start sent: tid: %ld", header.tid);
        ch->n_start_sent_ += it->second.headers.size();
    }
}

/** thread safe */
void Coordinator::batch_start(TxnChopper* ch) {

    // new txn id for every new and retry.
    BatchRequestHeader batch_header = gen_batch_header(ch);

    std::vector<int> pi;
    std::vector<Value> input;
    int32_t server_id;
    int res;
    while ((res = ch->batch_next_piece(&batch_header, input, server_id, pi, this)) == 0) {

        rrr::FutureAttr fuattr;
        // remember this a asynchronous call! variable funtional range is important!
        fuattr.callback = [ch, pi, this](Future* fu) {
            bool callback = false;
            {
                ScopedLock(this->mtx_);

                i32 res;
                std::vector<mdb::Value> output;
                fu->get_reply() >> res >> output;
                ch->n_started_ += pi.size();
                ch->n_start_sent_--;

                if (res == REJECT) {
                    verify(mode_ == MODE_2PL);
                    ch->commit_.store(false);
                }
                if (!ch->commit_.load()) {
                    if (ch->n_start_sent_ == 0)
                        this->finish(ch);
                } else {
                    BatchStartArgsHelper bsah;
                    bsah.init_output_client(&output, pi.size());
                    if (ch->start_callback(pi, res, bsah))
                        this->batch_start(ch);
                    else if (ch->n_started_ == ch->n_pieces_) {
                        if (this->mode_ == MODE_OCC) {
                            this->prepare(ch);
                        } else if (this->mode_ == MODE_NONE) {
                            callback = true;
                            ch->reply_.res_ = SUCCESS;
                        } else if (this->mode_ == MODE_2PL) {
                            this->prepare(ch);
                        }
                    }
                }
            }

            if (callback) {
                TxnReply &txn_reply_buf = ch->get_reply();
                double last_latency = ch->last_attempt_latency();
                this->report(txn_reply_buf, last_latency
#ifdef TXN_STAT
                        , ch
#endif
                        );
                ch->callback_(txn_reply_buf);
                delete ch;
            }
        };

        RococoProxy* proxy = vec_rpc_proxy_[server_id];
        Future::safe_release(proxy->async_batch_start_pie(batch_header, input, fuattr));
        ch->n_start_sent_++;
    }
}

/** caller should be thread_safe */
void Coordinator::prepare(TxnChopper *ch) {
    verify(mode_ == MODE_OCC || mode_== MODE_2PL);
    RequestHeader header = gen_header(ch);

    // prepare piece, currently only useful for OCC
    rrr::FutureAttr fuattr;
    fuattr.callback = [ch, this] (Future *fu) {
        ScopedLock(this->mtx_);
        ch->n_prepared_++;
        int32_t e = fu->get_error_code();
        if (e != 0) {
            ch->commit_.store(false);
            Log_debug("2PL prepare failed");
        } else {
            int res;
            fu->get_reply() >> res;
            Log::debug("pre res: %d", res);
            if (res == REJECT) {
                ch->commit_.store(false);
            }
        }
        if (ch->n_prepared_ == ch->proxies_.size()) {
            this->finish(ch);
        }
    };
    std::vector<i32> sids;
    sids.reserve(ch->proxies_.size());
    for (auto& rp: ch->proxies_)
        sids.push_back(rp);
    for (auto& rp: ch->proxies_) {
        Log::debug("send prepare tid: %ld", header.tid);

        RococoProxy* proxy = vec_rpc_proxy_[rp];
        Future::safe_release(proxy->async_prepare_txn(header.tid, sids, fuattr));

        site_prepare_[rp]++;
    }
}

/** caller should be thread safe */
void Coordinator::finish(TxnChopper *ch) {
    verify(mode_ == MODE_OCC || mode_== MODE_2PL);

    // commit or abort piece
    rrr::FutureAttr fuattr;
    fuattr.callback = [ch, this] (Future *fu) {
        bool callback = false;
        bool retry = false;
        {
            ScopedLock(this->mtx_);
            ch->n_finished_++;

            Log::debug("finish");
            if (ch->n_finished_ == ch->proxies_.size()) {
                if (ch->reply_.res_ == REJECT && ch->can_retry()) {
                    retry = true;
                } else {
                    callback = true;
                }
            }
        }
        Log::debug("callback: %s, retry: %s", callback ? "True" : "False",
               retry ? "True" : "False");
        if (retry) {
            // random sleep before restart
//            struct timespec sleep_time, buf;
//            sleep_time.tv_sec = coo_id_;//RandomGenerator::rand(0, 4);
//            sleep_time.tv_nsec = 0;//RandomGenerator::rand(0, 1000 * 1000 * 1000 - 1);
//            nanosleep(&sleep_time, &buf);
//            uint64_t sleep_time = coo_id_ * 1000000 / 2;
//            apr_sleep(sleep_time);

            this->restart(ch);
        }
        if (callback) {
            // generate a reply and callback.
            TxnReply &txn_reply_buf = ch->get_reply();
            double last_latency = ch->last_attempt_latency();
            this->report(txn_reply_buf, last_latency
#ifdef TXN_STAT
                    , ch
#endif
                    );
            //if (retry_wait_ && txn_reply_buf.res_ != SUCCESS) {
            //    struct timespec sleep_time, buf;
            //    sleep_time.tv_sec = 0;
            //    sleep_time.tv_nsec = RandomGenerator::rand(1000 * 1000, 10 * 1000 * 1000);
            //    nanosleep(&sleep_time, &buf);
            //}
            ch->callback_(txn_reply_buf);
            delete ch;
        }
    };

    if (ch->commit_.load()) {
        Log::debug("send finish");
        ch->reply_.res_ = SUCCESS;
        for (auto& rp: ch->proxies_) {
            RococoProxy* proxy = vec_rpc_proxy_[rp];
            Future::safe_release(proxy->async_commit_txn(ch->txn_id_, fuattr));
            site_commit_[rp]++;
        }
    } else {
        Log::debug("send abort");
        ch->reply_.res_ = REJECT;
        for (auto& rp: ch->proxies_) {
            RococoProxy* proxy = vec_rpc_proxy_[rp];
            Future::safe_release(proxy->async_abort_txn(ch->txn_id_, fuattr));
            site_abort_[rp]++;
        }
    }
}

void Coordinator::deptran_batch_start(TxnChopper *ch) {

    // new txn id for every new and retry.
    RequestHeader header = gen_header(ch);

    int pi;
    std::vector<Value>* input = nullptr;
    int32_t server_id;
    int res;
    int output_size;
    std::unordered_map<int32_t, deptran_batch_start_t> serv_start_reqs;
    while ((res = ch->next_piece(input, output_size, server_id, pi, header.p_type)) == 0) {
        header.pid = next_pie_id();
        auto it = serv_start_reqs.find(server_id);
        if (it == serv_start_reqs.end()) {
            it = serv_start_reqs.insert(std::pair<int32_t, deptran_batch_start_t>(server_id, deptran_batch_start_t())).first;
        }
        verify(input != nullptr);
        it->second.headers.push_back(header);
        it->second.inputs.push_back(*input);
        it->second.pis.push_back(pi);
    }

    for (auto it = serv_start_reqs.begin(); it != serv_start_reqs.end(); it++) {
        // remember this a asynchronous call! variable funtional range is important!
        std::vector<int> pis(it->second.pis);
        std::vector<RequestHeader> headers(it->second.headers);

        it->second.fuattr.callback = [ch, pis, this, headers](Future* fu) {
            bool early_return = false;
            {
                Log::debug("Batch back");
                ScopedLock(this->mtx_);

                BatchChopStartResponse res;
                fu->get_reply() >> res;
                verify(pis.size() == res.is_defers.size());

                Graph<TxnInfo> &gra = *(res.gra_m.gra);
                //Log::debug("receive deptran start response, tid: %llx, pid: %llx, graph size: %d", headers[i].tid, headers[i].pid, gra.size());
                verify(gra.size() > 0);
                ch->gra_.union_graph(gra);

                if (gra.size() > 1)
                    ch->disable_early_return();

                bool callback_ret = false;
                for (int i = 0; i < res.is_defers.size(); i++) {
                    ch->n_started_++;
                    if (ch->start_callback(pis[i],  res.outputs[i], res.is_defers[i])) {
                        callback_ret = true;
                    }
                }

                if (callback_ret) {
                    this->deptran_batch_start(ch);
                }

                else if (ch->n_started_ == ch->n_pieces_) {
                    this->deptran_finish(ch);
                    if (ch->do_early_return()) {
                        early_return = true;
                    }
                }
            }
            if (early_return) {
                ch->reply_.res_ = SUCCESS;
                TxnReply &txn_reply_buf = ch->get_reply();
                double last_latency = ch->last_attempt_latency();
                this->report(txn_reply_buf, last_latency
#ifdef TXN_STAT
                        , ch
#endif
                        );
                ch->callback_(txn_reply_buf);
            }

        };

        RococoProxy* proxy = vec_rpc_proxy_[it->first];
        Future::safe_release(proxy->async_rcc_batch_start_pie(
                    it->second.headers,
                    it->second.inputs,
                    it->second.fuattr
                    ));
    }
}

void Coordinator::deptran_start(TxnChopper* ch) {

    // new txn id for every new and retry.
    RequestHeader header = gen_header(ch);

    int pi;
    std::vector<Value>* input = nullptr;
    int32_t server_id;
    int res;
    int output_size;
    while ((res = ch->next_piece(input, output_size, server_id, pi, header.p_type)) == 0) {
        header.pid = next_pie_id();

        rrr::FutureAttr fuattr;
        // remember this a asynchronous call! variable funtional range is important!
        fuattr.callback = [ch, pi, this, header](Future* fu) {
            bool early_return = false;
            {
           //     Log::debug("try locking at start response, tid: %llx, pid: %llx", header.tid, header.pid);
                ScopedLock(this->mtx_);

                ChopStartResponse res;
                fu->get_reply() >> res;

                if (IS_MODE_RO6) {
                    auto &ro = res.ro_list;
                    this->ro_txn_.insert(ro.begin(), ro.end());
                }

                // where should I store this graph?
                Graph<TxnInfo> &gra = *(res.gra_m.gra);
                Log::debug("start response graph size: %d", (int)gra.size());
                verify(gra.size() > 0);
                ch->gra_.union_graph(gra);

                Log::debug("receive deptran start response, tid: %llx, pid: %llx, graph size: %d", header.tid, header.pid, gra.size());
                if (gra.size() > 1)
                    ch->disable_early_return();

                ch->n_started_++;
                if (ch->start_callback(pi, res))
                    this->deptran_start(ch);
                else if (ch->n_started_ == ch->n_pieces_) {
                    this->deptran_finish(ch);
                    if (ch->do_early_return()) {
                        early_return = true;
                    }
                }
            }
            if (early_return) {
                ch->reply_.res_ = SUCCESS;
                TxnReply &txn_reply_buf = ch->get_reply();
                double last_latency = ch->last_attempt_latency();
                this->report(txn_reply_buf, last_latency
#ifdef TXN_STAT
                        , ch
#endif
                        );
                ch->callback_(txn_reply_buf);
            }

        };

        RococoProxy* proxy = vec_rpc_proxy_[server_id];
        Log::debug("send deptran start request, tid: %llx, pid: %llx", ch->txn_id_, header.pid);
        verify(input != nullptr);
        Future::safe_release(proxy->async_rcc_start_pie(header, *input, fuattr));
    }

}

/** caller should be thread safe */
void Coordinator::deptran_finish(TxnChopper *ch) {
    verify(IS_MODE_RCC || IS_MODE_RO6);
    Log::debug("deptran finish, %llx", ch->txn_id_);

    // commit or abort piece
    rrr::FutureAttr fuattr;
    fuattr.callback = [ch, this] (Future *fu) {
        int e = fu->get_error_code();
        verify(e == 0);

        bool callback = false;
        {
            ScopedLock(this->mtx_);
            ch->n_finished_++;

            ChopFinishResponse res;

            Log::debug("receive finish response. tid: %llx", ch->txn_id_);

            fu->get_reply() >> res;

            if (ch->n_finished_ == ch->proxies_.size()) {
                ch->finish_callback(res);
                callback = true;
            }
        }

        if (callback) {
            // generate a reply and callback.
            Log::debug("deptran callback, %llx", ch->txn_id_);
            if (!ch->do_early_return()) {
                ch->reply_.res_ = SUCCESS;
                TxnReply &txn_reply_buf = ch->get_reply();
                double last_latency = ch->last_attempt_latency();
                this->report(txn_reply_buf, last_latency
#ifdef TXN_STAT
                        , ch
#endif
                        );
                ch->callback_(txn_reply_buf);
            }
            delete ch;
        }
    };

    Log::debug("send deptran finish requests to %d servers, tid: %llx, graph size: %d", (int) ch->proxies_.size(), ch->txn_id_, ch->gra_.size());
    verify (ch->proxies_.size() == ch->gra_.find(ch->txn_id_)->data_.servers_.size());

    ChopFinishRequest req;
    req.txn_id = ch->txn_id_;
    req.gra = ch->gra_;
    if (IS_MODE_RO6) {
        // TODO. merge the read_only list.
        auto &ro = req.ro_list;
        ro.insert(ro.begin(), ro_txn_.begin(), ro_txn_.end());
    }

    verify(ch->gra_.size() > 0);
    verify(req.gra.size() > 0);
    for (auto& rp: ch->proxies_) {
        RococoProxy* proxy = vec_rpc_proxy_[rp];
        Future::safe_release(proxy->async_rcc_finish_txn(req, fuattr));
    }
}

void Coordinator::deptran_start_ro(TxnChopper* ch) {

    // new txn id for every new and retry.
    RequestHeader header = gen_header(ch);

    int pi;
    std::vector<Value>* input = nullptr;
    int32_t server_id;
    int res;
    int output_size;
    while ((res = ch->next_piece(input, output_size, server_id, pi, header.p_type)) == 0) {
        header.pid = next_pie_id();

        rrr::FutureAttr fuattr;
        // remember this a asynchronous call! variable funtional range is important!
        fuattr.callback = [ch, pi, this, header](Future* fu) {
            {
                ScopedLock(this->mtx_);

                std::vector<Value> res;
                fu->get_reply() >> res;

                Log::debug("receive deptran RO start response, tid: %llx, pid: %llx, ", header.tid, header.pid);

                ch->n_started_++;
                if (ch->read_only_start_callback(pi, NULL, res))
                    this->deptran_start_ro(ch);
                else if (ch->n_started_ == ch->n_pieces_) {
                    ch->read_only_reset();
                    this->deptran_finish_ro(ch);
                }
            }
        };

        RococoProxy* proxy = vec_rpc_proxy_[server_id];
        Log::debug("send deptran RO start request, tid: %llx, pid: %llx", ch->txn_id_, header.pid);
        verify(input != nullptr);
        Future::safe_release(proxy->async_rcc_ro_start_pie(header, *input, fuattr));
    }
}

void Coordinator::deptran_finish_ro(TxnChopper* ch) {

    RequestHeader header = gen_header(ch);
    int pi;
    std::vector<Value>* input = nullptr;
    int32_t server_id;
    int res;
    int output_size;
    while ((res = ch->next_piece(input, output_size, server_id, pi, header.p_type)) == 0) {
        header.pid = next_pie_id();

        rrr::FutureAttr fuattr;
        // remember this a asynchronous call! variable funtional range is important!
        fuattr.callback = [ch, pi, this, header](Future* fu) {
            bool callback = false;
            {
                ScopedLock(this->mtx_);

                int res;
                std::vector<Value> output;
                fu->get_reply() >> output;

                Log::debug("receive deptran RO start response (phase 2), tid: %llx, pid: %llx, ", header.tid, header.pid);

                ch->n_started_++;
                bool do_next_piece = ch->read_only_start_callback(pi, &res, output);
                if (do_next_piece)
                    deptran_finish_ro(ch);
                else if (ch->n_started_ == ch->n_pieces_) {
                    if (res == SUCCESS) {
                        ch->reply_.res_ = SUCCESS;
                        callback = true;
                    }
                    else if (ch->can_retry()) {
                        ch->read_only_reset();
                        double last_latency = ch->last_attempt_latency();
                        if (ccsi_)
                            ccsi_->txn_retry_one(this->thread_id_, ch->txn_type_, last_latency);
                        this->deptran_finish_ro(ch);
                        callback = false;
                    }
                    else {
                        ch->reply_.res_ = REJECT;
                        callback = true;
                    }
                }
            }
            if (callback) {
                // generate a reply and callback.
                Log::debug("deptran RO callback, %llx", ch->txn_id_);
                TxnReply &txn_reply_buf = ch->get_reply();
                double last_latency = ch->last_attempt_latency();
                this->report(txn_reply_buf, last_latency
#ifdef TXN_STAT
                        , ch
#endif
                        );
                ch->callback_(txn_reply_buf);
                delete ch;
            }
        };

        RococoProxy* proxy = vec_rpc_proxy_[server_id];
        Log::debug("send deptran RO start request (phase 2), tid: %llx, pid: %llx", ch->txn_id_, header.pid);
        verify(input != nullptr);
        Future::safe_release(proxy->async_rcc_ro_start_pie(header, *input, fuattr));
    }
}

void Coordinator::report(TxnReply &txn_reply,
        double last_latency
#ifdef TXN_STAT
        , TxnChopper *ch
#endif
        ) {
    if (ccsi_) {
        if (txn_reply.res_ == SUCCESS) {
#ifdef TXN_STAT
            txn_stats_[ch->txn_type_].one(ch->proxies_.size(), ch->p_types_);
#endif
            ccsi_->txn_success_one(thread_id_, txn_reply.txn_type_, txn_reply.start_time_, txn_reply.time_, last_latency, txn_reply.n_try_);
        }
        else
            ccsi_->txn_reject_one(thread_id_, txn_reply.txn_type_, txn_reply.start_time_, txn_reply.time_, last_latency, txn_reply.n_try_);
    }
}

} // namespace rococo
