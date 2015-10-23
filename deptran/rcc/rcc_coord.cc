#include "marshal-value.h"
#include "rcc_coord.h"
#include "frame.h"
#include "dtxn.h"
#include "txn-chopper-factory.h"
#include "benchmark_control_rpc.h"

namespace rococo {
void RCCCoord::deptran_batch_start(TxnChopper *ch) {
  // new txn id for every new and retry.
  RequestHeader header = gen_header(ch);

  int pi;

  std::vector<Value> *input = nullptr;
  int32_t server_id;
  int     res;
  int     output_size;
  std::unordered_map<int32_t, deptran_batch_start_t> serv_start_reqs;

  while ((res =
            ch->next_piece(input, output_size, server_id, pi,
                           header.p_type)) == 0) {
    header.pid = next_pie_id();
    auto it = serv_start_reqs.find(server_id);

    if (it == serv_start_reqs.end()) {
      it =
        serv_start_reqs.insert(std::pair<int32_t,
                                         deptran_batch_start_t>(server_id,
                                                                deptran_batch_start_t()))
        .first;
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

    it->second.fuattr.callback = [ch, pis, this, headers](Future * fu) {
      bool early_return = false;
      {
        Log::debug("Batch back");
        ScopedLock(this->mtx_);

        BatchChopStartResponse res;
        fu->get_reply() >>     res;
        verify(pis.size() == res.is_defers.size());

        Graph<TxnInfo>& gra = *(res.gra_m.gra);

        // Log::debug("receive deptran start response, tid: %llx, pid: %llx,
        // graph size: %d", headers[i].tid, headers[i].pid, gra.size());
        verify(gra.size() > 0);
        ch->gra_.Aggregate(gra);

        if (gra.size() > 1) ch->disable_early_return();

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
        TxnReply& txn_reply_buf = ch->get_reply();
        double    last_latency  = ch->last_attempt_latency();
        this->report(txn_reply_buf, last_latency
#ifdef TXN_STAT
                     , ch
#endif // ifdef TXN_STAT
                     );
        ch->callback_(txn_reply_buf);
      }
    };

    RococoProxy *proxy = commo_->vec_rpc_proxy_[it->first];
    Future::safe_release(proxy->async_rcc_batch_start_pie(
                           it->second.headers,
                           it->second.inputs,
                           it->second.fuattr
                           ));
  }
}

void RCCCoord::deptran_start(TxnChopper *ch) {
  // new txn id for every new and retry.
  RequestHeader header = gen_header(ch);

  int pi;

  std::vector<Value> *input = nullptr;
  int32_t server_id;
  int     res;
  int     output_size;

  while ((res =
            ch->next_piece(input, output_size, server_id, pi,
                           header.p_type)) == 0) {
    header.pid = next_pie_id();

    rrr::FutureAttr fuattr;

    // remember this a asynchronous call! variable funtional range is important!
    fuattr.callback = [ch, pi, this, header](Future * fu) {
      bool early_return = false;
      {
        //     Log::debug("try locking at start response, tid: %llx, pid: %llx",
        // header.tid, header.pid);
        ScopedLock(this->mtx_);

        ChopStartResponse  res;
        fu->get_reply() >> res;

        // where should I store this graph?
        Graph<TxnInfo>& gra = *(res.gra_m.gra);
        Log::debug("start response graph size: %d", (int)gra.size());
        verify(gra.size() > 0);
        ch->gra_.Aggregate(gra);

        Log::debug(
          "receive deptran start response, tid: %llx, pid: %llx, graph size: %d",
          header.tid,
          header.pid,
          gra.size());

        if (gra.size() > 1) ch->disable_early_return();

        ch->n_started_++;

        if (ch->start_callback(pi, res)) this->deptran_start(ch);
        else if (ch->n_started_ == ch->n_pieces_) {
          this->deptran_finish(ch);

          if (ch->do_early_return()) {
            early_return = true;
          }
        }
      }

      if (early_return) {
        ch->reply_.res_ = SUCCESS;
        TxnReply& txn_reply_buf = ch->get_reply();
        double    last_latency  = ch->last_attempt_latency();
        this->report(txn_reply_buf, last_latency
#ifdef TXN_STAT
                     , ch
#endif // ifdef TXN_STAT
                     );
        ch->callback_(txn_reply_buf);
      }
    };

    RococoProxy *proxy = commo_->vec_rpc_proxy_[server_id];
    Log::debug("send deptran start request, tid: %llx, pid: %llx",
               cmd_id_,
               header.pid);
        verify(input != nullptr);
    Future::safe_release(proxy->async_rcc_start_pie(header, *input, fuattr));
  }
}

/** caller should be thread safe */
void RCCCoord::deptran_finish(TxnChopper *ch) {
        verify(IS_MODE_RCC || IS_MODE_RO6);
  Log::debug("deptran finish, %llx", cmd_id_);

  // commit or abort piece
  rrr::FutureAttr fuattr;
  fuattr.callback = [ch, this](Future * fu) {
    int e = fu->get_error_code();
        verify(e == 0);

    bool callback = false;
    {
      ScopedLock(this->mtx_);
      ch->n_finished_++;

      ChopFinishResponse res;

      Log::debug("receive finish response. tid: %llx", cmd_id_);

      fu->get_reply() >> res;

      if (ch->n_finished_ == ch->proxies_.size()) {
        ch->finish_callback(res);
        callback = true;
      }
    }

    if (callback) {
      // generate a reply and callback.
      Log::debug("deptran callback, %llx", cmd_id_);

      if (!ch->do_early_return()) {
        ch->reply_.res_ = SUCCESS;
        TxnReply& txn_reply_buf = ch->get_reply();
        double    last_latency  = ch->last_attempt_latency();
        this->report(txn_reply_buf, last_latency
#ifdef TXN_STAT
                     , ch
#endif // ifdef TXN_STAT
                     );
        ch->callback_(txn_reply_buf);
      }
      delete ch;
    }
  };

  Log::debug(
    "send deptran finish requests to %d servers, tid: %llx, graph size: %d",
    (int)ch->proxies_.size(),
    cmd_id_,
    ch->gra_.size());
  verify(ch->proxies_.size() == ch->gra_.FindV(
           cmd_id_)->data_->servers_.size());

  ChopFinishRequest req;
  req.txn_id = cmd_id_;
  req.gra    = ch->gra_;

  verify(ch->gra_.size() > 0);
  verify(req.gra.size() > 0);

  for (auto& rp : ch->proxies_) {
    RococoProxy *proxy = commo_->vec_rpc_proxy_[rp];
    Future::safe_release(proxy->async_rcc_finish_txn(req, fuattr));
  }
}

void RCCCoord::deptran_start_ro(TxnChopper *ch) {
  // new txn id for every new and retry.
  RequestHeader header = gen_header(ch);

  int pi;

  std::vector<Value> *input = nullptr;
  int32_t server_id;
  int     res;
  int     output_size;

  while ((res =
            ch->next_piece(input, output_size, server_id, pi,
                           header.p_type)) == 0) {
    header.pid = next_pie_id();

    rrr::FutureAttr fuattr;

    // remember this a asynchronous call! variable funtional range is important!
    fuattr.callback = [ch, pi, this, header](Future * fu) {
      {
        ScopedLock(this->mtx_);

        std::vector<Value> res;
        fu->get_reply() >> res;

        Log::debug("receive deptran RO start response, tid: %llx, pid: %llx, ",
                   header.tid,
                   header.pid);

        ch->n_started_++;

        if (ch->read_only_start_callback(pi, NULL, res)) this->deptran_start_ro(
            ch);
        else if (ch->n_started_ == ch->n_pieces_) {
          ch->read_only_reset();
          this->deptran_finish_ro(ch);
        }
      }
    };

    RococoProxy *proxy = commo_->vec_rpc_proxy_[server_id];
    Log::debug("send deptran RO start request, tid: %llx, pid: %llx",
               cmd_id_,
               header.pid);
    verify(input != nullptr);
    Future::safe_release(proxy->async_rcc_ro_start_pie(header, *input, fuattr));
  }
}

void RCCCoord::deptran_finish_ro(TxnChopper *ch) {
  RequestHeader header = gen_header(ch);
  int pi;

  std::vector<Value> *input = nullptr;
  int32_t server_id;
  int     res;
  int     output_size;

  while ((res =
            ch->next_piece(input, output_size, server_id, pi,
                           header.p_type)) == 0) {
    header.pid = next_pie_id();

    rrr::FutureAttr fuattr;

    // remember this a asynchronous call! variable funtional range is important!
    fuattr.callback = [ch, pi, this, header](Future * fu) {
      bool callback = false;
      {
        ScopedLock(this->mtx_);

        int res;
        std::vector<Value> output;
        fu->get_reply() >> output;

        Log::debug(
          "receive deptran RO start response (phase 2), tid: %llx, pid: %llx, ",
          header.tid,
          header.pid);

        ch->n_started_++;
        bool do_next_piece = ch->read_only_start_callback(pi, &res, output);

        if (do_next_piece) deptran_finish_ro(ch);
        else if (ch->n_started_ == ch->n_pieces_) {
          if (res == SUCCESS) {
            ch->reply_.res_ = SUCCESS;
            callback        = true;
          }
          else if (ch->can_retry()) {
            ch->read_only_reset();
            double last_latency = ch->last_attempt_latency();

            if (ccsi_) ccsi_->txn_retry_one(this->thread_id_,
                                            ch->txn_type_,
                                            last_latency);
            this->deptran_finish_ro(ch);
            callback = false;
          }
          else {
            ch->reply_.res_ = REJECT;
            callback        = true;
          }
        }
      }

      if (callback) {
        // generate a reply and callback.
        Log::debug("deptran RO callback, %llx", cmd_id_);
        TxnReply& txn_reply_buf = ch->get_reply();
        double    last_latency  = ch->last_attempt_latency();
        this->report(txn_reply_buf, last_latency
#ifdef TXN_STAT
                     , ch
#endif // ifdef TXN_STAT
                     );
        ch->callback_(txn_reply_buf);
        delete ch;
      }
    };

    RococoProxy *proxy = commo_->vec_rpc_proxy_[server_id];
    Log::debug("send deptran RO start request (phase 2), tid: %llx, pid: %llx",
               cmd_id_,
               header.pid);
    verify(input != nullptr);
    Future::safe_release(proxy->async_rcc_ro_start_pie(header, *input, fuattr));
  }
}

void RCCCoord::do_one(TxnRequest& req) {
  // pre-process
  ScopedLock(this->mtx_);
  TxnChopper *ch = Frame().CreateChopper(req);
  cmd_id_ = this->next_txn_id();

  Log::debug("do one request");

  if (ccsi_) ccsi_->txn_start_one(thread_id_, ch->txn_type_);

  if (recorder_) {
    std::string log_s;
    req.get_log(cmd_id_, log_s);
    std::function<void(void)> start_func = [this, ch]() {
      if (ch->is_read_only()) deptran_start_ro(ch);
      else {
        if (batch_optimal_) deptran_batch_start(ch);
        else deptran_start(ch);
      }
    };
    recorder_->submit(log_s, start_func);
  } else {
    if (ch->is_read_only()) deptran_start_ro(ch);
    else {
      if (batch_optimal_) deptran_batch_start(ch);
      else deptran_start(ch);
    }
  }
}
} // namespace rococo
