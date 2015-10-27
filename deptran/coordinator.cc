#include "marshal-value.h"
#include "coordinator.h"
#include "frame.h"
#include "sharding.h"
#include "txn_req_factory.h"
#include "txn-chopper-factory.h"
#include "benchmark_control_rpc.h"

/**
 * What shoud we do to change this to asynchronous?
 * 1. Fisrt we need to have a queue to hold all transaction requests.
 * 2. pop next request, send start request for each piece until there is no
 *available left.
 *          in the callback, send the next piece of start request.
 *          if responses to all start requests are collected.
 *              send the finish request
 *                  in callback, remove it from queue.
 *
 *
 */

namespace rococo {
Coordinator::Coordinator(uint32_t coo_id,
                         std::vector<std::string> &addrs,
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
                                               site_piece_(addrs.size(), 0),
                                               mtx_(),
                                               start_mtx_() {
  uint64_t k = coo_id_;

  k <<= 32;
  k++;
  commo_ = new Commo(addrs);
  this->next_pie_id_.store(k);
  this->next_txn_id_.store(k);
  recorder_ = NULL;
  retry_wait_ = Config::GetConfig()->retry_wait();
}

// TODO obsolete
RequestHeader Coordinator::gen_header(TxnChopper *ch) {
  RequestHeader header;

  header.cid = coo_id_;
  header.tid = cmd_id_;
  header.t_type = ch->txn_type_;
  return header;
}

BatchRequestHeader Coordinator::gen_batch_header(TxnChopper *ch) {
  BatchRequestHeader batch_header;

  batch_header.t_type = ch->txn_type_;
  batch_header.cid = coo_id_;
  batch_header.tid = cmd_id_;
  return batch_header;
}

/** thread safe */
void Coordinator::do_one(TxnRequest &req) {
  // pre-process
  ScopedLock(this->mtx_);
  TxnChopper *ch = Frame().CreateChopper(req);
  cmd_ = ch;
  cmd_id_ = this->next_txn_id();
  cleanup(); // In case of reuse.

  // turn off batch now
  // if (batch_optimal_)
  //     batch_start(ch);

  Log::debug("do one request");

  if (ccsi_) ccsi_->txn_start_one(thread_id_, ch->txn_type_);

  switch (mode_) {
    case MODE_OCC:
    case MODE_2PL:
      //        if (recorder_) {
      if (false) {
//        std::string log_s;
//        req.get_log(ch->txn_id_, log_s);
//        std::function<void(void)> start_func = [this, ch]() {
//          if (batch_optimal_) {
//            naive_batch_start(ch);
//          } else {
//            LegacyStart(ch);
//          }
//        };
//        recorder_->submit(log_s, start_func);
      } else {
        Start();
      }
      break;
    case MODE_RPC_NULL:
      rpc_null_start(ch);
      break;
    case MODE_RCC:
    case MODE_RO6:
    case MODE_NONE:
    default:
      verify(0);
  }
  // finish request is triggered in the callback of start request.
}

void Coordinator::rpc_null_start(TxnChopper *ch) {
  rrr::FutureAttr fuattr;

  fuattr.callback = [ch, this](Future *fu) {
    ch->reply_.res_ = SUCCESS;
    TxnReply &txn_reply_buf = ch->get_reply();
    double last_latency = ch->last_attempt_latency();
    this->report(txn_reply_buf, last_latency
#ifdef TXN_STAT
        , ch
#endif // ifdef TXN_STAT
    );
    ch->callback_(txn_reply_buf);
    delete ch;
  };

  RococoProxy *proxy = commo_->vec_rpc_proxy_[coo_id_ % commo_->vec_rpc_proxy_.size()];
  Future::safe_release(proxy->async_rpc_null(fuattr));
}

void Coordinator::cleanup() {
  cmd_map_.clear();
  start_ack_map_.clear();
  TxnChopper *ch = (TxnChopper*) cmd_;
  if (ch) ch->n_start_sent_ = 0; // TODO remove
}

void Coordinator::restart(TxnChopper *ch) {
  ScopedLock(this->mtx_);
  cleanup();
  ch->retry();

  double last_latency = ch->last_attempt_latency();

  if (ccsi_) ccsi_->txn_retry_one(this->thread_id_, ch->txn_type_, last_latency);

//  if (batch_optimal_) naive_batch_start(ch);
//  else LegacyStart(ch);
  Start();
}

void Coordinator::Start() {
  std::lock_guard<std::mutex> lock(start_mtx_);
  StartRequest req;
  req.cmd_id = cmd_id_;
  Command* subcmd;
  phase_t phase = phase_;
  std::function<void(StartReply&)> callback = [this, phase] (StartReply &reply) {
    this->StartAck(reply, phase);
  };
  while ((subcmd = cmd_->GetNextSubCmd(cmd_map_)) != nullptr) {
    req.pie_id = next_pie_id();
    Log_debug("send out start request, "
                  "cmd_id: %lx, inn_id: %d, pie_id: %lx",
              cmd_id_, subcmd->inn_id_, req.pie_id);
    verify(cmd_map_.find(subcmd->inn_id_) == cmd_map_.end());
//    auto &cmd = cmd_map_[subcmd->inn_id_];
//    verify(cmd == nullptr);
    cmd_map_[subcmd->inn_id_] = subcmd;
    req.cmd = subcmd;
    commo_->SendStart(subcmd->GetPar(), req, this, callback);
  }
}

bool Coordinator::AllStartAckCollected() {
  for (auto &pair: start_ack_map_) {
    if (!pair.second) return false;
  }
  return true;
}

void Coordinator::StartAck(StartReply &reply, const phase_t &phase) {
  ScopedLock(this->mtx_);
  if (phase != phase_) return;

  TxnChopper *ch = (TxnChopper *)cmd_;
  ch->n_started_++; // TODO replace this
  ch->n_start_sent_--;

  Log_debug("get start ack for cmd_id: %lx, inn_id: %d",
            cmd_id_, reply.cmd->inn_id_);
  start_ack_map_[reply.cmd->inn_id_] = true;
  if (reply.res == REJECT) {
    ch->commit_.store(false);
  }
  if (!ch->commit_.load()) {
    if (ch->n_start_sent_ == 0) {
      phase_++;
      this->Finish();
    }
  } else {
    cmd_->Merge(*reply.cmd);
    if (cmd_->HasMoreSubCmd(cmd_map_)) {
      Log_debug("command has more sub-cmd, cmd_id: %lx,"
                    " n_started_: %d, n_pieces: %d",
                cmd_id_, ch->n_started_, ch->n_pieces_);
      Start();
    } else if (AllStartAckCollected()) {
      Log_debug("receive all start acks, txn_id: %lx", cmd_id_);
      phase_++;
      Prepare();
    } else {
      // skip
    }
  } 
}

//
//void Coordinator::LegacyStart(TxnChopper *ch) {
//  // new txn id for every new and retry.
//  RequestHeader header = gen_header(ch);
//  int pi;
//  std::vector<Value> *input;
//  int32_t server_id;
//  int res;
//  int output_size;
//  while ((res = ch->next_piece(input,
//                               output_size,
//                               server_id,
//                               pi,
//                               header.p_type)) == 0) {
//    header.pid = next_pie_id();
//
//    std::function<void(Future*)> callback = [ch, pi, this](Future *fu) {
//      this->LegacyStartAck(ch, pi, fu);
//    };
//
//    // remember this a asynchronous call!
//    // variable functional range is important!
//
//    commo_->SendStart(server_id, header, *input, output_size, callback);
//
//    ch->n_start_sent_++;
//    site_piece_[server_id]++;
//  }
//}

//
//void Coordinator::LegacyStartAck(TxnChopper *ch, int pi, Future *fu) {
//  ScopedLock(this->mtx_);
//
//  int res;
//  std::vector<mdb::Value> output;
//  fu->get_reply() >> res >> output;
//  ch->n_started_++;
//  ch->n_start_sent_--;
//
//  if (res == REJECT) {
//    verify(this->mode_ == MODE_2PL);
//    ch->commit_.store(false);
//  }
//
//  if (!ch->commit_.load()) {
//    if (ch->n_start_sent_ == 0) {
//      this->finish(ch);
//    }
//  } else {
//    if (ch->start_callback(pi, res, output)) {
//      this->LegacyStart(ch);
//    } else if (ch->n_started_ == ch->n_pieces_) {
//      this->Prepare(ch);
//    }
//  }
//}

void Coordinator::naive_batch_start(TxnChopper *ch) {

}

/** caller should be thread_safe */
void Coordinator::Prepare() {
  TxnChopper *ch = (TxnChopper*) cmd_;
  verify(mode_ == MODE_OCC || mode_ == MODE_2PL);
  RequestHeader header = gen_header(ch);

  // prepare piece, currently only useful for OCC
  std::function<void(Future*)> callback = [ch, this] (Future *fu) {
    this->PrepareAck(ch, fu);
  };

  std::vector<i32> sids;
//  sids.reserve(ch->proxies_.size());

  for (auto &rp : ch->proxies_) {
    sids.push_back(rp);
  }
  for (auto &rp : ch->proxies_) {
    Log::debug("send prepare tid: %ld", header.tid);
    commo_->SendPrepare(rp, header.tid, sids, callback); 
    site_prepare_[rp]++;
  }
}

void Coordinator::PrepareAck(TxnChopper *ch, Future *fu) {
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
    this->Finish();
  }
}

/** caller should be thread safe */
void Coordinator::Finish() {
  TxnChopper *ch = (TxnChopper*)cmd_;
  verify(mode_ == MODE_OCC || mode_ == MODE_2PL);

  Log_debug("send out finish request, cmd_id: %lx", cmd_id_);
  // commit or abort piece
  rrr::FutureAttr fuattr;
  fuattr.callback = [ch, this](Future *fu) {
    this->FinishAck(ch, fu);
  };

  if (ch->commit_.load()) {
    Log::debug("send finish");
    ch->reply_.res_ = SUCCESS;

    for (auto &rp : ch->proxies_) {
      RococoProxy *proxy = commo_->vec_rpc_proxy_[rp];
      Future::safe_release(proxy->async_commit_txn(cmd_id_, fuattr));
      site_commit_[rp]++;
    }
  } else {
    Log::debug("send abort");
    ch->reply_.res_ = REJECT;

    for (auto &rp : ch->proxies_) {
      RococoProxy *proxy = commo_->vec_rpc_proxy_[rp];
      Future::safe_release(proxy->async_abort_txn(cmd_id_, fuattr));
      site_abort_[rp]++;
    }
  }
}

void Coordinator::FinishAck(TxnChopper *ch, Future *fu) {
  bool callback = false;
  bool retry = false;
  {
    ScopedLock(this->mtx_);
    ch->n_finished_++;

    Log::debug("finish");

    if (ch->n_finished_ == ch->proxies_.size()) {
      if ((ch->reply_.res_ == REJECT) && ch->can_retry()) {
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
    //            sleep_time.tv_nsec = 0;//RandomGenerator::rand(0, 1000 *
    // 1000 * 1000 - 1);
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
#endif // ifdef TXN_STAT
    );

    // if (retry_wait_ && txn_reply_buf.res_ != SUCCESS) {
    //    struct timespec sleep_time, buf;
    //    sleep_time.tv_sec = 0;
    //    sleep_time.tv_nsec = RandomGenerator::rand(1000 * 1000, 10 * 1000 *
    // 1000);
    //    nanosleep(&sleep_time, &buf);
    // }
    ch->callback_(txn_reply_buf);
    delete ch;
  }
}

void Coordinator::report(TxnReply &txn_reply,
                         double last_latency
#ifdef TXN_STAT
    , TxnChopper *ch
#endif // ifdef TXN_STAT
) {
  if (ccsi_) {
    if (txn_reply.res_ == SUCCESS) {
#ifdef TXN_STAT
      txn_stats_[ch->txn_type_].one(ch->proxies_.size(), ch->p_types_);
#endif // ifdef TXN_STAT
      ccsi_->txn_success_one(thread_id_,
                             txn_reply.txn_type_,
                             txn_reply.start_time_,
                             txn_reply.time_,
                             last_latency,
                             txn_reply.n_try_);
    }
    else
      ccsi_->txn_reject_one(thread_id_,
                            txn_reply.txn_type_,
                            txn_reply.start_time_,
                            txn_reply.time_,
                            last_latency,
                            txn_reply.n_try_);
  }
}
} // namespace rococo
