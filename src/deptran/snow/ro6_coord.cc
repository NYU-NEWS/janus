#include "marshal-value.h"
#include "ro6_coord.h"
#include "frame.h"
#include "tx.h"
#include "compress.h"
#include "benchmark_control_rpc.h"

namespace janus {

void RO6Coord::deptran_start(TxData *ch) {
  // new txn id for every new and retry.
//  RequestHeader header = gen_header(ch);

  int pi;
//
//  map<int32_t, Value> *input = nullptr;
//  int32_t server_id;
//  int     res;
//  int     output_size;
//
//  SimpleCommand *subcmd = nullptr;
//  while ((subcmd = (SimpleCommand*) cmd_->GetNextReadySubCmd()) != nullptr) {
//    header.pid = next_pie_id();
//
//    rrr::FutureAttr fuattr;
//
//    // remember this a asynchronous call! variable funtional range is important!
//    fuattr.callback = [ch, pi, this, header, subcmd](Future * fu) {
//      bool early_return = false;
//      {
//        //     Log::debug("try locking at start response, tid: %llx, pid: %llx",
//        // header.tid, header.pid);
//        std::lock_guard<std::recursive_mutex> lock(this->mtx_);
//
//        ChopStartResponse  res;
//        fu->get_reply() >> res;
//
//        if (IS_MODE_RO6) {
//          std::vector<i64> ro;
//          Compressor::string_to_vector(res.read_only, &ro);
//
//          this->ro_txn_.insert(ro.begin(), ro.end());
//        }
//
//        // where should I store this graph?
//        Graph<TxnInfo>& gra = *(res.gra_m.gra);
//        Log::debug("start response graph size: %d", (int)gra.size());
//        verify(gra.size() > 0);
//        ch->gra_.Aggregate(gra);
//
//        Log_debug(
//          "receive deptran start response, tid: %llx, pid: %llx, graph size: %d",
//          subcmd->root_id_,
//          subcmd->id_,
//          gra.size());
//
//        if (gra.size() > 1) ch->disable_early_return();
//
//        ch->n_pieces_out_++;
//
//        if (ch->start_callback(pi, res)) this->deptran_start(ch);
//        else if (ch->n_pieces_out_ == ch->GetNPieceAll()) {
//          this->deptran_finish(ch);
//
//          if (ch->do_early_return()) {
//            early_return = true;
//          }
//        }
//      }
//
//      if (early_return) {
//        ch->reply_.res_ = SUCCESS;
//        TxReply& txn_reply_buf = ch->get_reply();
//        double    last_latency  = ch->last_attempt_latency();
//        this->report(txn_reply_buf, last_latency
//#ifdef TXN_STAT
//                     , ch
//#endif // ifdef TXN_STAT
//                     );
//        ch->callback_(txn_reply_buf);
//      }
//    };
//
//    RococoProxy *proxy = (RococoProxy*)comm()->rpc_proxies_[server_id];
//    Log::debug("send deptran start request, tid: %llx, pid: %llx",
//               cmd_->id_,
//               header.pid);
//        verify(input != nullptr);
//    Future::safe_release(proxy->async_rcc_start_pie(*subcmd, fuattr));
//  }
}

/** caller should be thread safe */
void RO6Coord::deptran_finish(TxData *ch) {
//  Log::debug("deptran finish, %llx", cmd_->id_);
//
//  // commit or abort piece
//  rrr::FutureAttr fuattr;
//  fuattr.callback = [ch, this](Future * fu) {
//    int e = fu->get_error_code();
//        verify(e == 0);
//
//    bool callback = false;
//    {
//      std::lock_guard<std::recursive_mutex> lock(this->mtx_);
//      n_finish_ack_++;
//
//      ChopFinishResponse res;
//
//      Log::debug("receive finish response. tid: %llx", cmd_->id_);
//
//      fu->get_reply() >> res;
//
//      if (n_finish_ack_ == ch->GetPartitionIds().size()) {
//        ch->finish_callback(res);
//        callback = true;
//      }
//    }
//
//    if (callback) {
//      // generate a reply and callback.
//      Log::debug("deptran callback, %llx", cmd_->id_);
//
//      if (!ch->do_early_return()) {
//        ch->reply_.res_ = SUCCESS;
//        TxReply& txn_reply_buf = ch->get_reply();
//        double    last_latency  = ch->last_attempt_latency();
//        this->report(txn_reply_buf, last_latency
//#ifdef TXN_STAT
//                     , ch
//#endif // ifdef TXN_STAT
//                     );
//        ch->callback_(txn_reply_buf);
//      }
//      delete ch;
//    }
//  };
//
//  Log_debug(
//    "send deptran finish requests to %d servers, tid: %llx, graph size: %d",
//    (int)ch->partition_ids_.size(),
//    cmd_->id_,
//    ch->gra_.size());
//  verify(ch->partition_ids_.size() == ch->gra_.FindV(
//           cmd_->id_)->data_->servers_.size());
//
//  ChopFinishRequest req;
//  req.txn_id = cmd_->id_;
//  req.gra    = ch->gra_;
//
//  if (IS_MODE_RO6) {
//    // merge the read_only list.
//    std::vector<i64> ro;
//    ro.insert(ro.begin(), ro_txn_.begin(), ro_txn_.end());
//    Compressor::string_to_vector(req.read_only, &ro);
//  }
//
//  verify(ch->gra_.size() > 0);
//  verify(req.gra.size() > 0);
//
//  for (auto& rp : ch->partition_ids_) {
//    RococoProxy *proxy = (RococoProxy*)comm()->rpc_proxies_[rp];
//    Future::safe_release(proxy->async_rcc_finish_txn(req, fuattr));
//  }
}

void RO6Coord::ro6_start_ro(TxData *ch) {
//  // new txn id for every new and retry.
//  RequestHeader header = gen_header(ch);
//
//  int pi;
//
//  map<int32_t, Value> *input = nullptr;
//  int32_t server_id;
//  int     res;
//  int     output_size;
//
//  SimpleCommand *subcmd = nullptr;
//  while ((subcmd = (SimpleCommand*) cmd_->GetNextReadySubCmd()) != nullptr) {
//    header.pid = next_pie_id();
//
//    rrr::FutureAttr fuattr;
//
//    // remember this a asynchronous call! variable funtional range is important!
//    fuattr.callback = [ch, pi, this, header](Future * fu) {
//      {
//        std::lock_guard<std::recursive_mutex> lock(this->mtx_);
//
//        map<int32_t, Value> res;
//        fu->get_reply() >> res;
//
//        Log::debug("receive deptran RO start response, tid: %llx, pid: %llx, ",
//                   header.tid,
//                   header.pid);
//
//        ch->n_pieces_out_++;
//
//        if (ch->read_only_start_callback(pi, NULL, res)) {
//          this->ro6_start_ro(ch);
//        } else if (ch->n_pieces_out_ == ch->GetNPieceAll()) {
//          // job finish here.
//          ch->reply_.res_ = SUCCESS;
//
//          // generate a reply and callback.
//          Log::debug("snow RO callback, %llx", cmd_->id_);
//          TxReply& txn_reply_buf = ch->get_reply();
//          double    last_latency  = ch->last_attempt_latency();
//          this->report(txn_reply_buf, last_latency
//#ifdef TXN_STAT
//                       , ch
//#endif // ifdef TXN_STAT
//                       );
//          ch->callback_(txn_reply_buf);
//          delete ch;
//
//          //                    ch->read_only_reset();
//          //                    this->deptran_finish_ro(ch);
//          // TODO add a finish request to free the data structure on server.
//        }
//      }
//    };
//
//    RococoProxy *proxy = (RococoProxy*)comm()->rpc_proxies_[server_id];
//    Log::debug("send deptran RO start request, tid: %llx, pid: %llx",
//               cmd_->id_,
//               header.pid);
//    verify(input != nullptr);
//    Future::safe_release(proxy->async_rcc_ro_start_pie(*subcmd, fuattr));
//  }
}

void RO6Coord::do_one(TxRequest & req) {
  // pre-process
  std::lock_guard<std::recursive_mutex> lock(this->mtx_);

//  TxData *ch = frame_->CreateChopper(req, txn_reg_);
//  cmd_->id_ = this->next_txn_id();
//
//  Log::debug("do one request");
//
//  if (ccsi_) ccsi_->txn_start_one(thread_id_, ch->type_);
//
//  verify(!batch_optimal_);
//
//  if (recorder_) {
//    std::string log_s;
//    req.get_log(cmd_->id_, log_s);
//    std::function<void(void)> start_func = [this, ch]() {
//      if (ch->IsReadOnly()) ro6_start_ro(ch);
//      else {
//        deptran_start(ch);
//      }
//    };
//    recorder_->submit(log_s, start_func);
//  } else {
//    if (ch->IsReadOnly()) ro6_start_ro(ch);
//    else {
//        deptran_start(ch);
//    }
//  }
}
} // namespace janus
