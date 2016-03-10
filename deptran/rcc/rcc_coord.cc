#include "marshal-value.h"
#include "rcc_coord.h"
#include "frame.h"
#include "dtxn.h"
#include "txn-chopper-factory.h"
#include "benchmark_control_rpc.h"

namespace rococo {

void RccCoord::Handout() {
  std::lock_guard<std::recursive_mutex> lock(mtx_);
  phase_++;
  // new txn id for every new and retry.
//  RequestHeader header = gen_header(ch);

  int pi;

  map<int32_t, Value> *input = nullptr;
  int32_t server_id;
  int     res;
  int     output_size;

  int cnt;
  while (cmd_->HasMoreSubCmdReadyNotOut()) {
    auto subcmd = (SimpleCommand*) cmd_->GetNextReadySubCmd();
    subcmd->id_ = next_pie_id();
    verify(subcmd->root_id_ == cmd_->id_);
    n_handout_++;
    cnt++;
    Log_debug("send out start request %ld, cmd_id: %lx, inn_id: %d, pie_id: %lx",
              n_handout_, cmd_->id_, subcmd->inn_id_, subcmd->id_);
    handout_acks_[subcmd->inn_id()] = false;
    commo()->SendHandout(*subcmd,
                         this,
                         std::bind(&ThreePhaseCoordinator::HandoutAck,
                                   this,
                                   phase_,
                                   std::placeholders::_1,
                                   std::placeholders::_2,
                                   std::placeholders::_3));
  }
}

void RccCoord::HandoutAck(phase_t phase,
                          int res,
                          Command& cmd,
                          RccGraph& graph) {
  std::lock_guard<std::recursive_mutex> lock(this->mtx_);
  verify(phase == phase_);
  n_handout_ack_++;
  TxnCommand *txn = (TxnCommand *) cmd_;
  handout_acks_[cmd.inn_id_] = true;

  Log_debug("get start ack %ld/%ld for cmd_id: %lx, inn_id: %d",
            n_handout_ack_, n_handout_, cmd_->id_, cmd.inn_id_);

  bool early_return = false;

  // where should I store this graph?
  Log::debug("start response graph size: %d", (int)graph.size());
  verify(graph.size() > 0);
  graph_.Aggregate(graph);

  Log_debug(
      "receive deptran start response, tid: %llx, pid: %llx, graph size: %d",
      subcmd->root_id_,
      subcmd->id_,
      gra.size());

  // TODO?
  if (graph.size() > 1) txn->disable_early_return();

  ch->n_pieces_out_++;
  cmd_->Merge(cmd);

  if (cmd_->HasMoreSubCmdReadyNotOut()) {
    Log_debug("command has more sub-cmd, cmd_id: %lx,"
                  " n_started_: %d, n_pieces: %d",
              cmd_->id_, ch->n_pieces_out_, ch->GetNPieceAll());
    Handout();
  } else if (AllHandoutAckReceived()) {
    Log_debug("receive all start acks, txn_id: %ld; START PREPARE", cmd_->id_);
    Finish();
    // TODO?
    if (ch->do_early_return()) {
      early_return = true;
    }
    //
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
  }
}

/** caller should be thread safe */
void RccCoord::Finish() {
        verify(IS_MODE_RCC || IS_MODE_RO6);
  Log::debug("deptran finish, %llx", cmd_->id_);
  TxnCommand *ch = (TxnCommand*) cmd_;
  // commit or abort piece
  rrr::FutureAttr fuattr;

  Log_debug(
    "send deptran finish requests to %d servers, tid: %llx, graph size: %d",
    (int)ch->partition_ids_.size(),
    cmd_->id_,
    ch->gra_.size());
  verify(ch->partition_ids_.size() == ch->gra_.FindV(
           cmd_->id_)->data_->servers_.size());

  ChopFinishRequest req;
  req.txn_id = cmd_->id_;
  req.gra    = ch->gra_;

  verify(ch->gra_.size() > 0);
  verify(req.gra.size() > 0);

  for (auto& rp : ch->partition_ids_) {
    RococoProxy *proxy = (RococoProxy*)comm()->rpc_proxies_[rp];
    Future::safe_release(proxy->async_rcc_finish_txn(req, fuattr));
  }
}

void RccCoord::FinishAck(phase_t phase, int res) {
  TxnCommand* ch = (TxnCommand*) cmd_;
  verify(phase_ == phase);
  bool callback = false;
  std::lock_guard<std::recursive_mutex> lock(this->mtx_);
  n_finish_ack_++;

  Log::debug("receive finish response. tid: %llx", cmd_->id_);

  if (n_finish_ack_ == ch->GetPartitionIds().size()) {
    ch->finish_callback(res);
    callback = true;
  }

  if (callback) {
    // generate a reply and callback.
    Log_debug("deptran callback, %llx", cmd_->id_);

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
}

void RccCoord::HandoutRo() {
  // new txn id for every new and retry.
  int pi;

  map<int32_t, Value> *input = nullptr;
  int32_t server_id;
  int     res;
  int     output_size;

  SimpleCommand *subcmd;
  while ((subcmd = (SimpleCommand*) cmd_->GetNextReadySubCmd()) != nullptr) {
    header.pid = next_pie_id();

    rrr::FutureAttr fuattr;

    // remember this a asynchronous call! variable funtional range is important!
    fuattr.callback = [ch, pi, this, header](Future * fu) {
      {

      }
    };

    RococoProxy *proxy = (RococoProxy*)comm()->rpc_proxies_[server_id];
    Log::debug("send deptran RO start request, tid: %llx, pid: %llx",
               cmd_->id_,
               header.pid);
    verify(input != nullptr);
    Future::safe_release(proxy->async_rcc_ro_start_pie(*subcmd, fuattr));
  }
}

void RccCoord::HandoutRoAck() {
  std::lock_guard<std::recursive_mutex> lock(this->mtx_);

  map<int32_t, Value> output;
  fu->get_reply() >> output;

  Log::debug("receive deptran RO start response, tid: %llx, pid: %llx, ",
             header.tid,
             header.pid);

  ch->n_pieces_out_++;

  if (ch->read_only_start_callback(pi, NULL, output)) {
    this->deptran_start_ro(ch);
  } else if (ch->n_pieces_out_ == ch->GetNPieceAll()) {
    ch->read_only_reset();
    this->deptran_finish_ro(ch);
  }
}

void RccCoord::deptran_finish_ro(TxnCommand *ch) {
  int pi;

  map<int32_t, Value> *input = nullptr;
  int32_t server_id;
  int     res;
  int     output_size;

  SimpleCommand *subcmd = nullptr;
  while ((subcmd = (SimpleCommand*) cmd_->GetNextReadySubCmd()) != nullptr) {
    header.pid = next_pie_id();

    rrr::FutureAttr fuattr;

    // remember this a asynchronous call! variable funtional range is important!
    fuattr.callback = [ch, pi, this, header](Future * fu) {
      bool callback = false;
      {
        std::lock_guard<std::recursive_mutex> lock(this->mtx_);

        int res;
        map<int32_t, Value> output;
        fu->get_reply() >> output;

        Log::debug(
          "receive deptran RO start response (phase 2), tid: %llx, pid: %llx, ",
          header.tid,
          header.pid);

        ch->n_pieces_out_++;
        bool do_next_piece = ch->read_only_start_callback(pi, &res, output);

        if (do_next_piece) deptran_finish_ro(ch);
        else if (ch->n_pieces_out_ == ch->GetNPieceAll()) {
          if (res == SUCCESS) {
            ch->reply_.res_ = SUCCESS;
            callback        = true;
          }
          else if (ch->can_retry()) {
            ch->read_only_reset();
            double last_latency = ch->last_attempt_latency();

            if (ccsi_) ccsi_->txn_retry_one(this->thread_id_,
                                            ch->type_,
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
        Log::debug("deptran RO callback, %llx", cmd_->id_);
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

    RococoProxy *proxy = (RococoProxy*)comm()->rpc_proxies_[server_id];
    Log::debug("send deptran RO start request (phase 2), tid: %llx, pid: %llx",
               cmd_->id_,
               header.pid);
    verify(input != nullptr);
    Future::safe_release(proxy->async_rcc_ro_start_pie(*subcmd, fuattr));
  }
}

void RccCoord::do_one(TxnRequest& req) {
  // pre-process
  std::lock_guard<std::recursive_mutex> lock(this->mtx_);
  TxnCommand *ch = frame_->CreateChopper(req, txn_reg_);
  verify(txn_reg_ != nullptr);
  cmd_ = ch;
  cmd_->id_ = this->next_txn_id();
  cmd_->root_id_ = this->next_txn_id();
  Reset();
  Log_debug("do one request");

  if (ccsi_) ccsi_->txn_start_one(thread_id_, cmd_->type_);

  if (recorder_) {
    std::string log_s;
    req.get_log(cmd_->id_, log_s);
    std::function<void(void)> start_func = [this, ch]() {
      if (ch->is_read_only())
        HandoutRo(ch);
      else
        Handout(ch);
    };
    recorder_->submit(log_s, start_func);
  } else {
    if (ch->is_read_only()) HandoutRo(ch);
  }
}
} // namespace rococo
