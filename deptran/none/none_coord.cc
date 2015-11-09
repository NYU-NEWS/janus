
#include "none_coord.h"
#include "frame.h"
#include "txn-chopper-factory.h"
#include "benchmark_control_rpc.h"

namespace rococo {

/** thread safe */
void NoneCoord::do_one(TxnRequest &req) {
  // pre-process
  std::lock_guard<std::mutex> lock(this->mtx_);
  TxnChopper *ch = Frame().CreateChopper(req);
  cmd_id_ = this->next_txn_id();

  Log::debug("do one request");

  if (ccsi_) ccsi_->txn_start_one(thread_id_, ch->txn_type_);

//  LegacyStart(ch);
  Start();
}

void NoneCoord::LegacyStartAck(TxnChopper *ch, int pi, Future *fu) {
  bool callback = false;
  {
    std::lock_guard<std::mutex> lock(this->mtx_);

    int res;
    std::vector<mdb::Value> output;
    fu->get_reply() >> res >> output;
    ch->n_pieces_out_++;

//        LegacyCommandOutput output;
//        fu-get_reply() >> output;
//        ch->merge(output);


    if (!ch->commit_.load()) {
      if (n_start_ == n_start_ack_) {
        this->Finish();
      }
    } else {
      if (ch->start_callback(pi, res, output)) {
//        this->LegacyStart(ch);
        Start();
      } else if (ch->n_pieces_out_ == ch->n_pieces_all_) {
        callback = true;
        ch->reply_.res_ = SUCCESS;
      }
    }
  }

  if (callback) {
    TxnReply &txn_reply_buf = ch->get_reply();
    double last_latency = ch->last_attempt_latency();
    this->report(txn_reply_buf, last_latency
#ifdef TXN_STAT
        , ch
#endif // ifdef TXN_STAT
    );
    ch->callback_(txn_reply_buf);
    delete ch;
  }
}


} // namespace rococo
