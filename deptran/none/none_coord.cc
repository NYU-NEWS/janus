
#include "none_coord.h"

namespace rococo {

/** thread safe */
void NoneCoord::do_one(TxnRequest &req) {
  // pre-process
  ScopedLock(this->mtx_);
  TxnChopper *ch = TxnChopperFactory::gen_chopper(req, benchmark_);
  ch->txn_id_ = this->next_txn_id();

  Log::debug("do one request");

  if (ccsi_) ccsi_->txn_start_one(thread_id_, ch->txn_type_);

  LegacyStart(ch);
}

void NoneCoord::LegacyStart(TxnChopper* ch) {
  // new txn id for every new and retry.
  RequestHeader header = gen_header(ch);

  int pi;

  std::vector<Value> *input;
  int32_t server_id;
  int     res;
  int     output_size;

  while ((res = ch->next_piece(input,
                               output_size,
                               server_id,
                               pi,
                               header.p_type)) == 0) {
    header.pid = next_pie_id();

    rrr::FutureAttr fuattr;

    // remember this a asynchronous call!
    // variable functional range is important!
    fuattr.callback = [ch, pi, this](Future * fu) {
      bool callback = false;
      {
        ScopedLock(this->mtx_);

        int res;
        std::vector<mdb::Value> output;
        fu->get_reply() >> res >> output;
        ch->n_started_++;
        ch->n_start_sent_--;

//        LegacyCommandOutput output;
//        fu-get_reply() >> output;
//        ch->merge(output);

        if (res == REJECT) {
          verify(this->mode_ == MODE_2PL);
          ch->commit_.store(false);
        }

        if (!ch->commit_.load()) {
          if (ch->n_start_sent_ == 0) {
            this->finish(ch);
          }
        } else {
          if (ch->start_callback(pi, res, output)) {
            this->LegacyStart(ch);
          } else if (ch->n_started_ == ch->n_pieces_) {
            callback        = true;
            ch->reply_.res_ = SUCCESS;
          }
        }
      }

      if (callback) {
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

    RococoProxy *proxy = vec_rpc_proxy_[server_id];
    Future::safe_release(proxy->async_start_pie(header,
                                                *input,
                                                (i32)output_size,
                                                fuattr));
    ch->n_start_sent_++;
    site_piece_[server_id]++;
  }


}


} // namespace rococo
