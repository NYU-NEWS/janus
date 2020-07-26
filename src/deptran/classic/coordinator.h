//
// Created by shuai on 11/25/15.
//
#pragma once

#include "../coordinator.h"

namespace janus {
class ClientControlServiceImpl;

class CoordinatorClassic : public Coordinator {
 public:
  enum Phase { INIT_END = 0, DISPATCH = 1, PREPARE = 2, COMMIT = 3 };
  CoordinatorClassic(uint32_t coo_id,
                     int benchmark,
                     ClientControlServiceImpl* ccsi,
                     uint32_t thread_id);

  virtual ~CoordinatorClassic() {}

  inline TxData& tx_data() {
    return *(TxData*) cmd_;
  }

  Communicator* commo();

#ifdef TXN_STAT
  typedef struct txn_stat_t {
    uint64_t                             n_serv_tch;
    uint64_t                             n_txn;
    std::unordered_map<int32_t, uint64_t>piece_cnt;
    txn_stat_t() : n_serv_tch(0), n_txn(0) {}
    void one(uint64_t _n_serv_tch, const std::vector<int32_t>& pie) {
      n_serv_tch += _n_serv_tch;
      n_txn++;

      for (int i = 0; i < pie.size(); i++) {
        if (pie[i] != 0) {
          auto it = piece_cnt.find(pie[i]);

          if (it == piece_cnt.end()) {
            piece_cnt[pie[i]] = 1;
          } else {
            piece_cnt[pie[i]]++;
          }
        }
      }
    }

    void output() {
      Log::info("SERV_TCH: %lu, TXN_CNT: %lu, MEAN_SERV_TCH_PER_TXN: %lf",
                n_serv_tch, n_txn, ((double)n_serv_tch) / n_txn);

      for (auto& it : piece_cnt) {
        Log::info("\tPIECE: %d, PIECE_CNT: %lu, MEAN_PIECE_PER_TXN: %lf",
                  it.first, it.second, ((double)it.second) / n_txn);
      }
    }
  } txn_stat_t;
  std::unordered_map<int32_t, txn_stat_t> txn_stats_;
#endif /* ifdef TXN_STAT */

  /** thread safe */
  uint64_t next_pie_id() {
    return this->next_pie_id_++;
  }

  /** do it asynchronously, thread safe. */
  virtual void DoTxAsync(TxRequest&) override;
  virtual void Reset() override;
  void Restart() override;

  virtual void DispatchAsync();
  virtual void DispatchAck(phase_t phase,
                           int res,
                           map<innid_t, map<int32_t, Value>>& outputs);
  void Prepare();
  void PrepareAck(phase_t phase, int res);
  virtual void Commit();
  void CommitAck(phase_t phase);
  void Abort() {
    verify(0);
  }
  void End();
  void ReportCommit();

  bool AllDispatchAcked();
  virtual void GotoNextPhase();

  void Report(TxReply& txn_reply, double last_latency
#ifdef                                  TXN_STAT
  ,
  TxnChopper *ch
#endif /* ifdef TXN_STAT */
  );

  void ForwardTxnRequest(TxRequest& req);
  void ForwardTxRequestAck(const TxReply&);
  // for debug
  set<txnid_t> ___phase_one_tids_ = set<txnid_t>();
  set<txnid_t> ___phase_three_tids_ = set<txnid_t>();
  void ___TestPhaseOne(txnid_t txn_id);
  void ___TestPhaseThree(txnid_t txn_id);
};
} // namespace janus
