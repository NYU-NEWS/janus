#pragma once

#include "__dep__.h"
#include "constants.h"
#include "txn_chopper.h"
//#include "all.h"
#include "msg.h"
#include "commo.h"
#include "client_worker.h"


namespace rococo {
class ClientControlServiceImpl;

class Coordinator {
 public:
  uint32_t coo_id_;
  int benchmark_;
  int32_t mode_;
  ClientControlServiceImpl *ccsi_;
  uint32_t thread_id_;
  bool batch_optimal_;
  bool retry_wait_;

  uint32_t n_start_ = 0;
  uint32_t n_start_ack_ = 0;
  uint32_t n_prepare_req_ = 0;
  uint32_t n_prepare_ack_ = 0;
  uint32_t n_finish_req_ = 0;
  uint32_t n_finish_ack_ = 0;

  std::atomic<uint64_t> next_pie_id_;
  std::atomic<uint64_t> next_txn_id_;

  Mutex mtx_;
  std::mutex start_mtx_;
  Commo *commo_;
  Recorder *recorder_;
  Command *cmd_; 
  cmdid_t cmd_id_;
  phase_t phase_ = 0;
//  map<innid_t, Command*> cmd_map_;
  map<innid_t, bool> start_ack_map_;
  Sharding* sharding_ = nullptr;

  std::vector<int> site_prepare_;
  std::vector<int> site_commit_;
  std::vector<int> site_abort_;
  std::vector<int> site_piece_;
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
  Coordinator(uint32_t coo_id,
              std::vector<std::string> &addrs,
              int benchmark,
              int32_t mode = MODE_OCC,
              ClientControlServiceImpl *ccsi = NULL,
              uint32_t thread_id = 0,
              bool batch_optimal = false);

  virtual ~Coordinator();

  /** thread safe */
  uint64_t next_pie_id() {
    return this->next_pie_id_++;
  }

  /** thread safe */
  uint64_t next_txn_id() {
    return this->next_txn_id_++;
  }

  /** return: SUCCESS or REJECT */

  // virtual int do_one() = 0;

  // /** return: SUCCESS or REJECT */
  // virtual int do_one(uint32_t &max_try) = 0; /** 0 TRY UNTIL SUCCESS; >=1 TRY
  // TIMES */

  /** do it asynchronously, thread safe.
   */
  virtual void do_one(TxnRequest &);
  virtual void cleanup();
  void restart(TxnChopper *ch);

  void Start();
  void StartAck(StartReply &reply, const phase_t &phase);
//  void LegacyStart(TxnChopper *ch);
//  void LegacyStartAck(TxnChopper *ch, int pi, Future *fu);
  void rpc_null_start(TxnChopper *ch);
  void naive_batch_start(TxnChopper *ch);
//  void batch_start(TxnChopper *ch);
  void Prepare();
  void PrepareAck(TxnChopper *ch, Future *fu);
  void Finish();
  void FinishAck(TxnChopper *ch, Future *fu);
  void Abort() {verify(0);}

  bool AllStartAckCollected();

    RequestHeader gen_header(TxnChopper *ch);

  BatchRequestHeader gen_batch_header(TxnChopper *ch);

  void report(TxnReply &txn_reply,
              double last_latency
#ifdef                                  TXN_STAT
  ,
  TxnChopper *ch
#endif /* ifdef TXN_STAT */
  );

  /* deprecated*/
  bool next_piece(std::vector<mdb::Value> **input,
                  RococoProxy **proxy,
                  int *pi,
                  int *p_type) {
    return false;
  }

  void start_callback(TxnRequest *req, int pi, int res,
                      std::vector<mdb::Value> &output) { }

  int exe_txn() {
    return 0;
  }
};
}
