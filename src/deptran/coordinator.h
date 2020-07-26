#pragma once

#include "__dep__.h"
#include "constants.h"
#include "procedure.h"
//#include "all.h"
#include "msg.h"
#include "communicator.h"
// #include "client_worker.h"

namespace janus {
class ClientControlServiceImpl;

enum ForwardRequestState { NONE=0, PROCESS_FORWARD_REQUEST, FORWARD_TO_LEADER };

//class CoordinatorBase {
//public:
//  std::mutex mtx_;
//  uint32_t n_start_ = 0;
//  locid_t loc_id_ = -1;
//  virtual ~CoordinatorBase() = default;
//  // TODO do_one should be replaced with Submit.
//  virtual void DoTxAsync(TxRequest &) = 0;
//  virtual void Reset() = 0;
//  virtual void restart(TxData *ch) = 0;
//};

class Coordinator {
 public:
  static std::mutex _dbg_txid_lock_;
  static std::unordered_set<txid_t> _dbg_txid_set_;
  bool _inuse_{false};
  uint32_t n_start_ = 0;
  locid_t loc_id_ = -1;
  uint32_t coo_id_;
  parid_t par_id_ = -1;
  int benchmark_;
  ClientControlServiceImpl *ccsi_ = nullptr;
  uint32_t thread_id_;
  bool batch_optimal_ = false;
  bool retry_wait_;
  shared_ptr<IntEvent> sp_ev_commit_{};
  shared_ptr<IntEvent> sp_ev_done_{};

  std::atomic<uint64_t> next_pie_id_;
  std::atomic<uint64_t> next_txn_id_;

  std::recursive_mutex mtx_{};
  Recorder *recorder_{nullptr};
  CmdData *cmd_{nullptr};
  phase_t phase_ = 0;
  map<innid_t, bool> dispatch_acks_ = {};
  map<innid_t, bool> handout_outs_ = {};
  Sharding* sharding_ = nullptr;
  shared_ptr<TxnRegistry> txn_reg_{nullptr};
  Communicator* commo_ = nullptr;
  Frame* frame_ = nullptr;

  txid_t ongoing_tx_id_{0};
  ForwardRequestState forward_status_ = NONE;

  // should be reset on issuing a new request
  uint32_t n_retry_ = 0;
  // below should be reset on retry.
  bool committed_ = false;
  bool commit_reported_ = false;
  bool validation_result_{true};
  bool aborted_ = false;
  uint32_t n_dispatch_ = 0;
  uint32_t n_dispatch_ack_ = 0;
  uint32_t n_prepare_req_ = 0;
  uint32_t n_prepare_ack_ = 0;
  uint32_t n_finish_req_ = 0;
  uint32_t n_finish_ack_ = 0;
  std::vector<int> site_prepare_;
  std::vector<int> site_commit_;
  std::vector<int> site_abort_;
  std::vector<int> site_piece_;
  std::function<void()> commit_callback_ = [] () {verify(0);};
  std::function<void()> exe_callback_ = [] () {verify(0);};
  // above should be reset

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
              int benchmark,
              ClientControlServiceImpl *ccsi = NULL,
              uint32_t thread_id = 0);

  virtual ~Coordinator();

  /** thread unsafe */
  uint64_t next_pie_id() {
    return this->next_pie_id_++;
  }

  /** thread unsafe */
  uint64_t next_txn_id() {
    auto ret = next_txn_id_++;
#ifdef DEBUG_CHECK
    _dbg_txid_lock_.lock();
    verify(_dbg_txid_set_.count(ret) == 0);
    _dbg_txid_set_.insert(ret);
    _dbg_txid_lock_.unlock();
#endif
    return ret;
  }

  virtual void DoTxAsync(TxRequest &) = 0;
  virtual void Submit(shared_ptr<Marshallable>& cmd,
                      const std::function<void()>& commit_callback = [](){},
                      const std::function<void()>& exe_callback = [](){}) {
    verify(0);
  }
  virtual void Reset() {
    committed_ = false;
    commit_reported_ = false;
    aborted_ = false;
    n_dispatch_ = 0;
    n_dispatch_ack_ = 0;
    n_prepare_req_ = 0;
    n_prepare_ack_ = 0;
    n_finish_req_ = 0;
    n_finish_ack_ = 0;
  }
  virtual uint64_t GenerateTimestamp() {
    uint64_t t;
    switch(Config::GetConfig()->timestamp_) {
      case Config::TimestampType::CLOCK:
        t = std::time(nullptr);
        t = t << 32;
        t |= (uint64_t) coo_id_;
        break;
      case Config::TimestampType::COUNTER:
        t = next_txn_id_.load();
        t = t << 32;
        t |= (uint64_t) coo_id_;
      default:
        verify(0);
    }
    return t;
  }
  virtual void restart(TxData *ch) {verify(0);};
  virtual void Restart() = 0;
};

} // namespace janus
