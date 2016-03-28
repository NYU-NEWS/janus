#pragma once

#include "__dep__.h"
#include "config.h"

namespace rococo {

struct ChopFinishResponse;

class RccGraph;
// TODO Should this class be merged with RCCDTxn?
class TxnInfo {
 private:
  int8_t status_ = TXN_UKN;

 public:
  txnid_t txn_id_;
  std::set<uint32_t> partition_;
  std::vector<uint64_t> pieces_;
  bool executed_ = false;

  bool committed_ = false;

  bool during_commit = false;
  bool during_asking = false;
  bool inquire_acked_ = false;

  vector<RccGraph*> graphs_for_inquire_ = {};
  vector<function<void()>> callbacks_for_inquire_ = {};

  ChopFinishResponse *res = nullptr;

  TxnInfo(uint64_t id) {
    txn_id_ = id;
  }

  ~TxnInfo() {
    Log_debug("txn info destruct, id: %llx", txn_id_);
    txn_id_ = 0; // for debug purpose
  }

  inline int8_t get_status() const {
    return status_;
  }

  inline int8_t status() const {
    return status_;
  }

  inline bool IsCommitting() const {
    return (status_ & TXN_CMT);
  }

  inline bool IsDecided() const {
    return (status_ & TXN_DCD);
  }

  inline bool IsExecuted() const {
    if (executed_) verify(IsDecided());
    return executed_;
  }

  bool Involve(svrid_t id) {
    auto it = partition_.find(id);
    if (it == partition_.end()) {
      return false;
    } else {
      return true;
    }
  }

  uint32_t random_server() {
    verify(partition_.size() > 0);
    uint32_t i = RandomGenerator::rand(0, partition_.size() - 1);
    auto it = partition_.begin();
    std::advance(it, i);
    uint32_t id = *(partition_.begin());
    Log::debug("random a related server, id: %x", id);
    return id;
  }

  bool operator<(const TxnInfo &rhs) const {
    return txn_id_ < rhs.txn_id_;
  }

  inline uint64_t id() {
    return txn_id_;
  }

  inline void set_id(uint64_t id) {
    txn_id_ = id;
  }

  inline void union_data(const TxnInfo &ti,
                         bool trigger = false,
                         bool server = false) {
    partition_.insert(ti.partition_.begin(), ti.partition_.end());
    union_status(ti.status_, trigger, server);
  }

  void trigger() {
    verify(0);
    for (auto &kv: events_) {
      if (kv.first <= status_) {
        while (kv.second.size() > 0) {
          DragonBall *ball = kv.second.back();
          kv.second.pop_back();
          ball->trigger();
        }
      }
    }
  }

  void union_status(int8_t status,
                    bool is_trigger = false,
                    bool is_server = false) {

    if (false) {
      // TODO
      // I cannot change the status of this txn.
    } else {
      status_ |= status;
    }
    if (is_trigger) {
      trigger();
    }
  }

  void register_event(int8_t status, DragonBall *ball) {
    if (status_ >= status) {
      ball->trigger();
    }
    else {
      events_[status].push_back(ball);
    }
  }

  // a simple simple state machine, don't have to marshal
  int32_t wait_finish_ = 0;
  int32_t wait_commit_ = 0;

  std::map<int8_t, std::vector<DragonBall *> > events_;

};

inline rrr::Marshal &operator<<(rrr::Marshal &m, const TxnInfo &ti) {
  m << ti.txn_id_ << ti.status() << ti.partition_;
  return m;
}

inline rrr::Marshal &operator>>(rrr::Marshal &m, TxnInfo &ti) {
  int8_t status;
  m >> ti.txn_id_ >> status >> ti.partition_;
  ti.union_status(status);
  return m;
}

} //namespace rcc 
