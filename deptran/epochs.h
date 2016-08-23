

#include "__dep__.h"

namespace rococo {

class EpochMgr {
 public:
  epoch_t curr_epoch_{1};
//  epoch_t oldest_alive_epoch_{0};

  struct EpochInfo {
    bool active{true};
    set<txnid_t> ids{};
    uint64_t n_done{0};

    void Add(txnid_t id) {
      verify(active);
      ids.insert(id);
    }
  };

  map<epoch_t, EpochInfo> epochs_;
  map<txnid_t, epoch_t>  id_to_epoch_;

  virtual void AddToEpoch(epoch_t e, txnid_t id) {
    id_to_epoch_[id] = e;
    epochs_[e].Add(id);
  }
  virtual void AddToCurrent(txnid_t id) {
    AddToEpoch(curr_epoch_, id);
  }
  virtual void Done(txnid_t id) {
    auto it = id_to_epoch_.find(id);
    verify(it != id_to_epoch_.end());
    auto e = id_to_epoch_[id];
    epochs_[e].n_done++;
  }
  virtual void Increment() {
    curr_epoch_++;
  }
  virtual epoch_t GetMostRecentInactiveEpoch() {
    epoch_t ret = 0;
    for (auto& pair: epochs_) {
      epoch_t e = pair.first;
      auto& info = pair.second;
      if (e == curr_epoch_) break;
      if (info.n_done == info.ids.size()) {
        info.active = false;
        ret = e;
      }
    }
    return ret;
  }
  virtual set<txnid_t> RemoveOld(epoch_t epoch) {
    set<txnid_t> ret;
    auto it = epochs_.begin();
    for (; it->first <= epoch; it = epochs_.begin()) {
      epoch_t e = it->first;
      auto& info = it->second;
      verify(info.n_done == info.ids.size());
      ret.insert(info.ids.begin(), info.ids.end());
      epochs_.erase(it);
    }
    return ret;
  }
//  virtual void IncrementAliveInEpoch(epoch_t epoch) {
//    verify(epoch >= oldest_alive_epoch_);
//  }
//  virtual void DecrementAliveInEpoch(epoch_t epoch) {
//    verify(epoch >= oldest_alive_epoch_);
//  }
//  virtual epoch_t CurrentEpoch() {
//    return curr_epoch_;
//  }
//  virtual void CheckIncrementEpoch() {
//    // check, last/current/future epoch is a/b/c
//    // if a is the oldest alive, and count of alive is 0,
//    // then advance to c.
//    epoch_t b = curr_epoch_;
//    epoch_t a = curr_epoch_ -1;
//    epoch_t c = curr_epoch_ + 1;
//    if (a == oldest_alive_epoch_) {
//      // TODO
////      if (epoch_[a].size() == 0) {
////        oldest_alive_epoch_= b;
////        curr_epoch_ = 0;
////      }
//    } else {
//      // wait.
//
//    }
//  }
//  virtual bool IsAlive(epoch_t epoch) {
//    return epoch >= oldest_alive_epoch_;
//  }
//  virtual int CountAlive(epoch_t epoch) {
//    return curr_epoch_ - oldest_alive_epoch_ + 1;
//  }
};

} // namespace rococo
