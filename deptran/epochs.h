

#include "__dep__.h"

namespace rococo {

class EpochMgr {
 public:
  epoch_t curr_epoch_{0};
  epoch_t oldest_alive_epoch_{0};

  map<epoch_t, int32_t> n_alive_{};
  map<epoch_t, set<id_t>> epoch_to_ids_;
  map<id_t, epoch_t>  id_to_epoch_;

  virtual void AddToCurrent(id_t id) {
    id_to_epoch_[id] = curr_epoch_;
    epoch_to_ids_[curr_epoch_].insert(id);
  }
  virtual void IncrementAliveInEpoch(epoch_t epoch) {
    verify(epoch >= oldest_alive_epoch_);
    n_alive_[epoch]++;
  }
  virtual void DecrementAliveInEpoch(epoch_t epoch) {
    verify(epoch >= oldest_alive_epoch_);
    n_alive_[epoch]--;
  }
  virtual epoch_t CurrentEpoch() {
    return curr_epoch_;
  }
  virtual void CheckIncrementEpoch() {
    // check, last/current/future epoch is a/b/c
    // if a is the oldest alive, and count of alive is 0,
    // then advance to c.
    epoch_t b = curr_epoch_;
    epoch_t a = curr_epoch_ -1;
    epoch_t c = curr_epoch_ + 1;
    if (a == oldest_alive_epoch_) {
      // TODO
//      if (epoch_[a].size() == 0) {
//        oldest_alive_epoch_= b;
//        curr_epoch_ = 0;
//      }
    } else {
      // wait.

    }
  }
  virtual bool IsAlive(epoch_t epoch) {
    return epoch >= oldest_alive_epoch_;
  }
  virtual int CountAlive(epoch_t epoch) {
    return curr_epoch_ - oldest_alive_epoch_ + 1;
  }
};

} // namespace rococo
