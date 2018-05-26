
#pragma once
#include "__dep__.h"

namespace janus {

/**
 * |---inactive---|---buffer---|---active---|
 * default value:
 * 0---------------1------------2-----------2
 * Can only add uncertain stuff to active epochs.
 * But can add "certain" stuff to buffer zone.
 * what does buffer zone means?
 *   0. any "involved" transaction are already decided.
 *   1. any new transaction cannot be in this epoch.
 *   2. if a new transaction appears in this zone,
 *      inquire shall return decided information,
 *      which prevents recursive inquiring back.
 * The rule to bring forward buffer zone???
 *   Future active transactions do not depend on any
 *   transaction that belong to this epoch.
 */
class EpochMgr {
 public:
  const int BUFFER_ZONE_LEN = 1;
  epoch_t curr_epoch_{2};
  epoch_t oldest_active_{2};
  epoch_t oldest_buffer_{1};
  epoch_t oldest_inactive_{0};

  struct EpochInfo {
    enum EpochState {INACTIVE, BUFFER, ACTIVE};
    bool active{true};
    bool buffer{false};
    bool inactive{false};
    set<txnid_t> ids{};
    uint64_t n_done{0};
    uint64_t n_ref{0};

    void Add(txnid_t id, bool is_done=false) {
      if (is_done) {
        n_done++;
      } else {
        verify(!inactive);
      }
      ids.insert(id);
    }

    void TransitionTo(EpochState state) {
      switch (state) {
        case INACTIVE:
          active = false;
          buffer = false;
          inactive = true;
          break;
        case BUFFER:
          active = false;
          buffer = true;
          inactive = false;
          break;
        case ACTIVE:
          active = true;
          buffer = false;
          inactive = false;
          break;
        default:
          verify(0);
      }
    }

    void Discard() {
      Log_info("discarding epoch:");
      active = false;
    }


  };

  map<epoch_t, EpochInfo> epochs_;
  map<txnid_t, epoch_t>  id_to_epoch_;

  virtual bool IsActive(epoch_t e) {
    return (oldest_active_ >= e);
  }
  virtual void AddToEpoch(epoch_t e, txnid_t id, bool is_decided=false) {
    return; // TODO re-enable this.
    verify(e > oldest_inactive_);
    id_to_epoch_[id] = e;
    epochs_[e].Add(id, is_decided);
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
  virtual epoch_t GrowActive() {
    return curr_epoch_++;
  }
  virtual epoch_t GrowBuffer() {
    // TODO there shall be enough room to grow.
    verify(oldest_active_ < curr_epoch_);
    EpochInfo& ei = epochs_[oldest_active_];
    if (ei.n_done == ei.ids.size()) {
      ei.TransitionTo(EpochInfo::BUFFER);
      oldest_active_ ++;
    } else {
      verify(oldest_active_ > 0);
    }
    return oldest_active_-1;
  }

  virtual epoch_t CheckBufferInactive() {
    return oldest_buffer_ - 1;
    verify(oldest_buffer_ < oldest_active_);
    EpochInfo& ei = epochs_[oldest_buffer_];
    if (ei.n_ref == 0) {
      return oldest_buffer_;
    } else {
      return oldest_buffer_ - 1;
    }
  }

  // return the most recent inactive epoch.
  virtual epoch_t IncrementInactive() {
    return oldest_buffer_ -1;
    verify(oldest_active_ < curr_epoch_);
    EpochInfo& ei = epochs_[oldest_active_];
    if (ei.n_done == ei.ids.size()) {
      ei.Discard();
      oldest_active_ ++;
    } else {
      verify(oldest_active_ > 0);
    }
    return oldest_active_-1;
  }

  virtual epoch_t GetMostRecentInactiveEpoch() {
    verify(0);
    return 0;
//    epoch_t ret = 0;
//    for (auto& pair: epochs_) {
//      epoch_t e = pair.first;
//      auto& info = pair.second;
//      if (e == curr_epoch_) break;
//      // trying to make epoch inactive?
//      // perhaps should not do it here.
//      if (info.n_done == info.ids.size()) {
//        info.Discard();
//        ret = e;
//      }
//    }
//    return ret;
  }
  void IncrementRef(epoch_t e) {
    epochs_[e].n_ref++;
  }

  void DecrementRef(epoch_t e) {
    epochs_[e].n_ref--;
  }
  virtual set<txnid_t> GrowInactive(epoch_t epoch) {
    verify(oldest_buffer_ >= epoch);
    verify(oldest_buffer_ <= epoch + 1);
    oldest_buffer_ = epoch + 1;
    set<txnid_t> ret;
    if (oldest_active_ < oldest_buffer_ - 1) {
      EpochInfo& ei = epochs_[oldest_active_];
      ret.insert(ei.ids.begin(), ei.ids.end());
      epochs_.erase(oldest_active_);
      oldest_active_++;
    }
    return ret;
  }
  virtual set<txnid_t> RemoveOld(epoch_t epoch) {
    verify(0);
    verify(epoch < curr_epoch_);
    verify(epoch >= oldest_active_);
    oldest_active_ = epoch + 1;
    set<txnid_t> ret;
    auto it = epochs_.begin();
    for (; it->first <= epoch; it = epochs_.begin()) {
      epoch_t e = it->first;
      auto& info = it->second;
      // is it possible that n_done does not equal ids size?
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

} // namespace janus
