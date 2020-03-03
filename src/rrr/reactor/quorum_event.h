#pragma once

#include <vector>
#include "event.h"

using rrr::Event;
using std::vector;

namespace janus {

class QuorumEvent : public Event {
 private:
  int32_t n_voted_yes_{0};
  int32_t n_voted_no_{0};
 public:
  int32_t n_total_ = -1;
  int32_t quorum_ = -1;
  bool timeouted_ = false;
  // fast vote result.
  vector<uint64_t> vec_timestamp_{};

  QuorumEvent() = delete;

  QuorumEvent(int n_total,
              int quorum) : Event(),
                            n_total_(n_total),
                            quorum_(quorum) {
  }

  bool Yes() {
    return n_voted_yes_ >= quorum_;
  }

  bool No() {
    verify(n_total_ >= quorum_);
    return n_voted_no_ > (n_total_ - quorum_);
  }

  void VoteYes() {
    n_voted_yes_++;
    Test();

  }

  void VoteNo() {
    n_voted_no_++;
    Test();
  }

  bool IsReady() override {
    if (timeouted_) {
      // TODO add time out support
      return true;
    }
    if (Yes()) {
//      Log_debug("voted: %d is equal or greater than quorum: %d",
//                (int)n_voted_yes_, (int) quorum_);
      return true;
    } else if (No()) {
      return true;
    }
//    Log_debug("voted: %d is smaller than quorum: %d",
//              (int)n_voted_, (int) quorum_);
    return false;
  }

};

} // namespace janus
