#pragma once

#include "../__dep__.h"
#include "reactor/event.h"

using rrr::Event;

namespace janus {

class QuorumEvent : public Event {
 public:
  int32_t n_total_ = -1;
  int32_t quorum_ = -1;
  int32_t n_voted_{0};
  bool timeouted_ = false;
  // fast vote result.
  vector<uint64_t> vec_timestamp_{};

  QuorumEvent() = delete;

  QuorumEvent(int n_total,
              int quorum) : Event(),
                            quorum_(quorum) {
  }

  bool IsReady() override {
    if (timeouted_) {
      // TODO add time out support
      return true;
    }
    if (n_voted_ >= quorum_) {
      Log_debug("voted: %d is equal or greater than quorum: %d",
                (int)n_voted_, (int) quorum_);
      return true;
    }
    Log_debug("voted: %d is smaller than quorum: %d",
              (int)n_voted_, (int) quorum_);
    return false;
  }

};

} // namespace janus
