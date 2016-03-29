
#include "../communicator.h"
#include "brq-common.h"

namespace rococo {
class BrqCommo : public Communicator {
public:

  void broadcast_fast_accept(groupid_t,
                             FastAcceptRequest& req,
                             std::              function<void(groupid_t,
                                                              FastAcceptReply *
                                                              reply)>) {
    // TODO
  }

  void broadcast_commit(groupid_t,
                        CommitRequest& req,
                        std::function<void(groupid_t, CommitReply *reply)>) {
    // TODO
  }
};
} // namespace
