#pragma once

#include "../communicator.h"

namespace janus {

class QuorumEvent;
class CommoFebruus : public Communicator {
 public:
  void BroadcastPreAccept(QuorumEvent& e, parid_t, txid_t);
  void BroadcastAccept(QuorumEvent& e,
                       parid_t partition_id,
                       txid_t tx_id,
                       ballot_t ballot,
                       uint64_t timestamp);
  void BroadcastCommit(const set<parid_t>&,
                       txid_t tx_id,
                       uint64_t timestamp);
};

} // namespace janus
