#pragma once
#include "../__dep__.h"
#include "../command.h"
#include "deptran/procedure.h"

namespace janus {


class TxData;
class TpcPrepareCommand : public Marshallable {
 public:
  TpcPrepareCommand() : Marshallable(MarshallDeputy::CMD_TPC_PREPARE) {
  }
  txnid_t tx_id_ = 0;
  int32_t ret_ = -1;
  shared_ptr<Marshallable> cmd_{nullptr};

  Marshal& ToMarshal(Marshal&) const override;
  Marshal& FromMarshal(Marshal&) override;
};

class TpcCommitCommand : public Marshallable {
 public:
  TpcCommitCommand() : Marshallable(MarshallDeputy::CMD_TPC_COMMIT) {
  }
  txnid_t tx_id_ = 0;
  int ret_ = -1;
  virtual Marshal& ToMarshal(Marshal&) const override;
  virtual Marshal& FromMarshal(Marshal&) override;
};

} // namespace janus
