#pragma once
#include "../__dep__.h"
#include "../command.h"
#include "../txn_chopper.h"

namespace rococo {


class TxnCommand;
class TpcPrepareCommand : public ContainerCommand {
 public:
  TpcPrepareCommand() {
    self_cmd_ = this;
    type_ = CMD_TPC_PREPARE;
  }
  txnid_t txn_id_ = 0;
  int32_t res_ = -1;
  vector<SimpleCommand> cmds_ = {};

  Marshal& ToMarshal(Marshal&) const override;
  Marshal& FromMarshal(Marshal&) override;
  virtual ContainerCommand& Execute() {verify(0);};
};

class TpcCommitCommand : public ContainerCommand {
 public:
  TpcCommitCommand() {
    self_cmd_ = this;
    type_ = CMD_TPC_COMMIT;
  }
  txnid_t txn_id_ = 0;
  int res_ = -1;
  virtual Marshal& ToMarshal(Marshal&) const;
  virtual Marshal& FromMarshal(Marshal&);
  virtual ContainerCommand& Execute() {verify(0);};
};

} // namespace rococo