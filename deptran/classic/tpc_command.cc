#include "tpc_command.h"
#include "../command.h"
#include "../command_marshaler.h"

using namespace rococo;

static int volatile x1 =
    ContainerCommand::RegInitializer(CMD_TPC_PREPARE,
                                     [] () -> ContainerCommand* {
                                       return new TpcPrepareCommand;
                                     });

static int volatile x2 =
    ContainerCommand::RegInitializer(CMD_TPC_COMMIT,
                                     [] () -> ContainerCommand* {
                                       return new TpcCommitCommand;
                                     });


Marshal& TpcPrepareCommand::ToMarshal(Marshal& m) const {
  m << txn_id_;
  m << res_;
  m << cmds_;
  return m;
}
Marshal& TpcPrepareCommand::FromMarshal(Marshal& m) {
  m >> txn_id_;
  m >> res_;
  m >> cmds_;
  return m;
}


Marshal& TpcCommitCommand::ToMarshal(Marshal& m) const {
  m << txn_id_;
  m << res_;
  return m;
}
Marshal& TpcCommitCommand::FromMarshal(Marshal& m) {
  m >> txn_id_;
  m >> res_;
  return m;
}
