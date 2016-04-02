#include "tpc_command.h"

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
  // TODO
  return m;
}
Marshal& TpcPrepareCommand::FromMarshal(Marshal& m) {
  // TODO
  return m;
}