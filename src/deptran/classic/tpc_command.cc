#include "tpc_command.h"
#include "../command.h"
#include "../command_marshaler.h"

using namespace janus;

static int volatile x1 =
    MarshallDeputy::RegInitializer(MarshallDeputy::CMD_TPC_PREPARE,
                                     [] () -> Marshallable* {
                                       return new TpcPrepareCommand;
                                     });

static int volatile x2 =
    MarshallDeputy::RegInitializer(MarshallDeputy::CMD_TPC_COMMIT,
                                     [] () -> Marshallable* {
                                       return new TpcCommitCommand;
                                     });


Marshal& TpcPrepareCommand::ToMarshal(Marshal& m) const {
  m << tx_id_;
  m << ret_;
//  m << (int32_t) cmd_.size();
//  for (auto o : cmd_) {
//    m << *o;
//  }
  MarshallDeputy md(cmd_);
  m << md;
  return m;
}

Marshal& TpcPrepareCommand::FromMarshal(Marshal& m) {
  m >> tx_id_;
  m >> ret_;
//  int32_t sz;
//  m >> sz;
//  verify(cmd_.empty());
//  for (int i = 0; i < sz; i++) {
//    auto o = make_shared<SimpleCommand>();
//    m >> *o;
//    cmd_.push_back(o);
//  }
  MarshallDeputy md;
  m >> md;
  if (!cmd_)
    cmd_ = md.sp_data_;
  else
    verify(0);
  return m;
}

Marshal& TpcCommitCommand::ToMarshal(Marshal& m) const {
  m << tx_id_;
  m << ret_;
  return m;
}
Marshal& TpcCommitCommand::FromMarshal(Marshal& m) {
  m >> tx_id_;
  m >> ret_;
  return m;
}
