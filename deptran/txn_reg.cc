#include "txn_reg.h"
#include "rcc/graph.h"
#include "rcc/graph_marshaler.h"
#include "marshal-value.h"
#include "command.h"
#include "command_marshaler.h"
#include "rcc_rpc.h"

namespace rococo {

//map<std::pair<base::i32, base::i32>,
//    txn_handler_defer_pair_t> TxnRegistry::all_;
//map<std::pair<base::i32, base::i32>, TxnRegistry::LockSetOracle> TxnRegistry::lck_oracle_;
txn_handler_defer_pair_t
TxnRegistry::get(const SimpleCommand &cmd) {
  return get(cmd.root_type_, cmd.type_);
}

} // namespace rococo
