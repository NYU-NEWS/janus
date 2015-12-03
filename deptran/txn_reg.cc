#include "txn_reg.h"
#include "rcc/graph.h"
#include "rcc/graph_marshaler.h"
#include "marshal-value.h"
#include "rcc_rpc.h"

namespace rococo {

//map<std::pair<base::i32, base::i32>,
//    txn_handler_defer_pair_t> TxnRegistry::all_;
//map<std::pair<base::i32, base::i32>, TxnRegistry::LockSetOracle> TxnRegistry::lck_oracle_;
txn_handler_defer_pair_t TxnRegistry::get(const RequestHeader &req_hdr) {
  return get(req_hdr.t_type, req_hdr.p_type);
}

} // namespace rococo
