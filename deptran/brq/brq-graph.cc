
#include "brq-graph.h"
#include "brq.h"

namespace rococo {

void BRQGraph::insert(BRQDTxn *dtxn) {
  // add into index
  auto ret = index_.insert(std::pair<txnid_t, BRQDTxn*>(dtxn->txn_id_, dtxn));
  if (ret.second == false) {
    verify(0);
  }
}

BRQDTxn* BRQGraph::search(txnid_t txn_id) {
  auto it = index_.find(txn_id);
  if (it != index_.end()) {
    return it->second;
  } else {
    return nullptr;
  }
}

void BRQGraph::aggregate(BRQSubGraph* subgraph) {
  ;
}
} // namespace rococo
