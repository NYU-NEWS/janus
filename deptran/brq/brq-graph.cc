
#include "brq-graph.h"
#include "brq.h"

namespace rococo {
void BRQGraph::Insert(BRQDTxn *dtxn) {
  // add into index
  auto ret = index_.insert(std::pair<txnid_t, BRQDTxn *>(dtxn->txn_id_, dtxn));

  if (ret.second == false) {
    verify(0);
  }
}

BRQDTxn * BRQGraph::Search(txnid_t txn_id) {
  auto it = index_.find(txn_id);

  if (it != index_.end()) {
    return it->second;
  } else {
    return nullptr;
  }
}

void BRQGraph::BuildEdgePointer(std::map<txnid_t, BRQDtxn*> &dtxn_map) {
  if (dtxn_map == nullptr) {
    dtxn_map = vertex_index_;
  }
  for (auto &pair: dtxn_map) {
    auto dtxn = pair.second;
    for (auto parent_id : dtxn->deps_) {
      auto parent = dtxn_map[parent_id];
      dtxn->from_.insert(parent);
      parent_->to_.insert(dtxn);
    }
  }
}

DTxn* BRQGraph::AggregateVertex(DTxn *a_dtxn) {
  // create the dtxn if not exist.
  auto dtxn = this->FindOrCreate(v->txn_id_);
  // add edges.
  for (auto dep : a_dtxn->deps_) {
    dtxn->deps_->insert(dep);
  }
  // update status
  if (a_dtxn->status_ > dtxn->status_) {
      dtxn->status_ = a_dtxn->status_;
//      status_changed_.insert(dtxn);
  }
  return dtxn;
}

void BRQGraph::Aggregate(BRQSubGraph &graph) {
  std::map<txnid_t, BRQDtxn*> txns;
  for (auto& pair: graph.vertex_index_) {
    auto dtxn = this->AggregateVertex(pair.second);
    txns[dtxn->txn_id_] = dtxn;
  }
  this->BuildEdgePointer(txns);
}

void HasNextExecutableSCC() {

}

void BRQGraph::CheckStatusChange(std::map<txnid_t BRQDTxn*t>& dtxn_map) {
  for (auto &pair: dtxn_map) {
    auto dtxn = pair.second;
  }
  // TODO
  while (HasNextExecutableSCC) {
    auto scc = NextExecutableSCC();
    // TODO sort scc
    for (auto dtxn : scc) {
      dtxn->commit_after();
    }
  }
}

// wait graph.

void WaitDCD(BRQDTxn *dtxn) {
  // TODO
}

void WaitCMT(BRQDTxn *dtxn) {
  // TODO
}

} // namespace rococo
