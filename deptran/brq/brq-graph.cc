
#include "brq-graph.h"
#include "brq.h"

namespace rococo {
void BRQGraph::Insert(BRQDTxn *dtxn) {
  // add into index
  auto ret = vertex_index_.insert(std::pair<txnid_t, BRQDTxn *>(dtxn->txn_id_, dtxn));

  if (ret.second == false) {
    verify(0);
  }
}

BRQDTxn* BRQGraph::Find(txnid_t txn_id) {
  auto it = vertex_index_.find(txn_id);

  if (it != vertex_index_.end()) {
    return it->second;
  } else {
    return nullptr;
  }
}

BRQDTxn* BRQGraph::Create(txnid_t txn_id) {
  BRQDTxn *dtxn = new BRQDTxn(txn_id, this);
  vertex_index_[txn_id] = dtxn;
  return dtxn;
}

BRQDTxn* BRQGraph::FindOrCreate(txnid_t txn_id) {
  BRQDTxn* ret = Find(txn_id);
  if (ret == nullptr) {
    ret = Create(txn_id);
  }
  return ret;
}

void BRQGraph::BuildEdgePointer(std::map<txnid_t, BRQDTxn*> &dtxn_map) {
  for (auto &pair: dtxn_map) {
    auto dtxn = pair.second;
    for (auto parent_id : dtxn->deps_) {
      auto parent = dtxn_map[parent_id];
      dtxn->from_.insert(parent);
      parent->to_.insert(dtxn);
    }
  }
}

BRQDTxn* BRQGraph::AggregateVertex(BRQDTxn *a_dtxn) {
  // create the dtxn if not exist.
  auto dtxn = this->FindOrCreate(a_dtxn->txn_id_);
  // add edges.
  for (auto dep : a_dtxn->deps_) {
    dtxn->deps_.insert(dep);
  }
  // update status
  if (a_dtxn->status_ > dtxn->status_) {
      dtxn->status_ = a_dtxn->status_;
  }
  return dtxn;
}

void BRQGraph::Aggregate(BRQGraph &graph) {
  std::map<txnid_t, BRQDTxn*> txns;
  for (auto& pair: graph.vertex_index_) {
    auto dtxn = this->AggregateVertex(pair.second);
    txns[dtxn->txn_id_] = dtxn;
  }
  this->BuildEdgePointer(txns);
  this->CheckStatusChange(txns);
}

void BRQGraph::CheckStatusChange(std::map<txnid_t, BRQDTxn*>& dtxn_map) {
  for (auto &pair: dtxn_map) {
    auto dtxn = pair.second;
    TestExecute(dtxn);
  }
}

void BRQGraph::TestExecute(BRQDTxn* dtxn) {
  if (dtxn->is_finished_ || dtxn->status_ < BRQDTxn::CMT) {
    return;
  }
  if (dtxn->status_ == BRQDTxn::CMT && !CheckPredCMT(dtxn)) {
    return;
  }
  auto scc = FindSCC(dtxn);
  if (!CheckPredFIN(scc))
    return;
  for (auto dtxn: scc) {
      dtxn->commit_exec();
  }
  for (auto dtxn: scc) {
    // test every decendent
    for (auto d: dtxn->to_) {
      TestExecute(d);
    }
  }
}

bool BRQGraph::CheckPredCMT(BRQDTxn *dtxn) {
  // TODO
}

bool BRQGraph::CheckPredFIN(std::set<BRQDTxn*>& scc) {
  // TODO
}

std::set<BRQDTxn*> BRQGraph::FindSCC(BRQDTxn*) {
  // TODO
  std::set<BRQDTxn*> ret;
  return ret;
}

} // namespace rococo
