
#include "brq-graph.h"
#include "brq.h"

namespace rococo {


void BRQGraph::Insert(BRQDTxn *dtxn) {
  // TODO
  // add into index
//  auto ret = vertex_index_.insert(std::pair<txnid_t, BRQDTxn *>(dtxn->txn_id_, dtxn));
//
//  if (ret.second == false) {
//    verify(0);
//  }
}

BRQDTxn* BRQGraph::Find(txnid_t txn_id) {
  auto it = vertex_index_.find(txn_id);
  if (it != vertex_index_.end()) {
    auto sptr = it->second->dtxn_;
    return sptr.get();
  } else {
    return nullptr;
  }
}

BRQDTxn* BRQGraph::Create(txnid_t txn_id) {
  BRQDTxn *dtxn = new BRQDTxn(txn_id, this);
  auto &sptr = vertex_index_[txn_id]->dtxn_;
  verify(sptr == nullptr);
  sptr = std::shared_ptr<BRQDTxn>(dtxn);
  return dtxn;
}

BRQDTxn* BRQGraph::FindOrCreate(txnid_t txn_id) {
  BRQDTxn* ret = Find(txn_id);
  if (ret == nullptr) {
    ret = Create(txn_id);
  }
  return ret;
}

void BRQGraph::BuildEdgePointer(BRQGraph &graph, std::map<txnid_t, BRQVertex*>& index) {
  for (auto &pair: graph.vertex_index_) {
    auto copy_vertex = pair.second;
    auto vertex = index[copy_vertex->dtxn_->txn_id_];
    for (auto pair : copy_vertex->from_) {
      auto parent = index[pair.first->dtxn_->txn_id_];
      vertex->from_.insert(std::make_pair(parent, 0));
      parent->to_.insert(std::make_pair(vertex,0));
    }
  }
}

BRQVertex* BRQGraph::AggregateVertex(BRQVertex *av) {
  // create the dtxn if not exist.
  auto vertex = vertex_index_[av->dtxn_->txn_id_];
  if (vertex->dtxn_ == nullptr) {
    vertex->dtxn_ = av->dtxn_;
  } else {
    // add edges. // TODO is this right?
    for (auto dep : av->dtxn_->deps_) {
      vertex->dtxn_->deps_.insert(dep);
    }
    // update status
    if (av->dtxn_->status_ > vertex->dtxn_->status_) {
      vertex->dtxn_->status_ = av->dtxn_->status_;
    }
  }
  return vertex;
}

void BRQGraph::Aggregate(BRQGraph &graph) {
  // aggregate vertexes
  std::map<txnid_t, BRQVertex*> index;
  for (auto& pair: graph.vertex_index_) {
    auto vertex = this->AggregateVertex(pair.second);
    index[vertex->dtxn_->txn_id_] = vertex;
  }
  // aggregate edges.
  this->BuildEdgePointer(graph, index);
  this->CheckStatusChange(index);
}

void BRQGraph::CheckStatusChange(std::map<txnid_t, BRQVertex*>& dtxn_map) {
  for (auto &pair: dtxn_map) {
    auto vertex = pair.second;
    TestExecute(vertex);
  }
}

void BRQGraph::TestExecute(BRQVertex* vertex) {
  auto dtxn = vertex->dtxn_;
  if (dtxn->is_finished_ || dtxn->status_ < BRQDTxn::CMT) {
    return;
  }
  if (dtxn->status_ == BRQDTxn::CMT && !CheckPredCMT(vertex)) {
    return;
  }
  auto scc = FindSCC(vertex);
  if (!CheckPredFIN(scc)) {
    return;
  }
  for (auto v: scc) {
    v->dtxn_->commit_exec();
  }
  for (auto v: scc) {
    // test every decendent
    for (auto pair: v->to_) {
      TestExecute(pair.first);
    }
  }
}

bool BRQGraph::TraversePred(BRQVertex* vertex, int64_t depth, std::function<bool(BRQVertex*)> &func, std::set<BRQVertex*> &walked) {
  auto pair = walked.insert(vertex);
  if (!pair.second) {
    return true;
  }
  for (auto pair : vertex->from_) {
    auto v = pair.first;
    if (!func(v))
      return false;
    if (depth < 0 || depth > 0) {
      if (!TraversePred(v, depth-1, func, walked))
        return false;
    }
  }
  return true;
}

bool BRQGraph::CheckPredCMT(BRQVertex* vertex) {
  std::set<BRQVertex*> walked;
  std::function<bool(BRQVertex*)> func = [] (BRQVertex* v) {
    if (v->dtxn_->status_ >= BRQDTxn::CMT) {
      return true;
    } else {
      // TODO trigger inquiry
      return false;
    }
  };
  return TraversePred(vertex, -1, func, walked);
}

bool BRQGraph::CheckPredFIN(std::set<BRQVertex*>& scc) {
  std::set<BRQVertex*> walked = scc;
  std::function<bool(BRQVertex*)> func = [] (BRQVertex* v) {
    if (v->dtxn_->is_finished_) {
      return true;
    } else {
      // TODO trigger inquiry
      return false;
    }
  };
  for (auto v : scc) {
    auto ret = TraversePred(v, 1, func, walked);
    if (!ret) return false;
  }
  return true;
}

std::set<BRQVertex*> BRQGraph::FindSCC(BRQVertex*) {
  // TODO
  std::set<BRQVertex*> ret;
  return ret;
}

} // namespace rococo
