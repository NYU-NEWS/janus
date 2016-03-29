
#include "../__dep__.h"
#include "../constants.h"
#include "dep_graph.h"
#include "dtxn.h"

namespace rococo {


//void BrqGraph::BuildEdgePointer(BrqGraph &graph,
//                                map<txnid_t, BRQVertex*>& index) {
//  for (auto &pair: graph.vertex_index_) {
//    auto copy_vertex = pair.second;
//    auto vertex = index[copy_vertex->id()];
//    for (auto pair : copy_vertex->incoming_) {
//      auto parent = index[pair.first->data_->txn_id_];
//      vertex->incoming_.insert(std::make_pair(parent, 0));
//      parent->outgoing_.insert(std::make_pair(vertex,0));
//    }
//  }
//}
//
//BRQVertex* BrqGraph::AggregateVertex(BRQVertex *av) {
//  // create the dtxn if not exist.
//  auto vertex = vertex_index_[av->id()]; // FIX
//  if (vertex->data_ == nullptr) {
//    vertex->data_ = av->data_;
//  } else {
//    // add edges. // TODO is this right?
//    for (auto dep : av->data_->deps_) {
//      vertex->data_->deps_.insert(dep);
//    }
//    // update status
//    if (av->data_->status_ > vertex->data_->status_) {
//      vertex->data_->status_ = av->data_->status_;
//    }
//  }
//  return vertex;
//}
//
//void BrqGraph::Aggregate(BrqGraph &graph) {
//  // aggregate vertexes
//  map<txnid_t, BRQVertex*> index;
//  for (auto& pair: graph.vertex_index_) {
//    auto vertex = this->AggregateVertex(pair.second);
//    index[vertex->id()] = vertex;
//  }
//  // aggregate edges.
//  this->BuildEdgePointer(graph, index);
//  this->CheckStatusChange(index);
//}
//
//void BrqGraph::CheckStatusChange(std::map<txnid_t, BRQVertex*>& dtxn_map) {
//  for (auto &pair: dtxn_map) {
//    auto vertex = pair.second;
//    TestExecute(vertex);
//  }
//}
//
//void BrqGraph::TestExecute(BRQVertex* vertex) {
//  auto dtxn = vertex->data_;
//  if (dtxn->is_finished_ || dtxn->status_ < BrqDTxn::CMT) {
//    return;
//  }
//  if (dtxn->status_ == BrqDTxn::CMT && !CheckPredCMT(vertex)) {
//    return;
//  }
//  auto scc = FindSCC(vertex);
//  if (!CheckPredFIN(scc)) {
//    return;
//  }
//  for (auto v: scc) {
//    v->data_->commit_exec();
//  }
//  for (auto v: scc) {
//    // test every decendent
//    for (auto pair: v->outgoing_) {
//      TestExecute(pair.first);
//    }
//  }
//}
//
//bool BrqGraph::CheckPredCMT(BRQVertex* vertex) {
//  std::set<BRQVertex*> walked;
//  std::function<bool(BRQVertex*)> func = [] (BRQVertex* v) {
//    if (v->data_->status_ >= BrqDTxn::CMT) {
//      return true;
//    } else {
//      // TODO trigger inquiry
//      return false;
//    }
//  };
//  return TraversePred(vertex, -1, func, walked);
//}
//
//bool BrqGraph::CheckPredFIN(std::vector<BRQVertex*>& scc) {
//  std::set<BRQVertex*> walked(scc.begin(), scc.end());
//  std::function<bool(BRQVertex*)> func = [] (BRQVertex* v) {
//    if (v->data_->is_finished_) {
//      return true;
//    } else {
//      // TODO trigger inquiry
//      return false;
//    }
//  };
//  for (auto v : scc) {
//    auto ret = TraversePred(v, 1, func, walked);
//    if (!ret) return false;
//  }
//  return true;
//}

} // namespace rococo
