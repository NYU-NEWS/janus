#pragma once
#include "../__dep__.h"
#include "graph.h"
#include "txn-info.h"
#include "dep_graph.h"

namespace rococo {
template<typename T>
inline rrr::Marshal &operator<<(rrr::Marshal &m, const Vertex<T> *&v) {
  int64_t u = std::uintptr_t(v);
  m << u;
  return m;
}

template<typename T>
inline rrr::Marshal &operator<<(rrr::Marshal &m, const Graph<T> &gra) {
  uint64_t n = gra.size();
  verify(n > 0);
  m << n;
  int i = 0;
  for (auto &pair : gra.vertex_index_) {
    auto &v = pair.second;
    i++;
    int32_t n_out_edge = v->outgoing_.size();
    m << v->data_->id();
    m << *(v->data_);
    m << n_out_edge;
    for (auto &it : v->outgoing_) {
      Vertex<T> *vv = it.first;
      verify(vv != nullptr);
      uint64_t id = vv->data_->id();
      int8_t weight = it.second;
      m << id << weight;
    }
  }
  verify(i == n);
  return m;
}

template<class T>
inline rrr::Marshal &operator>>(rrr::Marshal &m, Graph<T> &gra) {
  verify(gra.size() == 0);
  uint64_t n;
  m >> n;
  verify(n > 0);
  map<uint64_t, Vertex<T> *> ref;
  map<uint64_t, map<int64_t, int8_t> > v_to;

  // Log::debug("marshalling gra, graph size: %d", (int) n);

  int nn = n;

  while (nn-- > 0) {
    uint64_t v_id;
    m >> v_id;
    ref[v_id] = new Vertex<T>(v_id);
    m >> *(ref[v_id]->data_);
    int32_t n_out_edge;
    m >> n_out_edge;

    while (n_out_edge-- > 0) {
      uint64_t child_id;
      int8_t weight;
      m >> child_id;
      m >> weight;
      v_to[v_id][child_id] = weight;
    }
  }
  // insert vertexes into graph.
  verify(ref.size() == n);
  for (auto &kv : ref) {
    gra.vertex_index_[kv.first] = kv.second;
  }
  // build edge pointers.
  for (auto &kv : v_to) {
    uint64_t v_id = kv.first;
    map<int64_t, int8_t>& o_to = kv.second;
    Vertex<T> *v = ref[v_id];

    for (auto &tokv : o_to) {
      uint64_t child_vid = tokv.first;
      int8_t weight = tokv.second;
      Vertex<T> *child_v = ref[child_vid];
      v->outgoing_[child_v] = weight;
      child_v->incoming_[v] = weight;
    }
  }

  verify(gra.size() > 0);
  return m;
}

//
//inline rrr::Marshal &operator>>(rrr::Marshal &m, RccGraph &graph) {
//  verify(graph.size() == 0);
//  m >> graph.txn_gra_;
//  return m;
//}
//
//inline rrr::Marshal &operator<<(rrr::Marshal &m, const RccGraph &graph) {
////  graph.write_to_marshal(m);
//  m << graph.txn_gra_;
//  return m;
//}

} // namespace rococo