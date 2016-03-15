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
  int32_t n = gra.size();

  verify(n > 0);
  m << n;
  int i = 0;

  for (auto &pair : gra.vertex_index_) {
    auto &v = pair.second;
    i++;
    m << v->data_->id();
    m << *(v->data_);
    int32_t size = v->outgoing_.size();
    m << size;

    for (auto &it : v->outgoing_) {
      Vertex<T> *vv = it.first;
      verify(vv != nullptr);
      uint64_t id = vv->data_->id();
      m << id;
      int8_t relation = it.second;
      m << relation;
    }
  }
  verify(i == n);
  return m;
}

template<class T>
rrr::Marshal &operator>>(rrr::Marshal &m, Graph<T> &gra) {
  int32_t n;

  m >> n;
  verify(n > 0);
  std::map<uint64_t, Vertex<T> *> ref;
  std::map<uint64_t, std::map<int64_t, int8_t> > v_to;

  // Log::debug("marshalling gra, graph size: %d", (int) n);

  int nn = n;

  while (nn-- > 0) {
    int64_t o;
    m >> o;
    ref[o] = new Vertex<T>(o);
    m >> *(ref[o]->data_);
    int32_t k;
    m >> k;

    while (k-- > 0) {
      int64_t ii;
      int8_t jj;
      m >> ii;
      m >> jj;
      v_to[o][ii] |= jj;
    }
  }

  verify(ref.size() == n);

  for (auto &kv : ref) {
    gra.vertex_index_[kv.first] = kv.second;
  }

  for (auto &kv : v_to) {
    int64_t o = kv.first;
    std::map<int64_t, int8_t> o_to = kv.second;
    Vertex<T> *v = ref[o];

    for (auto &tokv : o_to) {
      int64_t to_o = tokv.first;
      int8_t type = tokv.second;
      Vertex<T> *to_v = ref[to_o];
      v->outgoing_[to_v] = type;
      to_v->incoming_[v] = type;
    }
  }

  verify(gra.size() > 0);
  return m;
}
//
//struct GraphMarshaler {
//  Graph<TxnInfo> *gra = nullptr;
//
//  // std::set<Vertex<TxnInfo>*> ret_set;
//  std::unordered_set<Vertex<TxnInfo> *> ret_set;
//
//  bool self_create = false;
//
//  ~GraphMarshaler() {
//    if (self_create) {
//      verify(gra);
//      delete gra;
//    }
//  }
//
//  void write_to_marshal(rrr::Marshal &m) const;
//
//  void marshal_help_1(rrr::Marshal &m,
//                      const std::unordered_set<Vertex<TxnInfo> *> &ret_set,
//                      Vertex<TxnInfo> *old_sv) const;
//
//  void marshal_help_2(rrr::Marshal &m,
//                      const std::unordered_set<Vertex<TxnInfo> *> &ret_set,
//                      Vertex<TxnInfo> *old_sv) const;
//};

inline rrr::Marshal &operator>>(rrr::Marshal &m, RccGraph &graph) {
  verify(graph.txn_gra_.size() == 0);
  m >> graph.txn_gra_;
  return m;
}

inline rrr::Marshal &operator<<(rrr::Marshal &m, const RccGraph &graph) {
  graph.write_to_marshal(m);
  return m;
}

} // namespace rococo