#pragma once

#include <algorithm>
#include <map>
#include "all.h"

namespace rococo {

template<typename T>
class Vertex {
 public:
  std::map<Vertex *, int8_t> from_;
  std::map<Vertex *, int8_t> to_;
  std::shared_ptr<T> data_;

  Vertex(uint64_t id) {
//    Log::debug("an empty vertex created");
    data_ = std::shared_ptr<T>(new T(id));
  }
};

template<typename T>
class Graph {
 public:
  std::unordered_map<uint64_t, Vertex<T> *> vertex_index_;

  Graph() {
    // Log::debug("an empty graph created");
  }

//  Graph(Graph<T> &gra) {
//    // create new vertexes.
//    for (auto &kv : gra.vertex_index_) {
//      uint64_t id = kv.first;
//      Vertex<T> *old_v = kv.second;
//      Vertex<T> *new_v = new Vertex<T>(old_v->data_);
//      vertex_index_[id] = new_v;
//    }
//
//    // create all new edges.
//    for (auto &kv : gra.vertex_index_) {
//      uint64_t id = kv.first;
//      Vertex<T> *old_sv = kv.second;
//      Vertex<T> *new_sv = vertex_index_[id];
//
//      for (auto &kkvv : old_sv->to_) {
//        Vertex<T> *old_tv = kkvv.first;
//        int8_t relation = kkvv.second;
//        Vertex<T> *new_tv = vertex_index_[old_tv->data_.id()];
//        new_sv->to_[new_tv] = relation;
//        new_tv->from_[old_tv] = relation;
//      }
//    }
//  }

  ~Graph() {
    // TODO delete all vertexes.
  }

//  Graph<T> &operator=(Graph<T> &gra) {
//    // TODO delete previous vertexes
//    vertex_index_.clear();
//
//    // create new vertexes.
//    int size1 = gra.vertex_index_.size();
//
//    for (auto &kv : gra.vertex_index_) {
//      uint64_t id = kv.first;
//      Vertex<T> *old_v = kv.second;
//
//      //            verify(old_v != nullptr);
//      //            verify(id == old_v->data_.id());
//      Vertex<T> *new_v = new Vertex<T>(old_v->data_);
//      vertex_index_[id] = new_v;
//    }
//
//    int size2 = gra.vertex_index_.size();
//    verify(size1 == size2);
//
//    //        Log::debug("loop time: %d. new gra size: %d. original gra size:
//    // %d", uuu, (int)id_index_.size(), (int) gra.id_index_.size());
//    verify(vertex_index_.size() == gra.vertex_index_.size());
//
//    // create all new edges.
//    for (auto &kv : gra.vertex_index_) {
//      uint64_t id = kv.first;
//      Vertex<T> *old_sv = kv.second;
//      Vertex<T> *new_sv = vertex_index_[id];
//
//      for (auto &kkvv : old_sv->to_) {
//        Vertex<T> *old_tv = kkvv.first;
//        int8_t relation = kkvv.second;
//        Vertex<T> *new_tv = vertex_index_[old_tv->data_.id()];
//
//        //                verify(gra.id_index_.find(old_tv->data_.id()) !=
//        // gra.id_index_.end());
//        verify(new_tv != nullptr);
//        new_sv->to_[new_tv] = relation;
//        new_tv->from_[new_sv] = relation;
//      }
//    }
//    return *this;
//  }

  int size() const {
    return vertex_index_.size();
  }

  Vertex<T> *Find(uint64_t id) {
    auto i = vertex_index_.find(id);

    if (i == vertex_index_.end()) {
      return NULL;
    } else {
      return i->second;
    }
  }

  Vertex<T> *FindOrCreate(uint64_t id) {
    auto &v = vertex_index_[id];

    if (v == nullptr) {
      v = new Vertex<T>(id);
      v->data_->set_id(id);
    }
    return v;
  }

//  bool Insert(Vertex<T> *vertex) {
//    auto i = vertex_index_.find(vertex->data_.id());
//    if (i != vertex_index_.end()) {
//      return false;
//    }
//    vertex_index_[vertex->data_.id()] = vertex;
//    for (auto &kv : vertex->from_) {
//      Vertex<T> *a = kv.first;
//      a->to_[vertex] |= kv.second;
//    }
//
//    for (auto &kv : vertex->to_) {
//      Vertex<T> *d = kv.first;
//      d->from_[vertex] |= kv.second;
//    }
//    return true;
//  }

  std::set<Vertex<T> *> FindAncestor(Vertex<T> *vertex) {
    std::set<Vertex<T> *> ret;
    std::vector<Vertex<T> *> togo;

    togo.push_back(vertex);

    while (togo.size() > 0) {
      Vertex<T> *v = togo.back();
      togo.pop_back();

      if (ret.find(v) == ret.end()) {
        // this one I have not gone through
        ret.insert(v);

        for (auto &kv : v->from_) {
          if (ret.find(kv.first) == ret.end()) {
            // this one I have not gone through
            togo.push_back(kv.first);
          } else {
            // this one I have gone through
          }
        }
      } else {
        // this one I have gone through
      }
    }
    ret.erase(vertex);
    return ret;
  }

  std::vector<Vertex<T> *> StrongConnect(
      Vertex<T> *v,
      std::map<Vertex<T> *, int> &indexes,
      std::map<Vertex<T> *, int> &lowlinks,
      int &index,
      std::vector<Vertex<T> *> &S) {
    indexes[v] = index;
    lowlinks[v] = index;
    index++;
    S.push_back(v);

    for (auto &kv : v->to_) {
      Vertex<T> *w = kv.first;

      if (indexes.find(w) == indexes.end()) {
        this->StrongConnect(w, indexes, lowlinks, index, S);
        lowlinks[v] = (lowlinks[v] < lowlinks[w]) ? lowlinks[v] : lowlinks[w];
      } else {
        for (auto &t : S) {
          if (t == w) {
            lowlinks[v] = lowlinks[v] < indexes[w] ? lowlinks[v] : indexes[w];
          }
        }
      }
    }

    std::vector<Vertex<T> *> ret;

    if (lowlinks[v] == indexes[v]) {
      Vertex<T> *w;

      do {
        w = S.back();
        S.pop_back();
        ret.push_back(w);
      } while (w != v);
    }
    return ret;
  }

  void QuickSortVV(std::vector<Vertex<T> *> &vv, int p, int r) {
    if (p >= r) {
      return;
    }

    int i = p - 1;
    auto x = vv[r];

    for (int j = p; j < r; j++) {
      if (vv[j]->data_->id() < x->data_->id()) {
        i++;
        auto tmp = vv[j];
        vv[j] = vv[i];
        vv[i] = tmp;
      }
    }

    int q = i + 1;
    auto tmp = vv[r];
    vv[r] = vv[q];
    vv[q] = tmp;

    QuickSortVV(vv, p, q - 1);
    QuickSortVV(vv, q + 1, r);
  }

  void SortVV(std::vector<Vertex<T> *> &vv, int flag) {
    QuickSortVV(vv, 0, vv.size() - 1);

    if (flag) {
      std::reverse(vv.begin(), vv.end());
    }
  }

  std::vector<Vertex<T> *> FindSortedSCC(
      Vertex<T> *vertex,
      std::vector<Vertex<T> *> *ret_sorted_scc) {
    std::map<Vertex<T> *, int> indexes;
    std::map<Vertex<T> *, int> lowlinks;
    int index = 0;
    std::vector<Vertex<T> *> S;

    std::vector<Vertex<T> *> &ret2 = *ret_sorted_scc;

    std::vector<Vertex<T> *> ret = StrongConnect(vertex,
                                                 indexes,
                                                 lowlinks,
                                                 index,
                                                 S);

    verify(ret.size() > 0);

    // if (RandomGenerator::rand(1, 100) == 1) {
    //    Log::info("scc size: %d", ret.size());
    // }

    // topology sort following type 2 and 3.
    // to ensure that the result is unique,

    // initial sets must be sorted, and DFS should also follow the order of txn
    // id.

    std::set<Vertex<T> *> scc_set(ret.begin(), ret.end());
    verify(scc_set.size() == ret.size());

    std::vector<Vertex<T> *> start_vv;

    // should ignore those which are not in the SCC.
    for (auto &v : scc_set) {
      bool type2 = false;

      for (auto &kv : v->from_) {
        Vertex<TxnInfo> *vt = kv.first;
        int8_t relation = kv.second;

        if (relation > WW) {
          if (scc_set.find(vt) != scc_set.end()) {
            type2 = true;
          } else {
            //                        Log::debug("parent type greater than 2 but
            // not in the same scc");
          }
        }
      }

      if (!type2) {
        start_vv.push_back(v);
      }
    }

    // sort ret following the id;
    verify(start_vv.size() > 0);
    SortVV(start_vv, 1);

    std::map<Vertex<T> *, int> dfs_status; // 2 for visited vertex.

    while (start_vv.size() > 0) {
      auto v = start_vv.back();
      start_vv.pop_back();

      dfs_status[v] = 1;
      ret2.push_back(v);

      // find children connected by R->W an W->R
      std::vector<Vertex<T> *> children;

      for (auto &kv : v->to_) {
        Vertex<TxnInfo> *vt = kv.first;
        int8_t relation = kv.second;

        // should only involve child in the scc.
        if ((relation >= 2) && (scc_set.find(vt) != scc_set.end())) {
          children.push_back(vt);
        }
      }

      // sort all children following id.

      if (children.size() > 0) {
        SortVV(children, 0);
      }

      for (auto &cv : children) {
        // judge if all its parents by have been visited.
        bool all_p_v = true;

        for (auto &pkv : cv->from_) {
          if (pkv.second < 2) {
            continue;
          } else {
            if (dfs_status[pkv.first] != 1) {
              all_p_v = false;
              break;
            }
          }
        }

        if (all_p_v == true) {
          start_vv.push_back(cv);
        }
      }
    }
    verify(ret2.size() == ret.size());
    return ret2;
  }

  std::vector<Vertex<T> *> FindSCC(Vertex<T> *vertex) {
    std::map<Vertex<T> *, int> indexes;
    std::map<Vertex<T> *, int> lowlinks;
    int index = 0;
    std::vector<Vertex<T> *> S;
    std::vector<Vertex<T> *> ret = StrongConnect(vertex,
                                                 indexes,
                                                 lowlinks,
                                                 index,
                                                 S);

    // if (RandomGenerator::rand(1, 100) == 1) {
    //    Log::info("scc size: %d", ret.size());
    // }

    return ret;
  }

//  std::vector<Vertex<T> *> find_scc(T &data) {
//    std::vector<int> ret;
//    Vertex<T> *v = this->Find(data.id());
//    verify(v != NULL);
//    return find_scc(v);
//  }

  std::vector<Vertex<T> *> FindSCC(uint64_t id) {
    std::vector<int> ret;
    Vertex<T> *v = this->Find(id);
    verify(v != NULL);
    return FindSCC(v);
  }

  void Aggregate(const Graph<T> &gra, bool is_server = false) {
    verify(gra.size() > 0);
    std::set<Vertex<T> *> new_vs;

    for (auto &pair : gra.vertex_index_) {
      auto &v = pair.second;
      // check if i have this vertex in my graph
      Vertex<T> *new_ov;
      auto i = this->vertex_index_.find(v->data_->id());

      if (i == vertex_index_.end()) {
        //       Log::debug("union: insert a new node in to the graph. node id:
        // %llx", v->data_.id());
        new_ov = new Vertex<T>(v->data_->id());
        new_ov->data_ = v->data_;
        vertex_index_[new_ov->data_->id()] = new_ov;
      } else {
        //       Log::debug("union: the node is already in the graph. node id:
        // %llx", v->data_.id());
        new_ov = i->second;
        new_ov->data_->union_data(*(v->data_), false, is_server);
      }

      for (auto &kv : v->to_) {
        Vertex<T> *tv = kv.first;
        auto i = vertex_index_.find(tv->data_->id());
        Vertex<T> *new_tv;

        if (i == vertex_index_.end()) {
          new_tv = new Vertex<T>(tv->data_->id());
          new_tv->data_ = tv->data_;
          vertex_index_[new_tv->data_->id()] = new_tv;
        } else {
          new_tv = i->second;
        }

        // do the logic
        int relation = kv.second; // TODO?
        new_ov->to_[new_tv] |= relation;
        new_tv->from_[new_ov] |= relation;
      }
      new_vs.insert(new_ov);
    }

    for (auto *v : new_vs) {
      v->data_->trigger();
    }
  }
};

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
    int32_t size = v->to_.size();
    m << size;

    for (auto &it : v->to_) {
      Vertex<TxnInfo> *vv = it.first;
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
      v->to_[to_v] = type;
      to_v->from_[v] = type;
    }
  }

  verify(gra.size() > 0);
  return m;
}

struct GraphMarshaler {
  Graph<TxnInfo> *gra = nullptr;

  // std::set<Vertex<TxnInfo>*> ret_set;
  std::unordered_set<Vertex<TxnInfo> *> ret_set;

  bool self_create = false;

  ~GraphMarshaler() {
    if (self_create) {
      verify(gra);
      delete gra;
    }
  }

  void write_to_marshal(rrr::Marshal &m) const;

  void marshal_help_1(
      rrr::Marshal &m,
      const std::unordered_set<Vertex<TxnInfo> *> &ret_set,
      Vertex<TxnInfo> *old_sv
  ) const;

  void marshal_help_2(
      rrr::Marshal &m,
      const std::unordered_set<Vertex<TxnInfo> *> &ret_set,
      Vertex<TxnInfo> *old_sv
  ) const;
};

inline rrr::Marshal &operator>>(rrr::Marshal &m, GraphMarshaler &gra_m) {
  verify(gra_m.gra == nullptr);

  gra_m.gra = new Graph<TxnInfo>();

  m >> *(gra_m.gra);

  gra_m.self_create = true;
  return m;
}

inline rrr::Marshal &operator<<(rrr::Marshal &m, const GraphMarshaler &gra_m) {
  gra_m.write_to_marshal(m);
  return m;
}
} // namespace rcc
