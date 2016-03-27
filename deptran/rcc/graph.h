#pragma once

#include "__dep__.h"
#include "constants.h"

namespace rococo {

template <typename T>
class Vertex {
 public:
  map<uint64_t, int8_t> parents_ = {};
  std::map<Vertex *, int8_t> outgoing_ = {};
  std::map<Vertex *, int8_t> incoming_ = {};
  std::shared_ptr<T> data_;

  Vertex(uint64_t id) { data_ = std::shared_ptr<T>(new T(id)); }

  Vertex(Vertex<T> &v) { data_ = v.data_;}

  void AddEdge(Vertex<T> *other, int8_t weight) {
    // printf("add edge: %d -> %d\n", this->id(), other->id());
    outgoing_[other] |= weight;
    other->incoming_[this] |= weight;
  }

  void AddParentEdge(Vertex<T> *other, int8_t weight) {
    // printf("add edge: %d -> %d\n", this->id(), other->id());
    incoming_[other] |= weight;
    other->outgoing_[this] |= weight;
  }

  uint64_t id() const { return data_->id(); }
};

template <typename T>
class Graph {
 public:
  typedef std::vector<Vertex<T> *> VertexList;
  std::unordered_map<uint64_t, Vertex<T> *> vertex_index_ = {};

  Graph() {}

  Graph(const VertexList &vertices) { AddV(vertices); }

  void Clear() {
    for (auto p : vertex_index_) {
      auto v = p.second;
      delete v;
    }
    vertex_index_.clear();
  }

  virtual ~Graph() {
    Clear();
  }

  void AddV(Vertex<T> *v) { vertex_index_[v->id()] = v; }

  void AddV(VertexList &l) {
    for (auto v : l) {
      AddV(v);
    }
  }

  Vertex<T> *FindV(uint64_t id) {
    auto i = vertex_index_.find(id);

    if (i == vertex_index_.end()) {
      return NULL;
    } else {
      return i->second;
    }
  }

  Vertex<T> *CreateV(uint64_t id) {
    auto v = new Vertex<T>(id);
    AddV(v);
    verify(v->id() == id);
    return v;
  }

  Vertex<T> *CreateV(Vertex<T>& av) {
    auto v = new Vertex<T>(av);
    AddV(v);
    verify(v->id() == av.id());
    return v;
  }

  Vertex<T> *FindOrCreateV(uint64_t id) {
    auto v = FindV(id);
    if (v == nullptr) v = CreateV(id);
    return v;
  }

  Vertex<T> *FindOrCreateV(Vertex<T>& av) {
    auto v = FindV(av.id());
    if (v == nullptr) v = CreateV(av);
    return v;
  }

  uint64_t size() const { return vertex_index_.size(); }

  // what does ret value stand for ???
  // false: aborted by user?
  bool TraversePred(Vertex<T> *vertex,
                    int64_t depth,
                    function<bool(Vertex<T> *)> &func,
                    set<Vertex<T> *> &walked) {
    auto pair = walked.insert(vertex);
    if (!pair.second)
      // already traversed.
      return true;

    for (auto pair : vertex->incoming_) {
      auto v = pair.first;
      if (!func(v))
        return false; // traverse aborted by users.
      if (depth < 0 || depth > 0) {
        if (!TraversePred(v, depth - 1, func, walked))
          return false;
      }
    }
    return true;
  }

  std::vector<Vertex<T> *> StrongConnect(Vertex<T> *v,
                                         std::map<Vertex<T> *, int> &indexes,
                                         std::map<Vertex<T> *, int> &lowlinks,
                                         int &index,
                                         std::vector<Vertex<T> *> &S) {
    indexes[v] = index;
    lowlinks[v] = index;
    index++;
    S.push_back(v);

    for (auto &kv : v->outgoing_) {
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

  std::vector<Vertex<T> *> FindSortedSCC(Vertex<T> *vertex,
                                         vector<Vertex<T> *> *ret_sorted_scc) {
    std::map<Vertex<T> *, int> indexes;
    std::map<Vertex<T> *, int> lowlinks;
    int index = 0;
    std::vector<Vertex<T> *> S;
    std::vector<Vertex<T> *> &ret2 = *ret_sorted_scc;
    std::vector<Vertex<T> *> ret =
        StrongConnect(vertex, indexes, lowlinks, index, S);
    verify(ret.size() > 0);

    std::set<Vertex<T> *> scc_set(ret.begin(), ret.end());
    verify(scc_set.size() == ret.size());

    std::vector<Vertex<T> *> start_vv;

    // should ignore those which are not in the SCC.
    for (auto &v : scc_set) {
      bool type2 = false;

      for (auto &kv : v->incoming_) {
        Vertex<T> *vt = kv.first;
        int8_t relation = kv.second;

        if (relation > WW) {
          if (scc_set.find(vt) != scc_set.end()) {
            type2 = true;
          } else {
            // Log::debug("parent type greater than 2 but not in the same scc");
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

    std::map<Vertex<T> *, int> dfs_status;  // 2 for visited vertex.

    while (start_vv.size() > 0) {
      auto v = start_vv.back();
      start_vv.pop_back();

      dfs_status[v] = 1;
      ret2.push_back(v);

      // find children connected by R->W an W->R
      std::vector<Vertex<T> *> children;

      for (auto &kv : v->outgoing_) {
        Vertex<T> *vt = kv.first;
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

        for (auto &pkv : cv->incoming_) {
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
    std::vector<Vertex<T> *> ret =
        StrongConnect(vertex, indexes, lowlinks, index, S);

    return ret;
  }

  std::vector<Vertex<T> *> FindSCC(uint64_t id) {
    std::vector<int> ret;
    Vertex<T> *v = this->FindV(id);
    verify(v != NULL);
    return FindSCC(v);
  }

  std::vector<VertexList> SCC() {
    std::vector<VertexList> result;
    std::map<Vertex<T> *, int> indexes;
    std::map<Vertex<T> *, int> lowlinks;
    VertexList S;
    int index = 0;
    for (auto pair : vertex_index_) {
      auto v = pair.second;
      if (indexes.find(v) == indexes.end()) {
        std::vector<Vertex<T> *> component =
            StrongConnect(v, indexes, lowlinks, index, S);
        result.push_back(component);
      }
    }
    return result;
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

      for (auto &kv : v->outgoing_) {
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
        int relation = kv.second;  // TODO?
        new_ov->outgoing_[new_tv] |= relation;
        new_tv->incoming_[new_ov] |= relation;
      }
      new_vs.insert(new_ov);
    }

    for (auto *v : new_vs) {
      v->data_->trigger();
    }
  }
};
}  // namespace rcc
