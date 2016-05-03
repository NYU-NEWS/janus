#pragma once

#include "__dep__.h"
#include "constants.h"
#include "marshallable.h"

namespace rococo {

template <typename T>
class Vertex {
 public:
  map<uint64_t, int8_t> parents_{};
  map<Vertex *, int8_t> outgoing_{};
  map<Vertex *, int8_t> incoming_{};
  bool walked_{false}; // flag for traversing.
  std::shared_ptr<T> data_{};
  std::shared_ptr<vector<Vertex*>> scc_{};

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

  bool operator== (Vertex<T>& rhs) const {
    for (auto& pair: incoming_) {
      auto id = pair.first->id();
      bool found = std::any_of(rhs.incoming_.begin(),
                               rhs.incoming_.end(),
                               [id] (std::pair<Vertex<T>*, int8_t> ppp) {
                                 return ppp.first->id() == id;
                               });
      if (!found) return false;
    }
    for (auto& pair: outgoing_) {
      auto id = pair.first->id();
      bool found = std::any_of(rhs.outgoing_.begin(),
                               rhs.outgoing_.end(),
                               [id] (std::pair<Vertex<T>*, int8_t> ppp) {
                                 return ppp.first->id() == id;
                               });
      if (!found) return false;
    }
    return true;
  }

  bool operator!= (Vertex<T>& rhs) const {
    return !(*this == rhs);
  }
};

template <typename T>
class Graph : public Marshallable {
 public:
  typedef std::vector<Vertex<T> *> VertexList;
  std::unordered_map<uint64_t, Vertex<T> *> vertex_index_ = {};

  Graph() {}

  Graph(const VertexList &vertices) { AddV(vertices); }

  void Clear() {
    for (auto p : vertex_index_) {
      auto v = p.second;
//      delete v; FIXME
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
                    vector<Vertex<T> *> *walked = nullptr) {
    bool to_clean = false;
    if (walked == nullptr) {
      to_clean = true;
      walked = new vector<Vertex<T>*>;
    }
    if (vertex->walked_) {
      return true;
    } else {
      vertex->walked_ = true;
    }
    walked->push_back(vertex);

    bool ret = true;
    for (auto pair : vertex->incoming_) {
      auto v = pair.first;
      if (!func(v)) {
        ret = false;
        break;
      }
      if (depth < 0 || depth > 0) {
        if (!TraversePred(v, depth - 1, func, walked)) {
          ret = false;
          break;
        }
      }
    }
    if (to_clean) {
      for (Vertex<T>* v: *walked) {
        v->walked_ = false;
      }
      delete walked;
    }

    return ret;
  }

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

  bool TraverseDescendant(Vertex<T> *vertex,
                          int64_t depth,
                          function<bool(Vertex<T> *)> &func,
                          set<Vertex<T> *> &walked,
                          int edge_type = EDGE_ALL) {
    auto pair = walked.insert(vertex);
    if (!pair.second)
      // already traversed.
      return true;

    for (auto pair : vertex->outgoing_) {
      auto v = pair.first;
      auto e = pair.second;
      if (edge_type != EDGE_ALL && edge_type != e)
        continue;
      if (!func(v))
        return false; // traverse aborted by users.
      if (depth < 0 || depth > 0) {
        if (!TraverseDescendant(v, depth - 1, func, walked, edge_type))
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

    vector<Vertex<T>*> ret;
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

  std::vector<Vertex<T> *>& FindSCC(Vertex<T> *vertex) {
    if (vertex->scc_) {
      // already computed.
      return *vertex->scc_;
    }
    map<Vertex<T> *, int> indexes;
    map<Vertex<T> *, int> lowlinks;
    int index = 0;
    std::vector<Vertex<T> *> S;

    vertex->scc_.reset(new vector<Vertex<T>*>);
    *vertex->scc_ = StrongConnect(vertex, indexes, lowlinks, index, S);
    return *vertex->scc_;
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

  Marshal& ToMarshal(Marshal &m) const override{
    uint64_t n = size();
    verify(n > 0 && n < 10000);
    m << n;
    int i = 0;
    for (auto &pair : vertex_index_) {
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

  Marshal& FromMarshal(Marshal &m) override{
    verify(size() == 0);
    uint64_t n;
    m >> n;
    verify(n > 0 && n < 10000);
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
      vertex_index_[kv.first] = kv.second;
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

    verify(size() > 0);
    return m;
  }
};
}  // namespace rcc
