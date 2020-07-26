#pragma once

#include "__dep__.h"
#include "constants.h"
#include "marshallable.h"

namespace janus {

template<class T>
class ParentEdge {
 public:
  std::set<parid_t> partitions_{}; // parent's partitions
  T* cache_ptr_{nullptr};
  bool operator==(const ParentEdge<T>& rhs) const {
//    verify(0);
//    return false;
//    return partitions_ == rhs.partitions_;
    return true;
  }
};


template<class T>
void ReplaceParentEdges(vector<pair<txid_t, ParentEdge<T>>>& lhs,
    vector<pair<txid_t, ParentEdge<T>>>& rhs) {
  for (auto& pair: rhs) {
    auto& id = pair.first;
    auto& edge = pair.second;
    for (auto& p2 : lhs) {
      if (p2.first == id) {
        edge.cache_ptr_ = p2.second.cache_ptr_;
        break;
      }
    }
  }
  lhs = std::move(rhs);
};

// This is a CRTP
template<class T>
class Vertex {
 private:
//  std::shared_ptr<T> data_{};
 public:
//  map<uint64_t, rank_t> parents_{}; // parent vertex ids in the graph.
//  map<T *, int8_t> outgoing_{}; // helper data structure, deprecated
//  map<T *, int8_t> incoming_{}; // helper data structure, deprecated
//  set<T *> removed_children_{}; // helper data structure, deprecated
//  bool walked_{false}; // flag for traversing.
  // for scc computation
  typedef vector<pair<txid_t, ParentEdge<T>>> parent_set_t;
  class SccHelper {
   private:
    parent_set_t parents_{};
    Vertex<T>* v_;

   public:
    int64_t scc_index_{-1};
    int64_t scc_lowlink_{0};
    bool scc_onstack_{false};
    set<txid_t> scc_ids_{};
    std::shared_ptr<vector<T*>> scc_{std::make_shared<vector<T*>>()};

    SccHelper() = delete;
    SccHelper(Vertex<T>* v): v_(v) {}

    parent_set_t& parents() {
      __debug_check();
      return parents_;
    }

    void __debug_check() {
#ifdef DEBUG_CHECK
      for (auto& pair: parents_) {
        verify(pair.first != v_->id());
      }
#endif
    }

    void ReplaceParentEdges(txid_t txid, parent_set_t& rhs) {
      __debug_check();
      for (auto& pair: rhs) {
        auto& id = pair.first;
        auto& edge = pair.second;
//        verify(id != txid);
        for (auto& p2 : parents_) {
          if (p2.first == id) {
            edge.cache_ptr_ = p2.second.cache_ptr_;
            break;
          }
        }
      }
      parents_ = std::move(rhs);
      __debug_check();
    };
  };
  SccHelper scc_i_{this};
  SccHelper scc_d_{this};

  SccHelper& scchelper(int rank) {
    verify(rank == RANK_D || rank == RANK_I);
    if (rank == RANK_I) {
      return scc_i_;
    } else if (rank == RANK_D) {
      return scc_d_;
    } else {
      verify(0);
    }
    return scc_d_;
  }

  T *this_pointer() {
    T *ret = static_cast<T *>(this);
    verify(ret != nullptr);
    return ret;
  }

  Vertex() {
//    data_ = std::shared_ptr<T>(new T(id));
  }
  Vertex(Vertex &v)  {
    verify(0);
  }
  virtual ~Vertex() {};
/*
  parent_set_t& GetParents() {
#ifdef DEBUG_CODE
    set<uint64_t> ret;
    for (auto& pair : incoming_) {
      ret.insert(pair.first->id());
    }
    verify(ret == parents_);
#endif
    return parents_;
  }
*/

  virtual void AddParentEdge(shared_ptr<T> rhs_v, rank_t rank) {
    // printf("add edge: %d -> %d\n", this->id(), other->id());
    verify(rhs_v->scchelper(rank).parents().size() >= 0);
    auto id = rhs_v->id();
//    parents_[id] |= weight;
//    auto& e = parents_[id];
    ParentEdge<T> e;
    e.partitions_ = rhs_v->subtx(rank).partition_;
    e.cache_ptr_ = rhs_v.get();
    verify(e.cache_ptr_ != this);
    scchelper(rank).parents().push_back(std::make_pair(id, e));
    scchelper(rank).__debug_check();
  }

  virtual uint64_t id() {
    verify(0);
    return 0;
  }

  bool operator==(Vertex &rhs) const {
    verify(0);
    return true;
  }

  bool operator!=(Vertex &rhs) const {
    return !(*this == rhs);
  }
};

template<typename V>
using Scc = vector<V*>;

// V is vertex type
template<typename V>
class Graph : public Marshallable {
 public:
  typedef std::vector<V *> VertexList;
  bool managing_memory_{true};
  std::unordered_map<uint64_t, shared_ptr<V>> vertex_index_{};
  uint64_t scc_next_index_{1};
  vector<V*> scc_S_{};
  vector<std::pair<V*, int>> scc_search_stack_{};
  vector<V*> scc_search_stack_min_{};

  virtual std::unordered_map<uint64_t, shared_ptr<V>> &vertex_index() {
    verify(managing_memory_);
    return vertex_index_;
  };

  Graph(bool managing_memory = true) : Marshallable(MarshallDeputy::UNKNOWN),
                                       managing_memory_(managing_memory) {}
  Graph(const VertexList &vertices) : Graph() {
    AddV(vertices);
  }
  void Clear() {
    vertex_index().clear();
  }

  virtual ~Graph() {
    Clear();
  }

  virtual void AddV(shared_ptr<V> &sp_v) {
    vertex_index()[sp_v->id()] = sp_v;
    verify(managing_memory_);
  }

  void AddV(VertexList &l) {
    for (auto v : l) {
      AddV(v);
    }
  }

  virtual shared_ptr<V>& FindV(uint64_t id) {
    auto i = vertex_index().find(id);
    if (i == vertex_index().end()) {
      verify(0);
    } else {
      return i->second;
    }
  }

  virtual shared_ptr<V> CreateV(uint64_t id) {
    verify(managing_memory_);
    shared_ptr<V> sp_v(new V(id));
    AddV(sp_v);
    verify(sp_v->id() == id);
#ifdef DEBUG_CODE
    verify(FindV(id));
#endif
    return sp_v;
  }

  virtual shared_ptr<V> CreateV(V &av) {
    verify(managing_memory_);
    shared_ptr<V> sp_v(new V(av));
    AddV(sp_v);
    verify(sp_v->id() == av.id());
    return sp_v;
  }

  void Remove(uint64_t id) {
    verify(managing_memory_);
    auto v = FindV(id);
    vertex_index().erase(id);
    // remove its parent and children pointers.
//    for (auto& pair: v->outgoing_) {
//      auto vv  = pair.first;
//      vv->incoming_.erase(v);
//      vv->parents_.erase(v->id());
//    }
//    for (auto& pair: v->incoming_) {
//      auto vv = pair.first;
//      vv->outgoing_.erase(v);
//    }
  }

  virtual shared_ptr<V> FindOrCreateV(uint64_t id) {
    auto i = vertex_index().find(id);
    if (i == vertex_index().end()) {
      auto v = CreateV(id);
      return v;
    } else {
      return i->second;
    }
  }

  virtual V* FindOrCreateParentVPtr(V& v, txid_t id, ParentEdge<V>& e) {
    auto& x = e.cache_ptr_;
    if (x == nullptr) {
      auto y = FindOrCreateV(id);
      x = y.get();
    }
    return x;
  }

  virtual shared_ptr<V> FindOrCreateV(V &av) {
    auto i = vertex_index().find(av.id());
    if (i == vertex_index().end()) {
      auto v = CreateV(av);
      return v;
    } else {
      return i->second;
    }
  }

  uint64_t size() const {
    return const_cast<Graph *>(this)->vertex_index().size();
  }

  enum SearchHint {
    Exit = 0,      // quit search
    Ok = 1,        // go on
    Skip = 2       // skip the current branch
  };

  // depth first search.
  int TraversePredNonRecursive(V& vertex,
                               rank_t rank,
                               const function<int(V& self, V& parent, unordered_set<V*>&)> &func,
                               const function<void(V& self)> &func_end = {},
                               bool cycle_detection = true) {
    auto to_walk = make_shared<vector<V *>>();
    auto walked = make_shared<unordered_set<V *>>();
    to_walk->push_back(&vertex);
    walked->insert(&vertex);

    int ret = SearchHint::Ok;
    int __debug_depth = 0;
    while (!to_walk->empty()) {
      verify(__debug_depth++ < 100000000);
      auto vvv = to_walk->back();
      if (cycle_detection) {
        walked->insert(vvv);
      }
      to_walk->pop_back();
      // because this coroutine can be switched out,
      // be careful when using iterator.
      vector<V*> vs;
      for (auto &pair : vvv->scchelper(rank).parents()) {
        auto &parent_id = pair.first;
        auto v = FindOrCreateParentVPtr(*vvv, parent_id, pair.second);
        vs.push_back(v);
      }
      for (auto& v: vs) {
        verify(vvv != v);
        ret = func(*vvv, *v, *walked);
        if (ret == SearchHint::Exit) {
          return ret;
        } else if (ret == SearchHint::Skip) {
          continue;
        }
        if (cycle_detection) {
          if (walked->count(v) > 0) {
            continue;
          }
        }
        to_walk->push_back(v);
        verify(ret == SearchHint::Ok);
      }
    }
    if (func_end) {
      for (auto& v : *walked) {
        func_end(*v);
      }
    }
    return ret;
  }

  // depth first search.
  int TraversePred(V& vertex,
                   rank_t rank,
                   int64_t depth,
                   function<int(V& )> &func,
                   shared_ptr<set<V *>> walked = nullptr) {
    if (walked == nullptr) {
      walked = make_shared<set<V *>>();
    }
//    if (walked.vertex.walked_) {
//      return true;
//    } else {
//      vertex.walked_ = true;
//    }
//    walked->push_back(&vertex);

    int ret = SearchHint::Ok;
    for (auto &pair : vertex.scchelper(rank).parents()) {
      auto v = FindV(pair.first);
      if (!walked->insert(v.get()).second) {
        continue;
      }
      ret = func(*v);
      if (ret == SearchHint::Exit) {
        break;
      } else if (ret == SearchHint::Skip) {
        continue;
      }
      verify(ret == SearchHint::Ok);
      if (depth < 0 || depth > 0) {
        ret = TraversePred(*v, rank, depth - 1, func, walked);
        if (ret == SearchHint::Exit)
          break;
      }
    }
    return ret;
  }

  // what does ret value stand for ???
  // false: aborted by user?
  bool TraversePred(V& vertex,
                    rank_t rank,
                    int64_t depth,
                    function<bool(V& )> &func,
                    set<V *> &walked) {
    auto pair = walked.insert(&vertex);
    if (!pair.second)
      // already traversed.
      return true;

    for (auto &p : vertex.parents_) {
      auto v = FindV(p.first);
      if (!func(*v))
        return false; // traverse aborted by users.
      if (depth < 0 || depth > 0) {
        if (!TraversePred(*v, rank, depth - 1, func, walked))
          return false;
      }
    }
    return true;
  }

  bool TraverseDescendant(V& vertex,
                          int64_t depth,
                          function<bool(V *)> &func,
                          set<V *> &walked,
                          int edge_type = EDGE_ALL) {
    verify(0);
//    auto pair = walked.insert(vertex);
//    if (!pair.second)
//      // already traversed.
//      return true;
//
//    for (auto& pair : vertex->outgoing_) {
//      auto& v = pair.first;
//      auto& e = pair.second;
//      if (edge_type != EDGE_ALL && edge_type != e)
//        continue;
//      if (!func(v))
//        return false; // traverse aborted by users.
//      if (depth < 0 || depth > 0) {
//        if (!TraverseDescendant(v, depth - 1, func, walked, edge_type))
//          return false;
//      }
//    }
    return true;
  }

// #define SCC_DEBUG 0
  void StrongConnectPredNonRecursive(V& vvvv,
                                     rank_t rank,
                                     map<V*, int> &indexes,
                                     map<V*, int> &lowlinks,
                                     int &index,
                                     vector<V*> &S,
                                     Scc<V>* p_ret) {

    verify(scc_search_stack_.empty());
    scc_search_stack_.push_back(std::make_pair(&vvvv, 0));
    while (!scc_search_stack_.empty()) {

      auto& pair = scc_search_stack_.back();
      auto& v = *pair.first;
      auto i = pair.second;
      scc_search_stack_.pop_back();
      if (i == 0) {
        verify(v.scchelper(rank).scc_index_ < 0);
        v.scchelper(rank).scc_index_ = scc_next_index_;
        v.scchelper(rank).scc_lowlink_ = scc_next_index_;
        scc_next_index_++;
        scc_S_.push_back(&v);
        v.scchelper(rank).scc_onstack_ = true;
      }

#ifdef SCC_DEBUG
    indexes[&v] = index;
    lowlinks[&v] = index;
    index++;
    verify(scc_next_index_ == index);
    S.push_back(&v);
#endif
    bool recurse = false;
    auto& parents = v.scchelper(rank).parents();
    for (; i < parents.size(); i++) {
      auto& p = parents[i];
      auto& w = *FindOrCreateParentVPtr(v, p.first, p.second);
      if (!w.scchelper(rank).scc_->empty()) // opt scc already computed
        continue;
#ifdef SCC_DEBUG
      verify((indexes.find(&w) == indexes.end()) == (w.scc_index_ < 0));
#endif
      if (w.scchelper(rank).scc_index_ < 0) {
        recurse = true;
        scc_search_stack_.push_back(std::make_pair(&v, i+1));
        scc_search_stack_.push_back(std::make_pair(&w, 0));
        break;

//      if (indexes.find(w) == indexes.end()) {
        this->StrongConnectPred(w, rank, indexes, lowlinks, index, S, nullptr);
#ifdef SCC_DEBUG
        lowlinks[&v] = (lowlinks[&v] < lowlinks[&w]) ?
                       lowlinks[&v] : lowlinks[&w];
#endif
        v.scchelper(rank).scc_lowlink_ = v.scchelper(rank).scc_lowlink_ < w.scchelper(rank).scc_lowlink_ ?
                                         v.scchelper(rank).scc_lowlink_ : w.scchelper(rank).scc_lowlink_;
      } else {
        if (w.scchelper(rank).scc_onstack_) {
          v.scchelper(rank).scc_lowlink_ = v.scchelper(rank).scc_lowlink_ < w.scchelper(rank).scc_index_ ?
                                           v.scchelper(rank).scc_lowlink_ : w.scchelper(rank).scc_index_;
        }
#ifdef SCC_DEBUG
        for (auto &t : S) {
          if (t == &w) {
            lowlinks[&v] = lowlinks[&v] < indexes[&w] ?
                           lowlinks[&v] : indexes[&w];
            verify(lowlinks[&v] == v.scc_lowlink_);
          }
        }
#endif
      }
    }
    if (recurse)
      continue;
//    Scc<V>& ret = *p_ret;
//    if (lowlinks[&v] == indexes[&v]) {
#ifdef SCC_DEBUG
    verify((lowlinks[&v] == indexes[&v]) == (v.scc_lowlink_ == v.scc_index_));
#endif
    if (v.scchelper(rank).scc_lowlink_ == v.scchelper(rank).scc_index_) {
      verify(v.scchelper(rank).scc_->empty());
//      v.scc_ = std::make_shared<vector<V*>>();
      V* w;
      do {
        w = scc_S_.back();
        scc_S_.pop_back();
        w->scchelper(rank).scc_onstack_ = false;
        v.scchelper(rank).scc_->push_back(w);
//        if (p_ret) {
//          p_ret->push_back(w);
//        }
      } while (w != &v);

      sort(v.scchelper(rank).scc_->begin(), v.scchelper(rank).scc_->end(), [](V* tx1, V* tx2)->bool{
        return tx1->id() < tx2->id();
      });

      for (auto x : *v.scchelper(rank).scc_) {
        x->scchelper(rank).scc_ = v.scchelper(rank).scc_;
      }
#ifdef DEBUG_CHECK
      bool xx = std::any_of(v.scchelper(rank).scc_->begin(), v.scchelper(rank).scc_->end(), [&](V* tx){
        return (tx->id()==v.id());
      });
      verify(xx);
#endif
    }
    if (!scc_search_stack_.empty()) {
      auto& w = v;
      auto& v = *scc_search_stack_.back().first;
#ifdef SCC_DEBUG
      lowlinks[&v] = (lowlinks[&v] < lowlinks[&w]) ?
                       lowlinks[&v] : lowlinks[&w];
#endif
      v.scchelper(rank).scc_lowlink_ = v.scchelper(rank).scc_lowlink_ < w.scchelper(rank).scc_lowlink_ ?
                                       v.scchelper(rank).scc_lowlink_ : w.scchelper(rank).scc_lowlink_;
    }

    }
  }

//  std::vector<V*> StrongConnect(V* v,
//                                std::map<V*, int> &indexes,
//                                std::map<V*, int> &lowlinks,
//                                int &index,
//                                std::vector<V*> &S) {
//    indexes[v] = index;
//    lowlinks[v] = index;
//    index++;
//    S.push_back(v);
//
//    for (auto &kv : v->outgoing_) {
//      V* w = (V*)kv.first;
//      if (w->scc_) // opt scc already computed
//        continue;
//
//      if (indexes.find(w) == indexes.end()) {
//        this->StrongConnect(w, indexes, lowlinks, index, S);
//        lowlinks[v] = (lowlinks[v] < lowlinks[w]) ? lowlinks[v] : lowlinks[w];
//      } else {
//        for (auto &t : S) {
//          if (t == w) {
//            lowlinks[v] = lowlinks[v] < indexes[w] ? lowlinks[v] : indexes[w];
//          }
//        }
//      }
//    }
//
//    vector<V*> ret;
//    if (lowlinks[v] == indexes[v]) {
//      V *w;
//      do {
//        w = S.back();
//        S.pop_back();
//        ret.push_back(w);
//      } while (w != v);
//    }
//    return ret;
//  }

// #define SCC_DEBUG 0
  void StrongConnectPred(V& v,
                         rank_t rank,
                         map<V*, int> &indexes,
                         map<V*, int> &lowlinks,
                         int &index,
                         vector<V*> &S,
                         Scc<V>* p_ret) {
    verify(v.scchelper(rank).scc_index_ < 0);
    v.scchelper(rank).scc_index_ = scc_next_index_;
    v.scchelper(rank).scc_lowlink_ = scc_next_index_;
    scc_next_index_++;
    scc_S_.push_back(&v);
    v.scchelper(rank).scc_onstack_ = true;

#ifdef SCC_DEBUG
    indexes[&v] = index;
    lowlinks[&v] = index;
    index++;
    verify(scc_next_index_ == index);
    S.push_back(&v);
#endif

    for (auto &p : v.scchelper(rank).parents()) {
      auto& w = *FindOrCreateParentVPtr(v, p.first, p.second);
      if (!w.scchelper(rank).scc_->empty()) // opt scc already computed
        continue;
#ifdef SCC_DEBUG
      verify((indexes.find(&w) == indexes.end()) == (w.scc_index_ < 0));
#endif
      if (w.scchelper(rank).scc_index_ < 0) {
//      if (indexes.find(w) == indexes.end()) {
        this->StrongConnectPred(w, rank, indexes, lowlinks, index, S, nullptr);
#ifdef SCC_DEBUG
        lowlinks[&v] = (lowlinks[&v] < lowlinks[&w]) ?
                       lowlinks[&v] : lowlinks[&w];
#endif
        v.scchelper(rank).scc_lowlink_ = v.scchelper(rank).scc_lowlink_ < w.scchelper(rank).scc_lowlink_ ?
            v.scchelper(rank).scc_lowlink_ : w.scchelper(rank).scc_lowlink_;
      } else {
        if (w.scchelper(rank).scc_onstack_) {
          v.scchelper(rank).scc_lowlink_ = v.scchelper(rank).scc_lowlink_ < w.scchelper(rank).scc_index_ ?
              v.scchelper(rank).scc_lowlink_ : w.scchelper(rank).scc_index_;
        }
#ifdef SCC_DEBUG
        for (auto &t : S) {
          if (t == &w) {
            lowlinks[&v] = lowlinks[&v] < indexes[&w] ?
                           lowlinks[&v] : indexes[&w];
            verify(lowlinks[&v] == v.scc_lowlink_);
          }
        }
#endif
      }
    }

//    Scc<V>& ret = *p_ret;
//    if (lowlinks[&v] == indexes[&v]) {
#ifdef SCC_DEBUG
    verify((lowlinks[&v] == indexes[&v]) == (v.scc_lowlink_ == v.scc_index_));
#endif
    if (v.scchelper(rank).scc_lowlink_ == v.scchelper(rank).scc_index_) {
      verify(v.scchelper(rank).scc_->empty());
//      v.scc_ = std::make_shared<vector<V*>>();
      V* w;
      do {
        w = scc_S_.back();
        scc_S_.pop_back();
        w->scchelper(rank).scc_onstack_ = false;
        v.scchelper(rank).scc_->push_back(w);
//        if (p_ret) {
//          p_ret->push_back(w);
//        }
      } while (w != &v);

      sort(v.scchelper(rank).scc_->begin(), v.scchelper(rank).scc_->end(), [](V* tx1, V* tx2)->bool{
        return tx1->id() < tx2->id();
      });

      for (auto x : *v.scchelper(rank).scc_) {
        x->scchelper(rank).scc_ = v.scchelper(rank).scc_;
      }
#ifdef DEBUG_CHECK
      bool xx = std::any_of(v.scchelper(rank).scc_->begin(), v.scchelper(rank).scc_->end(), [&](V* tx){
        return (tx->id()==v.id());
      });
      verify(xx);
#endif
    }
  }

//  std::vector<V*> StrongConnect(V* v,
//                                std::map<V*, int> &indexes,
//                                std::map<V*, int> &lowlinks,
//                                int &index,
//                                std::vector<V*> &S) {
//    indexes[v] = index;
//    lowlinks[v] = index;
//    index++;
//    S.push_back(v);
//
//    for (auto &kv : v->outgoing_) {
//      V* w = (V*)kv.first;
//      if (w->scc_) // opt scc already computed
//        continue;
//
//      if (indexes.find(w) == indexes.end()) {
//        this->StrongConnect(w, indexes, lowlinks, index, S);
//        lowlinks[v] = (lowlinks[v] < lowlinks[w]) ? lowlinks[v] : lowlinks[w];
//      } else {
//        for (auto &t : S) {
//          if (t == w) {
//            lowlinks[v] = lowlinks[v] < indexes[w] ? lowlinks[v] : indexes[w];
//          }
//        }
//      }
//    }
//
//    vector<V*> ret;
//    if (lowlinks[v] == indexes[v]) {
//      V *w;
//      do {
//        w = S.back();
//        S.pop_back();
//        ret.push_back(w);
//      } while (w != v);
//    }
//    return ret;
//  }

  void QuickSortVV(std::vector<V *> &vv, int p, int r) {
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

  void SortVV(std::vector<V *> &vv, int flag) {
    QuickSortVV(vv, 0, vv.size() - 1);

    if (flag) {
      std::reverse(vv.begin(), vv.end());
    }
  }
//
//  std::vector<V *> FindSortedSCC(V*vertex,
//                                         vector<V*> *ret_sorted_scc) {
//    std::map<V*, int> indexes;
//    std::map<V*, int> lowlinks;
//    int index = 0;
//    std::vector<V*> S;
//    std::vector<V*> &ret2 = *ret_sorted_scc;
//    std::vector<V*> ret =
//        StrongConnect(vertex, indexes, lowlinks, index, S);
//    verify(ret.size() > 0);
//
//    std::set<V*> scc_set(ret.begin(), ret.end());
//    verify(scc_set.size() == ret.size());
//
//    std::vector<V*> start_vv;
//
//    // should ignore those which are not in the SCC.
//    for (auto &v : scc_set) {
//      bool type2 = false;
//
//      for (auto &kv : v->incoming_) {
//        V* vt = kv.first;
//        int8_t relation = kv.second;
//
//        if (relation > WW) {
//          if (scc_set.find(vt) != scc_set.end()) {
//            type2 = true;
//          } else {
//            // Log::debug("parent type greater than 2 but not in the same scc");
//          }
//        }
//      }
//
//      if (!type2) {
//        start_vv.push_back(v);
//      }
//    }
//
//    // sort ret following the id;
//    verify(start_vv.size() > 0);
//    SortVV(start_vv, 1);
//
//    std::map<V*, int> dfs_status;  // 2 for visited vertex.
//
//    while (start_vv.size() > 0) {
//      auto v = start_vv.back();
//      start_vv.pop_back();
//
//      dfs_status[v] = 1;
//      ret2.push_back(v);
//
//      // find children connected by R->W an W->R
//      std::vector<V*> children;
//
//      for (auto &kv : v->outgoing_) {
//        V* vt = kv.first;
//        int8_t relation = kv.second;
//
//        // should only involve child in the scc.
//        if ((relation >= 2) && (scc_set.find(vt) != scc_set.end())) {
//          children.push_back(vt);
//        }
//      }
//
//      // sort all children following id.
//
//      if (children.size() > 0) {
//        SortVV(children, 0);
//      }
//
//      for (auto &cv : children) {
//        // judge if all its parents by have been visited.
//        bool all_p_v = true;
//
//        for (auto &pkv : cv->incoming_) {
//          if (pkv.second < 2) {
//            continue;
//          } else {
//            if (dfs_status[pkv.first] != 1) {
//              all_p_v = false;
//              break;
//            }
//          }
//        }
//
//        if (all_p_v == true) {
//          start_vv.push_back(cv);
//        }
//      }
//    }
//    verify(ret2.size() == ret.size());
//    return ret2;
//  }

  virtual Scc<V> &FindSccPred(V& vertex, int rank) {
    if (!vertex.scchelper(rank).scc_->empty()) {
      // already computed.
      return (Scc<V> &) (*(vertex.scchelper(rank).scc_));
    }
    auto up_indexes = std::make_unique<map<V*, int>>();
    auto up_lowlinks = std::make_unique<map<V*, int>>();
    int index = scc_next_index_;
//    scc_next_index_ = 0;
    auto up_S = std::make_unique<vector<V*>>();
    shared_ptr<Scc<V>> sp_scc(new Scc<V>);
    StrongConnectPredNonRecursive(vertex, rank, *up_indexes,
        *up_lowlinks, index, *up_S, sp_scc.get());
//    verify(!sp_scc->empty());
//    for (auto v : *sp_scc) {
//      v->scc_ = sp_scc;
//    }
//    return *sp_scc;
    return *(vertex.scchelper(rank).scc_);
  }

//  virtual Scc<V>& FindSCC(V *vertex) {
//    if (vertex->scc_) {
//      // already computed.
//      return *vertex->scc_;
//    }
//    map<V *, int> indexes;
//    map<V *, int> lowlinks;
//    int index = 0;
//    std::vector<V *> S;
//    std::shared_ptr<Scc<V>> ptr(new Scc<V>);
//    *ptr = StrongConnect(vertex, indexes, lowlinks, index, S);
//    for (V* v : *ptr) {
//      verify(!v->scc_); // FIXME
//      v->scc_ = ptr;
//    }
//    return *ptr;
//  }

//  Scc<V>& FindSCC(uint64_t id) {
//    std::vector<int> ret;
//    V *v = this->FindV(id);
//    verify(v != NULL);
//    return FindSCC(v);
//  }

//  std::vector<VertexList> SCC() {
//    std::vector<VertexList> result;
//    std::map<V *, int> indexes;
//    std::map<V *, int> lowlinks;
//    VertexList S;
//    int index = 0;
//    for (auto pair : vertex_index()) {
//      auto v = pair.second;
//      if (indexes.find(v) == indexes.end()) {
//        std::vector<V *> component =
//            StrongConnect(v, indexes, lowlinks, index, S);
//        result.push_back(component);
//      }
//    }
//    return result;
//  }

  void Aggregate(const Graph<V> &gra, bool is_server = false) {
    verify(0);
    verify(gra.size() > 0);
    std::set<V *> new_vs;

    for (auto &pair : gra.vertex_index()) {
      auto &v = pair.second;
      // check if i have this vertex in my graph
      V *new_ov;
      auto i = this->vertex_index().find(v->data_->id());

      if (i == vertex_index().end()) {
        //       Log::debug("union: insert a new node in to the graph. node id:
        // %llx", v->data_.id());
        new_ov = new V(v->data_->id());
        new_ov->data_ = v->data_;
        vertex_index()[new_ov->data_->id()] = new_ov;
      } else {
        //       Log::debug("union: the node is already in the graph. node id:
        // %llx", v->data_.id());
        new_ov = i->second;
        new_ov->data_->union_data(*(v->data_), false, is_server);
      }

      for (auto &kv : v->outgoing_) {
        V *tv = kv.first;
        auto j = vertex_index().find(tv->data_->id());
        V *new_tv;

        if (j == vertex_index().end()) {
          new_tv = new V(tv->data_->id());
          new_tv->data_ = tv->data_;
          vertex_index()[new_tv->data_->id()] = new_tv;
        } else {
          new_tv = j->second;
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

  Marshal &ToMarshal(Marshal &m) const override {
    verify(managing_memory_);
    uint64_t n = size();
    verify(n >= 0 && n < 10000);
    m << n;
    int i = 0;
    for (auto &pair : const_cast<Graph *>(this)->vertex_index()) {
      auto &v = pair.second;
      i++;
//      int32_t n_out_edge = v->outgoing_.size();
      m << v->id();
      m << *v;
//      m << n_out_edge;
//      for (auto &it : v->outgoing_) {
//        V* vv = static_cast<V*>(it.first);
//        verify(vv != nullptr);
//        uint64_t id = vv->id();
//        int8_t weight = it.second;
//        m << id << weight;
//      }
    }
//    verify(i == n);
    return m;
  }

  Marshal &FromMarshal(Marshal &m) override {
    verify(managing_memory_);
    verify(size() == 0);
    uint64_t n;
    m >> n;
    verify(n >= 0 && n < 10000);
    map<uint64_t, shared_ptr<V>> ref;
//    map<uint64_t, map<int64_t, int8_t> > v_to;

    // Log::debug("marshalling gra, graph size: %d", (int) n);

    int nn = n;

    while (nn-- > 0) {
      uint64_t v_id;
      m >> v_id;
      ref[v_id].reset(new V(v_id)); // TODO? can new RccDTxn?
      m >> *(ref[v_id]);
//      int32_t n_out_edge;
//      m >> n_out_edge;
//
//      while (n_out_edge-- > 0) {
//        uint64_t child_id;
//        int8_t weight;
//        m >> child_id;
//        m >> weight;
//        v_to[v_id][child_id] = weight;
//      }
    }
    // insert vertexes into graph.
    verify(ref.size() == n);
    for (auto &kv : ref) {
      vertex_index()[kv.first] = kv.second;
    }
    // build edge pointers.
//    for (auto &kv : v_to) {
//      uint64_t v_id = kv.first;
//      map<int64_t, int8_t>& o_to = kv.second;
//      V *v = ref[v_id];
//
//      for (auto &tokv : o_to) {
//        uint64_t child_vid = tokv.first;
//        int8_t weight = tokv.second;
//        V *child_v = ref[child_vid];
//        v->outgoing_[child_v] = weight;
//        child_v->incoming_[v] = weight;
//        child_v->parents_.insert(v->id());
//      }
//    }

//    verify(size() > 0);
    return m;
  }
};
}  // namespace janus
