#pragma once
#include "../__dep__.h"
#include "graph.h"
#include "dep_graph.h"

namespace janus {
template<typename T>
inline rrr::Marshal &operator<<(rrr::Marshal &m, const Vertex<T> *&v) {
  verify(0);
  int64_t u = std::uintptr_t(v);
  m << u;
  return m;
}

} // namespace janus
