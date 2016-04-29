#pragma once
#include "__dep__.h"

namespace rococo {

class Marshallable {
 public:
  int32_t rtti_ = -1;
  enum Kind {EMPTY_GRAPH=1, RCC_GRAPH=2};
  std::shared_ptr<Marshallable> data_{};
  virtual ~Marshallable() {};
  virtual Marshal& ToMarshal(Marshal& m) const {verify(0);};
  virtual Marshal& FromMarshal(Marshal& m);
};

inline Marshal& operator>>(Marshal& m, Marshallable& rhs) {
  m >> rhs.rtti_;
  // TODO
  rhs.FromMarshal(m);
  return m;
}

inline Marshal& operator<<(Marshal& m, const Marshallable& rhs) {
  m << rhs.rtti_;
  rhs.ToMarshal(m);
  return m;
}

} // namespace rococo;