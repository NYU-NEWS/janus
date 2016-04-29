#pragma once
#include "__dep__.h"

namespace rococo {

class Marshallable : public std::enable_shared_from_this<Marshallable> {
 protected:
  std::shared_ptr<Marshallable> data_{};
 public:
  int32_t rtti_ = -1;
  enum Kind {EMPTY_GRAPH=1, RCC_GRAPH=2};

  virtual ~Marshallable() {};
  virtual Marshal& ToMarshal(Marshal& m) const;
  virtual Marshal& FromMarshal(Marshal& m);
  virtual std::shared_ptr<Marshallable>& ptr() {return data_;};
  virtual std::shared_ptr<Marshallable> ptr() const {return data_;};
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