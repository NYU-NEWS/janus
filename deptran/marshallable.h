#pragma once
#include "__dep__.h"

namespace janus {

class Marshallable {
 public:
  int32_t kind_{0};
  Marshallable(int32_t k): kind_(k) {};
  virtual ~Marshallable() {};
  virtual Marshal& ToMarshal(Marshal& m) const;
  virtual Marshal& FromMarshal(Marshal& m);
};


class MarshallDeputy {
 public:
  static map<int32_t, function<Marshallable*()>>& Initializers();
  static int RegInitializer(int32_t, function<Marshallable*()>);
  static function<Marshallable*()>& GetInitializer(int32_t);

 public:
  Marshallable* data_{nullptr};
  bool manage_memory_{false};
  int32_t kind_;
  enum Kind {
    UNKNOWN=0,
    EMPTY_GRAPH=1,
    RCC_GRAPH=2,
    CONTAINER_CMD=3,
    CMD_TPC_PREPARE=4,
    CMD_TPC_COMMIT=5
  };
  /**
   * This should be called by the rpc layer.
   */
  MarshallDeputy() : kind_(UNKNOWN){}
  /**
   * This should be called by inherited class as instructor.
   * @param kind
   */
  MarshallDeputy(Marshallable* m, bool manage_memory): data_(m) {
    manage_memory_ = manage_memory;
  }


  Marshal& CreateActuallObjectFrom(Marshal &m);
  void SetMarshallable(Marshallable* m, bool manage_memory) {
    verify(data_ == nullptr);
    data_ = m;
    kind_ = m->kind_;
    manage_memory_ = manage_memory;
  }

  ~MarshallDeputy() {
    if (manage_memory_) {
      delete data_;
    }
  }
};

inline Marshal& operator>>(Marshal& m, MarshallDeputy& rhs) {
  m >> rhs.kind_;
  rhs.CreateActuallObjectFrom(m);
  return m;
}

inline Marshal& operator<<(Marshal& m, const MarshallDeputy& rhs) {
  m << rhs.kind_;
  verify(rhs.data_); // must be non-empty
  rhs.data_->ToMarshal(m);
  return m;
}

} // namespace rococo;