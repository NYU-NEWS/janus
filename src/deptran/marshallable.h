#pragma once
#include "__dep__.h"

namespace janus {

class Marshallable {
 public:
  int32_t kind_{0};
//  int32_t __debug_{10};
  Marshallable() = delete;
  explicit Marshallable(int32_t k): kind_(k) {};
  virtual ~Marshallable() {
//    if (__debug_ != 10) {
//      verify(0);
//    }
//    __debug_ = 30;
//    Log_debug("destruct marshallable.");
  };
  virtual Marshal& ToMarshal(Marshal& m) const;
  virtual Marshal& FromMarshal(Marshal& m);
};

class MarshallDeputy {
 public:
  typedef unordered_map<int32_t, function<Marshallable*()>> MarContainer;
  static MarContainer& Initializers();
  static int RegInitializer(int32_t, function<Marshallable*()>);
  static function<Marshallable*()> GetInitializer(int32_t);

 public:
  shared_ptr<Marshallable> sp_data_{nullptr};
  int32_t kind_{0};
  enum Kind {
    UNKNOWN=0,
    EMPTY_GRAPH=1,
    RCC_GRAPH=2,
    CONTAINER_CMD=3,
    CMD_TPC_PREPARE=4,
    CMD_TPC_COMMIT=5,
    CMD_VEC_PIECE=6
  };
  /**
   * This should be called by the rpc layer.
   */
  MarshallDeputy() : kind_(UNKNOWN){}
  /**
   * This should be called by inherited class as instructor.
   * @param kind
   */
  explicit MarshallDeputy(shared_ptr<Marshallable> m): sp_data_(std::move(m)) {
    kind_ = sp_data_->kind_;
  }

  Marshal& CreateActualObjectFrom(Marshal& m);
  void SetMarshallable(shared_ptr<Marshallable> m) {
    verify(sp_data_ == nullptr);
    sp_data_ = m;
    kind_ = m->kind_;
  }

  ~MarshallDeputy() = default;
};

inline Marshal& operator>>(Marshal& m, MarshallDeputy& rhs) {
  m >> rhs.kind_;
  rhs.CreateActualObjectFrom(m);
  return m;
}

inline Marshal& operator<<(Marshal& m, const MarshallDeputy& rhs) {
  verify(rhs.kind_ != MarshallDeputy::UNKNOWN);
  m << rhs.kind_;
  verify(rhs.sp_data_); // must be non-empty
  rhs.sp_data_->ToMarshal(m);
  return m;
}

} // namespace janus;
