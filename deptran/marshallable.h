#pragma once
#include "__dep__.h"

namespace janus {

/**
 * TODO: merge this with ContainerCommand, both of which works as a marshalling
 * proxy.
 */
class Marshallable : public std::enable_shared_from_this<Marshallable> {
 public:
  static map<int32_t, function<Marshallable*()>>& Initializers();
  static int RegInitializer(int32_t, function<Marshallable*()>);
  static function<Marshallable*()>& GetInitializer(int32_t);

 protected:
  std::shared_ptr<Marshallable> data_{};
 public:
  int32_t rtti_;
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
  Marshallable() : rtti_(UNKNOWN){}
  /**
   * This should be called by inherited class as instructor.
   * @param rtti
   */
  Marshallable(int32_t rtti) : rtti_(rtti) {

  }

  virtual ~Marshallable() {};
  Marshal& CreateActuallObjectFrom(Marshal &m);
  virtual Marshal& ToMarshal(Marshal& m) const;
  virtual Marshal& FromMarshal(Marshal& m);
  virtual std::shared_ptr<Marshallable>& ptr() {return data_;};
  virtual std::shared_ptr<Marshallable> ptr() const {return data_;};
};

inline Marshal& operator>>(Marshal& m, Marshallable& rhs) {
  m >> rhs.rtti_;
  // TODO
  rhs.CreateActuallObjectFrom(m);
  return m;
}

inline Marshal& operator<<(Marshal& m, const Marshallable& rhs) {
  m << rhs.rtti_;
  rhs.ToMarshal(m);
  return m;
}

} // namespace rococo;