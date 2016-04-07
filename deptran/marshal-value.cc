#include "__dep__.h"
#include "marshal-value.h"

namespace rrr {

rrr::Marshal &operator<<(rrr::Marshal &m, const mdb::Value &value) {
  m << value.ver_;
  switch (value.get_kind()) {
    case Value::I32:
      m << (i32) 0 << value.get_i32();
      break;
    case Value::I64:
      m << (i32) 1 << value.get_i64();
      break;
    case Value::DOUBLE:
      m << (i32) 2 << value.get_double();
      break;
    case Value::STR:
      m << (i32) 3 << value.get_str();
      break;
    default:
      verify(0);
      break;
  }
  return m;
}

rrr::Marshal &operator>>(rrr::Marshal &m, mdb::Value &value) {
  m >> value.ver_;
  i32 k;
  m >> k;
  switch (k) {
    case 0:
      int32_t i32;
      m >> i32;
      value.set_i32(i32);
      break;
    case 1:
      int64_t i64;
      m >> i64;
      value.set_i64(i64);
      break;
    case 2:
      double d;
      m >> d;
      value.set_double(d);
      break;
    case 3:
      std::string str;
      m >> str;
      value.set_str(str);
      break;
  }
  return m;
}

} // namespace rrr

