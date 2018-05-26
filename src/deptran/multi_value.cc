
#include "multi_value.h"

namespace janus {

int MultiValue::compare(const MultiValue& mv) const {
  int i = 0;
  for (i = 0; i < n_ && i < mv.n_; i++) {
    int r = v_[i].compare(mv.v_[i]);
    if (r != 0) {
      return r;
    }
  }
  if (i < n_) {
    return 1;
  } else if (i < mv.n_) {
    return -1;
  }
  return 0;
}

} // namespace janus
