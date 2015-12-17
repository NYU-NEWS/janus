#pragma once

#include "../three_phase/exec.h"

namespace rococo {

class OCCExecutor: public ThreePhaseExecutor {
 public:
  virtual int start_launch(
      const RequestHeader &header,
      const map<int32_t, Value> &input,
      const rrr::i32 &output_size,
      rrr::i32 *res,
      map<int32_t, Value> &output,
      rrr::DeferredReply *defer
  );

  virtual int prepare();
  virtual int commit();

};

} // namespace rococo