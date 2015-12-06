#pragma once

#include "../three_phase/exec.h"

namespace rococo {

class OCCExecutor: public ThreePhaseExecutor {
 public:
  virtual int start_launch(
      const RequestHeader &header,
      const std::vector <mdb::Value> &input,
      const rrr::i32 &output_size,
      rrr::i32 *res,
      std::vector <mdb::Value> *output,
      rrr::DeferredReply *defer
  );

  virtual int prepare();
  virtual int commit();

};

} // namespace rococo