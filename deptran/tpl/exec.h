#pragma once

#include "../__dep__.h"
#include "../constants.h"
#include "../three_phase/exec.h"

namespace rococo {

class TPLExecutor: public ThreePhaseExecutor {
  using ThreePhaseExecutor::ThreePhaseExecutor;
 public:
  virtual ~TPLExecutor(){};

  virtual int start_launch(
      const RequestHeader &header,
      const std::vector <mdb::Value> &input,
      const rrr::i32 &output_size,
      rrr::i32 *res,
      map <int32_t, Value> *output,
      rrr::DeferredReply *defer
  );

  // Below are merged from TxnRegistry.

  std::function<void(void)> get_2pl_proceed_callback(
      const RequestHeader &header,
      const mdb::Value *input,
      rrr::i32 input_size,
      rrr::i32 *res
  );

  std::function<void(void)> get_2pl_fail_callback(
      const RequestHeader &header,
      rrr::i32 *res,
      mdb::Txn2PL::PieceStatus *ps
  );

  std::function<void(void)> get_2pl_succ_callback(
      const RequestHeader &req,
      const mdb::Value *input,
      rrr::i32 input_size,
      rrr::i32 *res,
      mdb::Txn2PL::PieceStatus *ps
  );

  virtual int prepare();
  virtual int commit();

};

} // namespace rococo