#pragma once

#include "../__dep__.h"
#include "../executor.h"
#include "../rcc_rpc.h"
#include "../txn_reg.h"


namespace rococo {

class ThreePhaseExecutor: public Executor {
  using Executor::Executor;
 public:

  virtual ~ThreePhaseExecutor();

  virtual int start_launch(
      const RequestHeader &header,
      const std::vector <mdb::Value> &input,
      const rrr::i32 &output_size,
      rrr::i32 *res,
      std::vector <mdb::Value> *output,
      rrr::DeferredReply *defer
  );

  int prepare_launch(
      const std::vector <i32> &sids,
      rrr::i32 *res,
      rrr::DeferredReply *defer
  );

  virtual int prepare();

  int commit_launch(
      rrr::i32 *res,
      rrr::DeferredReply *defer
  );

  virtual int commit();

  int abort_launch(
      rrr::i32 *res,
      rrr::DeferredReply *defer
  );

  int abort();

  void execute(
      const RequestHeader &header,
      const std::vector <mdb::Value> &input,
      rrr::i32 *res,
      std::vector <mdb::Value> *output
  );

  void execute(
      const RequestHeader &header,
      const Value *input,
      rrr::i32 input_size,
      rrr::i32 *res,
      mdb::Value *output,
      rrr::i32 *output_size
  );

};

} // namespace rococo