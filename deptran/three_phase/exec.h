#pragma once

#include "../__dep__.h"
#include "../executor.h"
//#include "../rcc_rpc.h"
#include "../txn_reg.h"


namespace rococo {

class SimpleCommand;
class ThreePhaseExecutor: public Executor {
  using Executor::Executor;
 public:


  virtual ~ThreePhaseExecutor();

  virtual int StartLaunch(const SimpleCommand &cmd,
                          rrr::i32 *res,
                          map<int32_t, Value> &output,
                          rrr::DeferredReply *defer);

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

  virtual int abort();

  void execute(const SimpleCommand& cmd,
               rrr::i32 *res,
               map<int32_t, Value> &output);

  void execute(const SimpleCommand& cmd,
               rrr::i32 *res,
               map<int32_t, Value> &output,
               rrr::i32 *output_size);

};

} // namespace rococo