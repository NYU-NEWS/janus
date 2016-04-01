#pragma once

#include "../__dep__.h"
#include "../executor.h"
//#include "../rcc_rpc.h"
#include "../txn_reg.h"


namespace rococo {

class SimpleCommand;
class ClassicExecutor: public Executor {
  using Executor::Executor;
 public:
  virtual ~ClassicExecutor();

  virtual int StartLaunch(const SimpleCommand &cmd,
                          rrr::i32 *res,
                          map<int32_t, Value>* output,
                          rrr::DeferredReply *defer);

  int PrepareLaunch(const std::vector<i32> &sids,
                    int32_t *res,
                    rrr::DeferredReply *defer);

  virtual bool Prepare();

  int commit_launch(
      rrr::i32 *res,
      rrr::DeferredReply *defer
  );

  virtual int Commit();

  int abort_launch(
      rrr::i32 *res,
      rrr::DeferredReply *defer
  );

  virtual int abort();

  void Execute(const SimpleCommand &cmd,
               rrr::i32 *res,
               map<int32_t, Value> &output);

  void execute(const SimpleCommand& cmd,
               rrr::i32 *res,
               map<int32_t, Value> &output,
               rrr::i32 *output_size);

};

} // namespace rococo