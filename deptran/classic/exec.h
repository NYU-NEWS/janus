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
  vector<SimpleCommand> cmds_ = {};

  virtual ~ClassicExecutor();

  virtual int StartLaunch(const SimpleCommand &cmd,
                          rrr::i32 *res,
                          map<int32_t, Value>* output,
                          const function<void()>& callback);

  int PrepareLaunch(const std::vector<i32> &sids,
                    int32_t *res,
                    rrr::DeferredReply *defer);

  virtual bool Prepare();

  int CommitLaunch(int32_t *res,
                   const function<void()> &callback);

  virtual int Commit();

  int AbortLaunch(int32_t *res,
                  const function<void()> &callback);

  virtual int Abort();

  void Execute(const SimpleCommand &cmd,
               rrr::i32 *res,
               map<int32_t, Value> &output);

  void execute(const SimpleCommand& cmd,
               rrr::i32 *res,
               map<int32_t, Value> &output,
               rrr::i32 *output_size);

};

} // namespace rococo