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
  function<void(int)> prepare_reply_ = [] (int r) {};
  function<void(int)> commit_reply_ = [] (int r) {};


  virtual ~ClassicExecutor();

  virtual int OnDispatch(const SimpleCommand &cmd,
                         rrr::i32 *res,
                         map<int32_t, Value> *output,
                         const function<void()> &callback);

  virtual int OnDispatch(const vector<SimpleCommand>& cmd,
                         int32_t *res,
                         TxnOutput* output,
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
               map<int32_t, Value> &output) override;

  void execute(const SimpleCommand& cmd,
               rrr::i32 *res,
               map<int32_t, Value> &output,
               rrr::i32 *output_size);

};

} // namespace rococo